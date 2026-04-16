"""PersistentState – SQLite-Backed Tagesstatus für Live-Bots.

Persistiert über Restart hinweg:
  - daily_pnl pro Datum
  - trades_today pro Datum (Counter pro Symbol)
  - peak_equity (für DD-Berechnung)
  - cooldowns pro Symbol (Datum → Cooldown bis ts)
  - reserved_groups (für MIT-Independence über Restart)

Async-Implementation via aiosqlite. Schema-Migration ist idempotent.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

from core.logging import get_logger

log = get_logger(__name__)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily (
    day TEXT PRIMARY KEY,
    pnl REAL DEFAULT 0.0,
    trades_count INTEGER DEFAULT 0,
    by_symbol TEXT DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS account (
    key TEXT PRIMARY KEY,
    value REAL
);
CREATE TABLE IF NOT EXISTS cooldowns (
    symbol TEXT PRIMARY KEY,
    until_ts TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS reserved_groups (
    group_name TEXT NOT NULL,
    day TEXT NOT NULL,
    PRIMARY KEY (group_name, day)
);
"""


class PersistentState:
    """SQLite-backed Tagesstatus."""

    def __init__(self, db_path: str | Path):
        if not AIOSQLITE_AVAILABLE:
            raise RuntimeError("aiosqlite fehlt – pip install aiosqlite")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @asynccontextmanager
    async def _conn(self):
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            yield conn

    async def init(self) -> None:
        async with self._conn() as conn:
            await conn.executescript(_SCHEMA)
            await conn.commit()
        log.info("state.initialized", path=str(self.db_path))

    # ── Daily PnL & Trade-Counter ──────────────────────────────────────

    async def add_trade(self, day: date, symbol: str, pnl: float) -> None:
        key = day.isoformat()
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT pnl, trades_count, by_symbol FROM daily WHERE day=?",
                (key,),
            )
            row = await cur.fetchone()
            if row is None:
                by_sym = {symbol: 1}
                await conn.execute(
                    "INSERT INTO daily(day, pnl, trades_count, by_symbol) "
                    "VALUES (?,?,?,?)",
                    (key, pnl, 1, json.dumps(by_sym)),
                )
            else:
                by_sym = json.loads(row["by_symbol"] or "{}")
                by_sym[symbol] = by_sym.get(symbol, 0) + 1
                await conn.execute(
                    "UPDATE daily SET pnl=?, trades_count=?, by_symbol=? "
                    "WHERE day=?",
                    (row["pnl"] + pnl, row["trades_count"] + 1,
                     json.dumps(by_sym), key),
                )
            await conn.commit()

    async def daily_pnl(self, day: date) -> float:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT pnl FROM daily WHERE day=?", (day.isoformat(),),
            )
            row = await cur.fetchone()
            return float(row["pnl"]) if row else 0.0

    async def trades_today(self, day: date) -> dict[str, int]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT by_symbol FROM daily WHERE day=?", (day.isoformat(),),
            )
            row = await cur.fetchone()
            if not row:
                return {}
            return json.loads(row["by_symbol"] or "{}")

    # ── Account Peak (für DD) ──────────────────────────────────────────

    async def update_peak_equity(self, equity: float) -> float:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT value FROM account WHERE key='peak_equity'"
            )
            row = await cur.fetchone()
            current_peak = float(row["value"]) if row else 0.0
            new_peak = max(current_peak, equity)
            if new_peak != current_peak:
                await conn.execute(
                    "INSERT OR REPLACE INTO account(key, value) "
                    "VALUES ('peak_equity', ?)",
                    (new_peak,),
                )
                await conn.commit()
            return new_peak

    async def get_peak_equity(self) -> float:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT value FROM account WHERE key='peak_equity'"
            )
            row = await cur.fetchone()
            return float(row["value"]) if row else 0.0

    # ── Cooldowns ──────────────────────────────────────────────────────

    async def set_cooldown(self, symbol: str, until: datetime) -> None:
        async with self._conn() as conn:
            await conn.execute(
                "INSERT OR REPLACE INTO cooldowns(symbol, until_ts) VALUES (?,?)",
                (symbol, until.astimezone(timezone.utc).isoformat()),
            )
            await conn.commit()

    async def get_cooldowns(self) -> dict[str, datetime]:
        async with self._conn() as conn:
            cur = await conn.execute("SELECT symbol, until_ts FROM cooldowns")
            rows = await cur.fetchall()
        out: dict[str, datetime] = {}
        for r in rows:
            try:
                out[r["symbol"]] = datetime.fromisoformat(r["until_ts"])
            except ValueError:
                continue
        return out

    async def is_in_cooldown(self, symbol: str, now: datetime) -> bool:
        cools = await self.get_cooldowns()
        until = cools.get(symbol)
        if until is None:
            return False
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        return now < until

    async def clear_expired_cooldowns(self, now: datetime) -> int:
        cools = await self.get_cooldowns()
        expired = [s for s, u in cools.items()
                   if (u.tzinfo or timezone.utc) and now >= u]
        if not expired:
            return 0
        async with self._conn() as conn:
            for s in expired:
                await conn.execute("DELETE FROM cooldowns WHERE symbol=?", (s,))
            await conn.commit()
        return len(expired)

    # ── Group-Reservations (täglich, MIT) ──────────────────────────────

    async def reserve_group(self, group: str, day: date) -> None:
        async with self._conn() as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO reserved_groups(group_name, day) "
                "VALUES (?,?)",
                (group, day.isoformat()),
            )
            await conn.commit()

    async def reserved_groups(self, day: date) -> set[str]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT group_name FROM reserved_groups WHERE day=?",
                (day.isoformat(),),
            )
            rows = await cur.fetchall()
        return {r["group_name"] for r in rows}

    async def reset_day(self, day: date) -> None:
        async with self._conn() as conn:
            await conn.execute("DELETE FROM reserved_groups WHERE day=?",
                               (day.isoformat(),))
            await conn.commit()

    async def close(self) -> None:
        # aiosqlite: connections geschlossen via Context-Manager, nichts zu tun
        return None

    # ── Convenience-Snapshot ───────────────────────────────────────────

    async def snapshot(self, day: Optional[date] = None) -> dict:
        d = day or datetime.utcnow().date()
        return {
            "day": d.isoformat(),
            "daily_pnl": await self.daily_pnl(d),
            "trades_today": await self.trades_today(d),
            "peak_equity": await self.get_peak_equity(),
            "reserved_groups": list(await self.reserved_groups(d)),
            "cooldowns": {
                s: u.isoformat() for s, u in (await self.get_cooldowns()).items()
            },
        }
