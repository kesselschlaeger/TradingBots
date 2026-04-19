"""PersistentState – zentrale SQLite-Datenbank für alle Bots.

Single-Source-of-Truth für:
  - Trade-Historie (``trades``)
  - Equity-Zeitreihe (``equity_snapshots``)
  - Offene Positionen (``positions``)
  - Signal-Stream (``signals``)
  - Anomaly-Events (``anomaly_events``)
  - Tages-PnL-/Trade-Counter (``daily``)
  - Account-Peak (``account``)
  - Cooldowns (``cooldowns``)
  - Reservierte Gruppen für MIT-Independence (``reserved_groups``)

EINE DB-Datei für ALLE gleichzeitig laufenden Strategien (ORB, OBB,
Botti, BottiPair, …). Jede neue Tabelle trägt ``strategy TEXT NOT NULL``
als Diskriminator – so bleiben die Daten pro Strategie sauber getrennt,
während Dashboard und probabilistische Auswertungen (EV, Kelly, MIT-
Overlay) cross-strategy aggregieren können.

Async-Implementation via ``aiosqlite``. Alle Writer-Pfade hängen an
einem gemeinsamen ``asyncio.Lock`` (aiosqlite serialisiert Connections
zwar intern, der Lock deckt aber zusätzlich Read-Modify-Write-Sequenzen
ab, z.B. den UPSERT in ``update_or_create_position``).
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

from core.logging import get_logger
from core.models import AnomalyEvent

log = get_logger(__name__)


# ── Schema ─────────────────────────────────────────────────────────────────
# Wichtig: Jede domain-spezifische Tabelle trägt ``strategy TEXT NOT NULL``.

_SCHEMA_STATEMENTS: tuple[str, ...] = (
    # Tages-PnL/Trade-Counter – bestehende Tabelle, PK bleibt ``day``;
    # die strategy-Spalte wird via ALTER TABLE migriert (siehe
    # ``_migrate_daily``) und Unique-Constraint liegt auf (day, strategy).
    """
    CREATE TABLE IF NOT EXISTS daily (
        day TEXT NOT NULL,
        strategy TEXT NOT NULL DEFAULT '',
        pnl REAL DEFAULT 0.0,
        trades_count INTEGER DEFAULT 0,
        by_symbol TEXT DEFAULT '{}',
        PRIMARY KEY (day, strategy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS account (
        key TEXT PRIMARY KEY,
        value REAL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cooldowns (
        symbol TEXT PRIMARY KEY,
        until_ts TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reserved_groups (
        group_name TEXT NOT NULL,
        day TEXT NOT NULL,
        strategy TEXT NOT NULL DEFAULT '',
        PRIMARY KEY (group_name, day, strategy)
    )
    """,
    # ── Phase 1: neue Tabellen ──────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_ts TEXT NOT NULL,
        exit_ts TEXT,
        entry_price REAL NOT NULL,
        exit_price REAL,
        qty REAL NOT NULL,
        pnl REAL,
        pnl_pct REAL,
        reason TEXT,
        stop_price REAL,
        signal_strength REAL,
        mit_qty_factor REAL,
        ev_estimate REAL,
        group_name TEXT,
        features_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS equity_snapshots (
        ts TEXT NOT NULL,
        strategy TEXT NOT NULL,
        equity REAL,
        cash REAL,
        drawdown_pct REAL,
        peak_equity REAL,
        unrealized_pnl_total REAL,
        PRIMARY KEY (ts, strategy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT,
        entry_ts TEXT,
        entry_price REAL,
        qty REAL,
        stop_price REAL,
        last_update_ts TEXT,
        current_price REAL,
        unrealized_pnl REAL,
        unrealized_pnl_pct REAL,
        held_minutes INTEGER,
        UNIQUE (strategy, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy TEXT NOT NULL,
        symbol TEXT,
        ts TEXT,
        action TEXT,
        strength REAL,
        filtered_by TEXT,
        mit_passed INTEGER,
        ev_value REAL,
        features_json TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS anomaly_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy TEXT NOT NULL DEFAULT '',
        ts TEXT NOT NULL,
        check_name TEXT NOT NULL,
        severity TEXT NOT NULL,
        symbol TEXT,
        message TEXT,
        context_json TEXT
    )
    """,
)

_INDEX_STATEMENTS: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_trades_strategy_entry_ts "
    "ON trades(strategy, entry_ts)",
    "CREATE INDEX IF NOT EXISTS idx_trades_strategy_exit_ts "
    "ON trades(strategy, exit_ts)",
    "CREATE INDEX IF NOT EXISTS idx_trades_symbol_entry_ts "
    "ON trades(symbol, entry_ts)",
    "CREATE INDEX IF NOT EXISTS idx_equity_strategy_ts "
    "ON equity_snapshots(strategy, ts)",
    "CREATE INDEX IF NOT EXISTS idx_positions_strategy "
    "ON positions(strategy)",
    "CREATE INDEX IF NOT EXISTS idx_signals_strategy_ts "
    "ON signals(strategy, ts)",
    "CREATE INDEX IF NOT EXISTS idx_anomaly_strategy_ts "
    "ON anomaly_events(strategy, ts)",
    "CREATE INDEX IF NOT EXISTS idx_daily_strategy_day "
    "ON daily(strategy, day)",
)


class PersistentState:
    """SQLite-backed State – zentral für alle Strategien."""

    def __init__(self, db_path: str | Path):
        if not AIOSQLITE_AVAILABLE:
            raise RuntimeError("aiosqlite fehlt – pip install aiosqlite")
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._schema_ready: bool = False

    # ── Connection-Factory ────────────────────────────────────────────

    @asynccontextmanager
    async def _conn(self):
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            yield conn

    # ── Schema / Migration ───────────────────────────────────────────

    async def ensure_schema(self) -> None:
        """Idempotent: Tabellen + Indizes + WAL-Modus + Migration."""
        async with self._lock:
            if self._schema_ready:
                return
            async with self._conn() as conn:
                # Concurrency-Freundlich: WAL + Normal-Sync
                await conn.execute("PRAGMA journal_mode=WAL")
                await conn.execute("PRAGMA synchronous=NORMAL")
                await conn.execute("PRAGMA foreign_keys=ON")

                # Legacy-daily -> neue Schema-Form migrieren (ohne Datenverlust).
                await self._migrate_daily(conn)

                for stmt in _SCHEMA_STATEMENTS:
                    await conn.execute(stmt)
                for stmt in _INDEX_STATEMENTS:
                    await conn.execute(stmt)
                await conn.commit()
            self._schema_ready = True
        log.info("state.schema_ready", path=str(self.db_path))

    async def _migrate_daily(self, conn: Any) -> None:
        """Legacy daily (PK=day) -> (day, strategy) ohne Datenverlust."""
        cur = await conn.execute("PRAGMA table_info(daily)")
        cols = {row[1] for row in await cur.fetchall()}
        if not cols:
            return  # Tabelle existiert noch nicht – normale CREATE-Pfad
        if "strategy" in cols:
            return  # bereits migriert

        log.info("state.migrate_daily_add_strategy")
        # SQLite kann PK nicht online ändern – rebuild via shadow-table.
        await conn.execute("ALTER TABLE daily RENAME TO daily_legacy")
        await conn.execute(
            """
            CREATE TABLE daily (
                day TEXT NOT NULL,
                strategy TEXT NOT NULL DEFAULT '',
                pnl REAL DEFAULT 0.0,
                trades_count INTEGER DEFAULT 0,
                by_symbol TEXT DEFAULT '{}',
                PRIMARY KEY (day, strategy)
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO daily (day, strategy, pnl, trades_count, by_symbol)
            SELECT day, '', pnl, trades_count, COALESCE(by_symbol, '{}')
            FROM daily_legacy
            """
        )
        await conn.execute("DROP TABLE daily_legacy")

    async def init(self) -> None:
        """Abwärtskompatibler Alias für ``ensure_schema``."""
        await self.ensure_schema()

    async def close(self) -> None:
        return None  # aiosqlite-Connections werden pro Call geschlossen

    # ══════════════════════════════════════════════════════════════════
    # Writer: trades
    # ══════════════════════════════════════════════════════════════════

    async def save_trade(
        self,
        *,
        strategy: str,
        symbol: str,
        side: str,
        entry_ts: datetime | str,
        entry_price: float,
        qty: float,
        stop_price: Optional[float] = None,
        signal_strength: Optional[float] = None,
        mit_qty_factor: Optional[float] = None,
        ev_estimate: Optional[float] = None,
        group_name: Optional[str] = None,
        features_json: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> int:
        """Legt einen neuen offenen Trade an. Gibt die ID zurück."""
        ts = _iso(entry_ts)
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    """
                    INSERT INTO trades (
                        strategy, symbol, side, entry_ts, entry_price, qty,
                        stop_price, signal_strength, mit_qty_factor,
                        ev_estimate, group_name, features_json, reason
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        strategy, symbol, side, ts, float(entry_price),
                        float(qty), stop_price, signal_strength,
                        mit_qty_factor, ev_estimate, group_name,
                        features_json, reason,
                    ),
                )
                trade_id = cur.lastrowid
                await conn.commit()
        log.info("state.trade_opened", id=trade_id, strategy=strategy,
                 symbol=symbol, side=side)
        return int(trade_id or 0)

    async def close_trade(
        self,
        *,
        trade_id: Optional[int] = None,
        strategy: Optional[str] = None,
        symbol: Optional[str] = None,
        exit_ts: datetime | str,
        exit_price: float,
        pnl: Optional[float] = None,
        pnl_pct: Optional[float] = None,
        reason: Optional[str] = None,
    ) -> None:
        """Schließt den neuesten offenen Trade (``exit_ts IS NULL``)
        anhand ``trade_id`` ODER (``strategy``, ``symbol``)."""
        ts = _iso(exit_ts)
        async with self._lock:
            async with self._conn() as conn:
                if trade_id is not None:
                    row = await (
                        await conn.execute(
                            "SELECT id FROM trades WHERE id=? AND exit_ts IS NULL",
                            (int(trade_id),),
                        )
                    ).fetchone()
                else:
                    row = await (
                        await conn.execute(
                            """
                            SELECT id FROM trades
                            WHERE strategy=? AND symbol=? AND exit_ts IS NULL
                            ORDER BY entry_ts DESC LIMIT 1
                            """,
                            (strategy or "", symbol or ""),
                        )
                    ).fetchone()
                if row is None:
                    log.warning("state.close_trade_no_match",
                                trade_id=trade_id, strategy=strategy,
                                symbol=symbol)
                    return
                tid = int(row["id"])
                await conn.execute(
                    """
                    UPDATE trades
                    SET exit_ts=?, exit_price=?, pnl=?, pnl_pct=?, reason=?
                    WHERE id=?
                    """,
                    (ts, float(exit_price), pnl, pnl_pct, reason, tid),
                )
                await conn.commit()
        log.info("state.trade_closed", id=tid, pnl=pnl, reason=reason)

    # ══════════════════════════════════════════════════════════════════
    # Writer: equity_snapshots
    # ══════════════════════════════════════════════════════════════════

    async def save_equity_snapshot(
        self,
        *,
        strategy: str,
        ts: datetime | str,
        equity: float,
        cash: float = 0.0,
        drawdown_pct: float = 0.0,
        peak_equity: float = 0.0,
        unrealized_pnl_total: float = 0.0,
    ) -> None:
        ts_iso = _iso(ts)
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO equity_snapshots
                    (ts, strategy, equity, cash, drawdown_pct, peak_equity,
                     unrealized_pnl_total)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (
                        ts_iso, strategy, float(equity), float(cash),
                        float(drawdown_pct), float(peak_equity),
                        float(unrealized_pnl_total),
                    ),
                )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Writer: positions (UPSERT)
    # ══════════════════════════════════════════════════════════════════

    async def update_or_create_position(
        self,
        *,
        strategy: str,
        symbol: str,
        side: Optional[str] = None,
        entry_ts: Optional[datetime | str] = None,
        entry_price: Optional[float] = None,
        qty: Optional[float] = None,
        stop_price: Optional[float] = None,
        current_price: Optional[float] = None,
        unrealized_pnl: Optional[float] = None,
        unrealized_pnl_pct: Optional[float] = None,
        held_minutes: Optional[int] = None,
    ) -> None:
        last = _iso(datetime.now(timezone.utc))
        entry_iso = _iso(entry_ts) if entry_ts is not None else None
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "SELECT id FROM positions WHERE strategy=? AND symbol=?",
                    (strategy, symbol),
                )
                row = await cur.fetchone()
                if row is None:
                    await conn.execute(
                        """
                        INSERT INTO positions (
                            strategy, symbol, side, entry_ts, entry_price, qty,
                            stop_price, last_update_ts, current_price,
                            unrealized_pnl, unrealized_pnl_pct, held_minutes
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            strategy, symbol, side, entry_iso, entry_price,
                            qty, stop_price, last, current_price,
                            unrealized_pnl, unrealized_pnl_pct, held_minutes,
                        ),
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE positions SET
                            side=COALESCE(?, side),
                            entry_ts=COALESCE(?, entry_ts),
                            entry_price=COALESCE(?, entry_price),
                            qty=COALESCE(?, qty),
                            stop_price=COALESCE(?, stop_price),
                            last_update_ts=?,
                            current_price=COALESCE(?, current_price),
                            unrealized_pnl=COALESCE(?, unrealized_pnl),
                            unrealized_pnl_pct=COALESCE(?, unrealized_pnl_pct),
                            held_minutes=COALESCE(?, held_minutes)
                        WHERE id=?
                        """,
                        (
                            side, entry_iso, entry_price, qty, stop_price,
                            last, current_price, unrealized_pnl,
                            unrealized_pnl_pct, held_minutes, row["id"],
                        ),
                    )
                await conn.commit()

    async def remove_position(self, strategy: str, symbol: str) -> None:
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    "DELETE FROM positions WHERE strategy=? AND symbol=?",
                    (strategy, symbol),
                )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Writer: signals
    # ══════════════════════════════════════════════════════════════════

    async def save_signal(
        self,
        *,
        strategy: str,
        symbol: Optional[str] = None,
        ts: Optional[datetime | str] = None,
        action: Optional[str] = None,
        strength: Optional[float] = None,
        filtered_by: Optional[str] = None,
        mit_passed: Optional[bool] = None,
        ev_value: Optional[float] = None,
        features_json: Optional[str] = None,
    ) -> int:
        ts_iso = _iso(ts) if ts is not None else _iso(datetime.now(timezone.utc))
        mit_int: Optional[int] = None if mit_passed is None else int(bool(mit_passed))
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    """
                    INSERT INTO signals (
                        strategy, symbol, ts, action, strength,
                        filtered_by, mit_passed, ev_value, features_json
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        strategy, symbol, ts_iso, action, strength,
                        filtered_by, mit_int, ev_value, features_json,
                    ),
                )
                await conn.commit()
                return int(cur.lastrowid or 0)

    # ══════════════════════════════════════════════════════════════════
    # Writer: anomaly_events
    # ══════════════════════════════════════════════════════════════════

    async def log_anomaly(self, event: AnomalyEvent) -> None:
        ctx_json: Optional[str] = None
        try:
            ctx_json = json.dumps(event.context, default=str)
        except (TypeError, ValueError):
            ctx_json = None
        sev = event.severity.value if hasattr(event.severity, "value") else str(event.severity)
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    """
                    INSERT INTO anomaly_events (
                        strategy, ts, check_name, severity, symbol,
                        message, context_json
                    ) VALUES (?,?,?,?,?,?,?)
                    """,
                    (
                        event.strategy or "", _iso(event.timestamp),
                        event.check_name, sev, event.symbol,
                        event.message, ctx_json,
                    ),
                )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Writer: daily (jetzt strategy-aware)
    # ══════════════════════════════════════════════════════════════════

    async def update_daily_record(
        self,
        *,
        day: date,
        strategy: str,
        pnl_delta: float = 0.0,
        symbol: Optional[str] = None,
    ) -> None:
        """Inkrementiert pnl + trades_count für (day, strategy)."""
        key = day.isoformat()
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "SELECT pnl, trades_count, by_symbol FROM daily "
                    "WHERE day=? AND strategy=?",
                    (key, strategy),
                )
                row = await cur.fetchone()
                if row is None:
                    by_sym = {symbol: 1} if symbol else {}
                    await conn.execute(
                        """
                        INSERT INTO daily(day, strategy, pnl, trades_count, by_symbol)
                        VALUES (?,?,?,?,?)
                        """,
                        (
                            key, strategy, float(pnl_delta),
                            1 if symbol else 0,
                            json.dumps(by_sym),
                        ),
                    )
                else:
                    by_sym = json.loads(row["by_symbol"] or "{}")
                    if symbol:
                        by_sym[symbol] = by_sym.get(symbol, 0) + 1
                    await conn.execute(
                        """
                        UPDATE daily SET pnl=?, trades_count=?, by_symbol=?
                        WHERE day=? AND strategy=?
                        """,
                        (
                            float(row["pnl"]) + float(pnl_delta),
                            int(row["trades_count"]) + (1 if symbol else 0),
                            json.dumps(by_sym), key, strategy,
                        ),
                    )
                await conn.commit()

    # ── Legacy-API (bleibt für Abwärtskompatibilität) ───────────────

    async def add_trade(
        self, day: date, symbol: str, pnl: float, strategy: str = "",
    ) -> None:
        """Legacy-Alias – delegiert an ``update_daily_record``."""
        await self.update_daily_record(
            day=day, strategy=strategy, pnl_delta=pnl, symbol=symbol,
        )

    async def daily_pnl(self, day: date, strategy: Optional[str] = None) -> float:
        async with self._conn() as conn:
            if strategy is None:
                cur = await conn.execute(
                    "SELECT COALESCE(SUM(pnl),0.0) AS s FROM daily WHERE day=?",
                    (day.isoformat(),),
                )
            else:
                cur = await conn.execute(
                    "SELECT COALESCE(SUM(pnl),0.0) AS s FROM daily "
                    "WHERE day=? AND strategy=?",
                    (day.isoformat(), strategy),
                )
            row = await cur.fetchone()
            return float(row["s"]) if row else 0.0

    async def trades_today(
        self, day: date, strategy: Optional[str] = None,
    ) -> dict[str, int]:
        async with self._conn() as conn:
            if strategy is None:
                cur = await conn.execute(
                    "SELECT by_symbol FROM daily WHERE day=?",
                    (day.isoformat(),),
                )
            else:
                cur = await conn.execute(
                    "SELECT by_symbol FROM daily WHERE day=? AND strategy=?",
                    (day.isoformat(), strategy),
                )
            rows = await cur.fetchall()
        merged: dict[str, int] = {}
        for r in rows:
            part = json.loads(r["by_symbol"] or "{}")
            for k, v in part.items():
                merged[k] = merged.get(k, 0) + int(v)
        return merged

    # ══════════════════════════════════════════════════════════════════
    # Account-Peak
    # ══════════════════════════════════════════════════════════════════

    async def update_peak_equity(self, equity: float) -> float:
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "SELECT value FROM account WHERE key='peak_equity'"
                )
                row = await cur.fetchone()
                current_peak = float(row["value"]) if row else 0.0
                new_peak = max(current_peak, float(equity))
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

    # ══════════════════════════════════════════════════════════════════
    # Cooldowns
    # ══════════════════════════════════════════════════════════════════

    async def set_cooldown(self, symbol: str, until: datetime) -> None:
        async with self._lock:
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
        async with self._lock:
            async with self._conn() as conn:
                for s in expired:
                    await conn.execute("DELETE FROM cooldowns WHERE symbol=?", (s,))
                await conn.commit()
        return len(expired)

    # ══════════════════════════════════════════════════════════════════
    # Reserved-Groups (MIT-Independence, täglich)
    # ══════════════════════════════════════════════════════════════════

    async def reserve_group(
        self, group: str, day: date, strategy: str = "",
    ) -> None:
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    """
                    INSERT OR IGNORE INTO reserved_groups(group_name, day, strategy)
                    VALUES (?,?,?)
                    """,
                    (group, day.isoformat(), strategy),
                )
                await conn.commit()

    async def reserved_groups(
        self, day: date, strategy: Optional[str] = None,
    ) -> set[str]:
        async with self._conn() as conn:
            if strategy is None:
                cur = await conn.execute(
                    "SELECT group_name FROM reserved_groups WHERE day=?",
                    (day.isoformat(),),
                )
            else:
                cur = await conn.execute(
                    "SELECT group_name FROM reserved_groups "
                    "WHERE day=? AND strategy=?",
                    (day.isoformat(), strategy),
                )
            rows = await cur.fetchall()
        return {r["group_name"] for r in rows}

    async def reset_day(self, day: date, strategy: Optional[str] = None) -> None:
        async with self._lock:
            async with self._conn() as conn:
                if strategy is None:
                    await conn.execute(
                        "DELETE FROM reserved_groups WHERE day=?",
                        (day.isoformat(),),
                    )
                else:
                    await conn.execute(
                        "DELETE FROM reserved_groups "
                        "WHERE day=? AND strategy=?",
                        (day.isoformat(), strategy),
                    )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Reader: Dashboard + probabilistische Auswertungen
    # ══════════════════════════════════════════════════════════════════

    async def get_trades(
        self,
        *,
        strategy: Optional[str] = None,
        symbol: Optional[str] = None,
        since: Optional[datetime | str] = None,
        until: Optional[datetime | str] = None,
        only_closed: bool = False,
        limit: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = []
        params: list[Any] = []
        if strategy:
            where.append("strategy=?"); params.append(strategy)
        if symbol:
            where.append("symbol=?"); params.append(symbol)
        if since is not None:
            where.append("entry_ts >= ?"); params.append(_iso(since))
        if until is not None:
            where.append("entry_ts <= ?"); params.append(_iso(until))
        if only_closed:
            where.append("exit_ts IS NOT NULL")
        sql = "SELECT * FROM trades"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY entry_ts DESC"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        async with self._conn() as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_open_positions(
        self, strategy: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        async with self._conn() as conn:
            if strategy is None:
                cur = await conn.execute("SELECT * FROM positions")
            else:
                cur = await conn.execute(
                    "SELECT * FROM positions WHERE strategy=?", (strategy,),
                )
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_latest_equity_curve(
        self, strategy: Optional[str] = None, limit: int = 500,
    ) -> list[dict[str, Any]]:
        async with self._conn() as conn:
            if strategy is None:
                cur = await conn.execute(
                    "SELECT * FROM equity_snapshots ORDER BY ts DESC LIMIT ?",
                    (int(limit),),
                )
            else:
                cur = await conn.execute(
                    "SELECT * FROM equity_snapshots WHERE strategy=? "
                    "ORDER BY ts DESC LIMIT ?",
                    (strategy, int(limit)),
                )
            rows = await cur.fetchall()
        return list(reversed([dict(r) for r in rows]))

    async def get_strategies(self) -> list[str]:
        """Liefert alle Strategien, die Trades oder Equity-Einträge haben."""
        async with self._conn() as conn:
            cur = await conn.execute(
                """
                SELECT DISTINCT strategy FROM (
                    SELECT strategy FROM trades
                    UNION SELECT strategy FROM equity_snapshots
                    UNION SELECT strategy FROM positions
                ) WHERE strategy IS NOT NULL AND strategy <> ''
                """
            )
            rows = await cur.fetchall()
        return [r["strategy"] for r in rows]

    async def get_strategy_status(
        self, strategy: str,
    ) -> dict[str, Any]:
        """Aggregierter Bot-Status für das Dashboard."""
        today = datetime.now(timezone.utc).date()
        async with self._conn() as conn:
            eq = await (
                await conn.execute(
                    "SELECT equity, drawdown_pct, peak_equity, ts "
                    "FROM equity_snapshots WHERE strategy=? "
                    "ORDER BY ts DESC LIMIT 1",
                    (strategy,),
                )
            ).fetchone()
            opn = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM positions WHERE strategy=?",
                    (strategy,),
                )
            ).fetchone()
            todays = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c, COALESCE(SUM(pnl), 0.0) AS pnl
                    FROM trades WHERE strategy=? AND exit_ts IS NOT NULL
                    AND substr(exit_ts,1,10)=?
                    """,
                    (strategy, today.isoformat()),
                )
            ).fetchone()
        return {
            "strategy": strategy,
            "equity": float(eq["equity"]) if eq else None,
            "drawdown_pct": float(eq["drawdown_pct"]) if eq else None,
            "peak_equity": float(eq["peak_equity"]) if eq else None,
            "last_equity_ts": eq["ts"] if eq else None,
            "open_positions": int(opn["c"]) if opn else 0,
            "trades_today": int(todays["c"]) if todays else 0,
            "pnl_today": float(todays["pnl"]) if todays else 0.0,
        }

    # ══════════════════════════════════════════════════════════════════
    # Convenience-Snapshot (Legacy – für Alt-Aufrufer)
    # ══════════════════════════════════════════════════════════════════

    async def snapshot(self, day: Optional[date] = None) -> dict:
        d = day or datetime.now(timezone.utc).date()
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


# ── Helpers ───────────────────────────────────────────────────────────

def _iso(ts: datetime | str) -> str:
    """Normalisiert Timestamps zu ISO-8601 (UTC bevorzugt)."""
    if isinstance(ts, str):
        return ts
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()
