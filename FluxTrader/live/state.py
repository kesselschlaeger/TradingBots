"""PersistentState – zentrale SQLite-Datenbank für alle Bot-Instanzen.

Tabellen und Zweck
------------------
trades             – vollständige Trade-Historie (Entry + Exit + PnL)
equity_snapshots   – Equity-Zeitreihe (Drawdown, Peak, unrealisiertes PnL)
positions          – aktuell offene Positionen (UPSERT per Bot+Symbol)
signals            – vollständiger Signal-Stream (auch gefilterte Signale)
anomaly_events     – Anomalie- und Alert-Events (Health-Monitoring)
account            – Key/Value-Store je Bot (z.B. peak_equity)
cooldowns          – Symbol-Cooldowns mit Ablaufzeitstempel
reserved_groups    – MIT-Independence: reservierte Gruppen je Tag
bot_heartbeat      – Live-Status je Bot (Broker, Circuit-Breaker, Lag)

Primäre Bot-Identität
---------------------
Das Tupel ``(bot_name, strategy)`` identifiziert eindeutig eine laufende
Bot-Instanz. Beide Felder sind in jeder Tabelle ``TEXT NOT NULL`` und bilden
gemeinsam den Composite-Key. Einzelne Felder reichen nicht zur Adressierung.

Connection-Pattern und Lock
---------------------------
Jede Methode öffnet ihre eigene aiosqlite-Connection via ``_conn()``
(asynccontextmanager). Connections werden nach jedem Call automatisch
geschlossen. Ein globaler ``asyncio.Lock`` serialisiert alle Writer-Pfade
einschließlich ``upsert_bot_heartbeat`` – damit sind auch Read-Modify-Write-
Sequenzen (z.B. UPSERT in ``update_or_create_position``) race-condition-frei.

Atomare Write-Sequenzen
-----------------------
``open_trade_atomic``:  trades INSERT + positions UPSERT + optional
                        reserved_groups INSERT in einer DB-Transaktion.
``close_trade_atomic``: trades UPDATE + positions DELETE in einer Transaktion.
Beide sind die einzige Trade-Lifecycle-API – kein direktes INSERT/UPDATE
außerhalb dieser Methoden.

Timestamps
----------
Alle Timestamps werden ausschließlich Python-seitig via ``_iso()`` erzeugt
und als ISO-8601-UTC-Strings gespeichert. Kein ``datetime('now')`` in SQL.
"""
from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

try:
    import aiosqlite
    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

from core.logging import get_logger
from core.models import AnomalyEvent

log = get_logger(__name__)


# ── Schema ─────────────────────────────────────────────────────────────────

_SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS account (
        key         TEXT NOT NULL,
        bot_name    TEXT NOT NULL,
        strategy    TEXT NOT NULL,
        value       REAL,
        PRIMARY KEY (key, bot_name, strategy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cooldowns (
        symbol      TEXT NOT NULL,
        bot_name    TEXT NOT NULL,
        strategy    TEXT NOT NULL,
        until_ts    TEXT NOT NULL,
        PRIMARY KEY (symbol, bot_name, strategy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS reserved_groups (
        group_name  TEXT NOT NULL,
        day         TEXT NOT NULL,
        bot_name    TEXT NOT NULL,
        strategy    TEXT NOT NULL,
        PRIMARY KEY (group_name, day, bot_name, strategy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS trades (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name         TEXT NOT NULL,
        strategy         TEXT NOT NULL,
        symbol           TEXT NOT NULL,
        side             TEXT NOT NULL,
        entry_ts         TEXT NOT NULL,
        exit_ts          TEXT,
        entry_price      REAL NOT NULL,
        exit_price       REAL,
        exit_reason      TEXT,
        exit_signal      TEXT,
        exit_strength    REAL,
        exit_features_json TEXT,
        qty              REAL NOT NULL,
        pnl              REAL,
        pnl_pct          REAL,
        stop_price       REAL,
        signal_strength  REAL,
        mit_qty_factor   REAL,
        ev_estimate      REAL,
        group_name       TEXT,
        features_json    TEXT,
        created_at       TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS equity_snapshots (
        ts                    TEXT NOT NULL,
        bot_name              TEXT NOT NULL,
        strategy              TEXT NOT NULL,
        equity                REAL,
        cash                  REAL NOT NULL DEFAULT 0.0,
        drawdown_pct          REAL NOT NULL DEFAULT 0.0,
        peak_equity           REAL NOT NULL DEFAULT 0.0,
        unrealized_pnl_total  REAL NOT NULL DEFAULT 0.0,
        created_at            TEXT NOT NULL,
        PRIMARY KEY (ts, bot_name, strategy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id            INTEGER REFERENCES trades(id) ON DELETE SET NULL,
        bot_name            TEXT NOT NULL,
        strategy            TEXT NOT NULL,
        symbol              TEXT NOT NULL,
        side                TEXT,
        entry_ts            TEXT,
        entry_price         REAL,
        qty                 REAL,
        stop_price          REAL,
        last_update_ts      TEXT,
        current_price       REAL,
        unrealized_pnl      REAL,
        unrealized_pnl_pct  REAL,
        held_minutes        INTEGER,
        entry_signal        TEXT,
        entry_reason        TEXT,
        broker_order_id     TEXT,
        order_reference     TEXT,
        created_at          TEXT NOT NULL,
        UNIQUE (bot_name, strategy, symbol)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS signals (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name      TEXT NOT NULL,
        strategy      TEXT NOT NULL,
        symbol        TEXT,
        ts            TEXT NOT NULL,
        action        TEXT,
        strength      REAL,
        filtered_by   TEXT,
        mit_passed    INTEGER,
        ev_value      REAL,
        features_json TEXT,
        created_at    TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS anomaly_events (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_name     TEXT NOT NULL,
        strategy     TEXT NOT NULL,
        ts           TEXT NOT NULL,
        check_name   TEXT NOT NULL,
        severity     TEXT NOT NULL,
        symbol       TEXT,
        message      TEXT,
        context_json TEXT,
        created_at   TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS bot_heartbeat (
        bot_name             TEXT NOT NULL,
        strategy             TEXT NOT NULL,
        last_bar_ts          TEXT,
        last_watchdog_ts     TEXT,
        last_bar_lag_ms      REAL,
        broker_connected     INTEGER NOT NULL DEFAULT 0,
        broker_adapter       TEXT    NOT NULL DEFAULT '',
        circuit_breaker      INTEGER NOT NULL DEFAULT 0,
        symbol_status_json   TEXT    NOT NULL DEFAULT '{}',
        updated_at           TEXT NOT NULL,
        PRIMARY KEY (bot_name, strategy)
    )
    """,
)

_INDEX_STATEMENTS: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_trades_bot_strategy_entry_ts "
    "ON trades(bot_name, strategy, entry_ts)",
    "CREATE INDEX IF NOT EXISTS idx_trades_bot_strategy_exit_ts "
    "ON trades(bot_name, strategy, exit_ts)",
    "CREATE INDEX IF NOT EXISTS idx_trades_symbol_entry_ts "
    "ON trades(symbol, entry_ts)",
    "CREATE INDEX IF NOT EXISTS idx_trades_bot_strategy_exit_day "
    "ON trades(bot_name, strategy, substr(exit_ts, 1, 10))",
    "CREATE INDEX IF NOT EXISTS idx_equity_bot_strategy_ts "
    "ON equity_snapshots(bot_name, strategy, ts)",
    "CREATE INDEX IF NOT EXISTS idx_positions_bot_strategy_symbol "
    "ON positions(bot_name, strategy, symbol)",
    "CREATE INDEX IF NOT EXISTS idx_signals_bot_strategy_ts "
    "ON signals(bot_name, strategy, ts)",
    "CREATE INDEX IF NOT EXISTS idx_anomaly_bot_strategy_ts "
    "ON anomaly_events(bot_name, strategy, ts)",
    "CREATE INDEX IF NOT EXISTS idx_heartbeat_updated_at "
    "ON bot_heartbeat(updated_at)",
)


class PersistentState:
    """SQLite-backed State – zentral für alle Bot-Instanzen."""

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

    # ── Schema ────────────────────────────────────────────────────────

    async def ensure_schema(self) -> None:
        """Idempotent: PRAGMAs + CREATE TABLE IF NOT EXISTS + Spalten-Migration + Indexes.

        Reihenfolge ist kritisch:
          1. CREATE TABLE IF NOT EXISTS  – erzeugt fehlende Tabellen
          2. Spalten-Migration           – fügt fehlende Spalten hinzu (inkl. bot_name)
          3. CREATE INDEX IF NOT EXISTS  – Indexes referenzieren ggf. neue Spalten
        """
        async with self._lock:
            if self._schema_ready:
                return
            async with self._conn() as conn:
                await conn.execute("PRAGMA journal_mode=WAL")
                await conn.execute("PRAGMA synchronous=NORMAL")
                await conn.execute("PRAGMA foreign_keys=ON")

                # ── Schritt 1: Tabellen erzeugen (bereits existierende werden übersprungen)
                for stmt in _SCHEMA_STATEMENTS:
                    await conn.execute(stmt)

                # ── Schritt 2: Spalten-Migrationen (vor Indexes!) ─────────────────────
                # bot_name-Migration: In bestehenden DBs ohne bot_name würden die Indexes
                # auf ON trades(bot_name, ...) mit "no such column" fehlschlagen.
                # ALTER TABLE ADD COLUMN bot_name TEXT NOT NULL DEFAULT '' ist sicher:
                # bestehende Zeilen erhalten bot_name='', neue schreiben den echten Wert.
                _bot_name_tables = (
                    "trades", "equity_snapshots", "positions",
                    "signals", "anomaly_events",
                    "account", "cooldowns", "reserved_groups", "bot_heartbeat",
                )
                for tbl in _bot_name_tables:
                    tbl_cur = await conn.execute(f"PRAGMA table_info({tbl})")
                    tbl_cols = {row[1] for row in await tbl_cur.fetchall()}
                    if "bot_name" not in tbl_cols:
                        await conn.execute(
                            f"ALTER TABLE {tbl} ADD COLUMN bot_name TEXT NOT NULL DEFAULT ''"
                        )
                        log.info("state.migrate_bot_name", table=tbl)

                # trades: Spalten-Umbenennungen / Ergänzungen
                cur = await conn.execute("PRAGMA table_info(trades)")
                cols = {row[1] for row in await cur.fetchall()}
                if "reason" in cols and "exit_reason" not in cols:
                    await conn.execute(
                        "ALTER TABLE trades RENAME COLUMN reason TO exit_reason"
                    )
                if "exit_signal" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN exit_signal TEXT")
                if "exit_strength" not in cols:
                    await conn.execute("ALTER TABLE trades ADD COLUMN exit_strength REAL")
                if "exit_features_json" not in cols:
                    await conn.execute(
                        "ALTER TABLE trades ADD COLUMN exit_features_json TEXT"
                    )

                # bot_heartbeat: last_watchdog_ts
                hb_cur = await conn.execute("PRAGMA table_info(bot_heartbeat)")
                hb_cols = {row[1] for row in await hb_cur.fetchall()}
                if "last_watchdog_ts" not in hb_cols:
                    await conn.execute(
                        "ALTER TABLE bot_heartbeat ADD COLUMN last_watchdog_ts TEXT"
                    )

                # ── Schritt 3: Indexes (jetzt existieren alle referenzierten Spalten) ──
                for stmt in _INDEX_STATEMENTS:
                    await conn.execute(stmt)

                await conn.commit()
            self._schema_ready = True
        log.info("state.schema_ready", path=str(self.db_path))

    async def init(self) -> None:
        await self.ensure_schema()

    async def close(self) -> None:
        return None

    # ══════════════════════════════════════════════════════════════════
    # Trade Lifecycle
    # ══════════════════════════════════════════════════════════════════

    async def open_trade_atomic(
        self,
        *,
        bot_name: str,
        strategy: str,
        symbol: str,
        side: str,
        entry_ts: datetime | str,
        entry_price: float,
        qty: float,
        stop_price: float | None = None,
        signal_strength: float | None = None,
        mit_qty_factor: float | None = None,
        ev_estimate: float | None = None,
        group_name: str | None = None,
        features_json: str | None = None,
        reason: str | None = None,
        current_price: float | None = None,
        entry_signal: str | None = None,
        broker_order_id: str | None = None,
        order_reference: str | None = None,
        reserve_group_name: str | None = None,
        reserve_day: date | None = None,
    ) -> int:
        """Atomare Eröffnung: trades INSERT + positions UPSERT + optional reserved_groups
        in einer einzigen Transaktion. Gibt trade_id zurück."""
        ts = _iso(entry_ts)
        now = _iso(datetime.now(timezone.utc))
        c_price = current_price if current_price is not None else float(entry_price)

        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    """
                    INSERT INTO trades (
                        bot_name, strategy, symbol, side, entry_ts,
                        entry_price, qty, stop_price, signal_strength,
                        mit_qty_factor, ev_estimate, group_name,
                        features_json, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        bot_name, strategy, symbol, side, ts,
                        float(entry_price), float(qty), stop_price,
                        signal_strength, mit_qty_factor, ev_estimate,
                        group_name, features_json, now,
                    ),
                )
                trade_id = int(cur.lastrowid or 0)

                pos_row = await (
                    await conn.execute(
                        "SELECT id FROM positions "
                        "WHERE bot_name=? AND strategy=? AND symbol=?",
                        (bot_name, strategy, symbol),
                    )
                ).fetchone()
                if pos_row is None:
                    await conn.execute(
                        """
                        INSERT INTO positions (
                            trade_id, bot_name, strategy, symbol, side,
                            entry_ts, entry_price, qty, stop_price,
                            last_update_ts, current_price,
                            unrealized_pnl, unrealized_pnl_pct, held_minutes,
                            entry_signal, entry_reason,
                            broker_order_id, order_reference, created_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            trade_id, bot_name, strategy, symbol, side,
                            ts, float(entry_price), float(qty), stop_price,
                            now, c_price, 0.0, 0.0, 0,
                            entry_signal, reason,
                            broker_order_id, order_reference, now,
                        ),
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE positions SET
                            trade_id=?, side=?, entry_ts=?,
                            entry_price=?, qty=?, stop_price=?,
                            last_update_ts=?, current_price=?,
                            unrealized_pnl=0.0, unrealized_pnl_pct=0.0,
                            held_minutes=0,
                            entry_signal=COALESCE(?, entry_signal),
                            entry_reason=COALESCE(?, entry_reason),
                            broker_order_id=COALESCE(?, broker_order_id),
                            order_reference=COALESCE(?, order_reference)
                        WHERE id=?
                        """,
                        (
                            trade_id, side, ts, float(entry_price),
                            float(qty), stop_price, now, c_price,
                            entry_signal, reason,
                            broker_order_id, order_reference,
                            pos_row["id"],
                        ),
                    )

                if reserve_group_name and reserve_day is not None:
                    await conn.execute(
                        "INSERT OR IGNORE INTO reserved_groups "
                        "(group_name, day, bot_name, strategy) VALUES (?,?,?,?)",
                        (reserve_group_name, reserve_day.isoformat(),
                         bot_name, strategy),
                    )

                await conn.commit()

        log.info("state.trade_opened_atomic", bot_name=bot_name,
                 strategy=strategy, symbol=symbol, trade_id=trade_id)
        return trade_id

    async def close_trade_atomic(
        self,
        *,
        bot_name: str,
        strategy: str,
        trade_id: int | None = None,
        symbol: str | None = None,
        exit_ts: datetime | str,
        exit_price: float,
        pnl: float | None = None,
        pnl_pct: float | None = None,
        exit_reason: str | None = None,
        exit_signal: str | None = None,
        exit_strength: float | None = None,
        exit_features_json: str | None = None,
    ) -> None:
        """Atomarer Exit: trades UPDATE + positions DELETE in einer Transaktion."""
        ts = _iso(exit_ts)
        async with self._lock:
            async with self._conn() as conn:
                if trade_id is not None:
                    row = await (
                        await conn.execute(
                            "SELECT id, symbol FROM trades "
                            "WHERE id=? AND bot_name=? AND strategy=? "
                            "AND exit_ts IS NULL",
                            (int(trade_id), bot_name, strategy),
                        )
                    ).fetchone()
                else:
                    row = await (
                        await conn.execute(
                            """
                            SELECT id, symbol FROM trades
                            WHERE bot_name=? AND strategy=? AND symbol=?
                            AND exit_ts IS NULL
                            ORDER BY entry_ts DESC LIMIT 1
                            """,
                            (bot_name, strategy, symbol or ""),
                        )
                    ).fetchone()
                if row is None:
                    log.warning("state.close_trade_atomic_no_match",
                                bot_name=bot_name, strategy=strategy,
                                trade_id=trade_id, symbol=symbol)
                    return

                tid = int(row["id"])
                sym = row["symbol"]

                await conn.execute(
                    """
                    UPDATE trades
                    SET exit_ts=?, exit_price=?, pnl=?, pnl_pct=?,
                        exit_reason=?, exit_signal=?, exit_strength=?, exit_features_json=?
                    WHERE id=?
                    """,
                    (
                        ts, float(exit_price), pnl, pnl_pct,
                        exit_reason, exit_signal, exit_strength,
                        exit_features_json, tid,
                    ),
                )
                await conn.execute(
                    "DELETE FROM positions "
                    "WHERE bot_name=? AND strategy=? AND symbol=?",
                    (bot_name, strategy, sym),
                )
                await conn.commit()

        log.info("state.trade_closed_atomic", bot_name=bot_name,
                 strategy=strategy, trade_id=tid, pnl=pnl,
                 exit_reason=exit_reason, exit_signal=exit_signal)

    # ══════════════════════════════════════════════════════════════════
    # Equity
    # ══════════════════════════════════════════════════════════════════

    async def save_equity_snapshot(
        self,
        *,
        bot_name: str,
        strategy: str,
        ts: datetime | str,
        equity: float,
        cash: float = 0.0,
        drawdown_pct: float = 0.0,
        peak_equity: float = 0.0,
        unrealized_pnl_total: float = 0.0,
    ) -> None:
        ts_iso = _iso(ts)
        now = _iso(datetime.now(timezone.utc))
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO equity_snapshots
                    (ts, bot_name, strategy, equity, cash, drawdown_pct,
                     peak_equity, unrealized_pnl_total, created_at)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        ts_iso, bot_name, strategy, float(equity),
                        float(cash), float(drawdown_pct),
                        float(peak_equity), float(unrealized_pnl_total), now,
                    ),
                )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Positionen
    # ══════════════════════════════════════════════════════════════════

    async def update_or_create_position(
        self,
        *,
        bot_name: str,
        strategy: str,
        symbol: str,
        side: str | None = None,
        entry_ts: datetime | str | None = None,
        entry_price: float | None = None,
        qty: float | None = None,
        stop_price: float | None = None,
        current_price: float | None = None,
        unrealized_pnl: float | None = None,
        unrealized_pnl_pct: float | None = None,
        held_minutes: int | None = None,
        trade_id: int | None = None,
        entry_signal: str | None = None,
        entry_reason: str | None = None,
        broker_order_id: str | None = None,
        order_reference: str | None = None,
    ) -> None:
        now = _iso(datetime.now(timezone.utc))
        entry_iso = _iso(entry_ts) if entry_ts is not None else None
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "SELECT id FROM positions "
                    "WHERE bot_name=? AND strategy=? AND symbol=?",
                    (bot_name, strategy, symbol),
                )
                row = await cur.fetchone()
                if row is None:
                    await conn.execute(
                        """
                        INSERT INTO positions (
                            trade_id, bot_name, strategy, symbol, side,
                            entry_ts, entry_price, qty, stop_price,
                            last_update_ts, current_price,
                            unrealized_pnl, unrealized_pnl_pct, held_minutes,
                            entry_signal, entry_reason,
                            broker_order_id, order_reference, created_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            trade_id, bot_name, strategy, symbol, side,
                            entry_iso, entry_price, qty, stop_price,
                            now, current_price, unrealized_pnl,
                            unrealized_pnl_pct, held_minutes,
                            entry_signal, entry_reason,
                            broker_order_id, order_reference, now,
                        ),
                    )
                else:
                    await conn.execute(
                        """
                        UPDATE positions SET
                            trade_id=COALESCE(?, trade_id),
                            side=COALESCE(?, side),
                            entry_ts=COALESCE(?, entry_ts),
                            entry_price=COALESCE(?, entry_price),
                            qty=COALESCE(?, qty),
                            stop_price=COALESCE(?, stop_price),
                            last_update_ts=?,
                            current_price=COALESCE(?, current_price),
                            unrealized_pnl=COALESCE(?, unrealized_pnl),
                            unrealized_pnl_pct=COALESCE(?, unrealized_pnl_pct),
                            held_minutes=COALESCE(?, held_minutes),
                            entry_signal=COALESCE(?, entry_signal),
                            entry_reason=COALESCE(?, entry_reason),
                            broker_order_id=COALESCE(?, broker_order_id),
                            order_reference=COALESCE(?, order_reference)
                        WHERE id=?
                        """,
                        (
                            trade_id, side, entry_iso, entry_price, qty,
                            stop_price, now, current_price, unrealized_pnl,
                            unrealized_pnl_pct, held_minutes,
                            entry_signal, entry_reason,
                            broker_order_id, order_reference,
                            row["id"],
                        ),
                    )
                await conn.commit()

    async def remove_position(
        self, *, bot_name: str, strategy: str, symbol: str,
    ) -> None:
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    "DELETE FROM positions "
                    "WHERE bot_name=? AND strategy=? AND symbol=?",
                    (bot_name, strategy, symbol),
                )
                await conn.commit()

    async def get_open_positions(
        self, bot_name: str, strategy: str,
    ) -> list[dict[str, Any]]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM positions WHERE bot_name=? AND strategy=?",
                (bot_name, strategy),
            )
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    # ══════════════════════════════════════════════════════════════════
    # Signale
    # ══════════════════════════════════════════════════════════════════

    async def save_signal(
        self,
        *,
        bot_name: str,
        strategy: str,
        ts: datetime | str,
        symbol: str | None = None,
        action: str | None = None,
        strength: float | None = None,
        filtered_by: str | None = None,
        mit_passed: bool | None = None,
        ev_value: float | None = None,
        features_json: str | None = None,
    ) -> int:
        ts_iso = _iso(ts)
        now = _iso(datetime.now(timezone.utc))
        mit_int: int | None = None if mit_passed is None else int(bool(mit_passed))
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    """
                    INSERT INTO signals (
                        bot_name, strategy, symbol, ts, action, strength,
                        filtered_by, mit_passed, ev_value, features_json,
                        created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        bot_name, strategy, symbol, ts_iso, action,
                        strength, filtered_by, mit_int, ev_value,
                        features_json, now,
                    ),
                )
                await conn.commit()
                return int(cur.lastrowid or 0)

    # ══════════════════════════════════════════════════════════════════
    # Heartbeat
    # ══════════════════════════════════════════════════════════════════

    async def upsert_bot_heartbeat(
        self,
        *,
        bot_name: str,
        strategy: str,
        last_bar_ts: datetime | str | None = None,
        last_watchdog_ts: datetime | str | None = None,
        last_bar_lag_ms: float | None = None,
        broker_connected: bool = False,
        broker_adapter: str = "",
        circuit_breaker: bool = False,
        symbol_status: dict | None = None,
    ) -> None:
        last_bar_ts_iso: str | None = None
        if last_bar_ts is not None:
            last_bar_ts_iso = (
                last_bar_ts if isinstance(last_bar_ts, str) else _iso(last_bar_ts)
            )
        last_watchdog_ts_iso: str | None = None
        if last_watchdog_ts is not None:
            last_watchdog_ts_iso = (
                last_watchdog_ts
                if isinstance(last_watchdog_ts, str)
                else _iso(last_watchdog_ts)
            )
        status_json = "{}"
        if symbol_status is not None:
            try:
                status_json = json.dumps(symbol_status, default=str)
            except (TypeError, ValueError):
                status_json = "{}"
        now = _iso(datetime.now(timezone.utc))
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    """
                    INSERT INTO bot_heartbeat (
                        bot_name, strategy, last_bar_ts, last_bar_lag_ms,
                        last_watchdog_ts,
                        broker_connected, broker_adapter,
                        circuit_breaker, symbol_status_json, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(bot_name, strategy) DO UPDATE SET
                        last_bar_ts=COALESCE(excluded.last_bar_ts, bot_heartbeat.last_bar_ts),
                        last_bar_lag_ms=COALESCE(excluded.last_bar_lag_ms, bot_heartbeat.last_bar_lag_ms),
                        last_watchdog_ts=COALESCE(excluded.last_watchdog_ts, bot_heartbeat.last_watchdog_ts),
                        broker_connected=excluded.broker_connected,
                        broker_adapter=excluded.broker_adapter,
                        circuit_breaker=excluded.circuit_breaker,
                        symbol_status_json=excluded.symbol_status_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        bot_name, strategy, last_bar_ts_iso,
                        float(last_bar_lag_ms) if last_bar_lag_ms is not None else None,
                        last_watchdog_ts_iso,
                        int(bool(broker_connected)),
                        str(broker_adapter),
                        int(bool(circuit_breaker)),
                        status_json, now,
                    ),
                )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Anomalien
    # ══════════════════════════════════════════════════════════════════

    async def log_anomaly(self, event: AnomalyEvent) -> None:
        ctx_json: str | None = None
        try:
            ctx_json = json.dumps(event.context, default=str)
        except (TypeError, ValueError):
            ctx_json = None
        sev = event.severity.value if hasattr(event.severity, "value") else str(event.severity)
        now = _iso(datetime.now(timezone.utc))
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    """
                    INSERT INTO anomaly_events (
                        bot_name, strategy, ts, check_name, severity,
                        symbol, message, context_json, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event.bot_name, event.strategy or "",
                        _iso(event.timestamp), event.check_name,
                        sev, event.symbol, event.message,
                        ctx_json, now,
                    ),
                )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Account-Peak
    # ══════════════════════════════════════════════════════════════════

    async def update_peak_equity(
        self, bot_name: str, strategy: str, equity: float,
    ) -> float:
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "SELECT value FROM account "
                    "WHERE key='peak_equity' AND bot_name=? AND strategy=?",
                    (bot_name, strategy),
                )
                row = await cur.fetchone()
                current_peak = float(row["value"]) if row else 0.0
                new_peak = max(current_peak, float(equity))
                if new_peak != current_peak:
                    await conn.execute(
                        "INSERT OR REPLACE INTO account "
                        "(key, bot_name, strategy, value) "
                        "VALUES ('peak_equity',?,?,?)",
                        (bot_name, strategy, new_peak),
                    )
                    await conn.commit()
                return new_peak

    async def get_peak_equity(self, bot_name: str, strategy: str) -> float:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT value FROM account "
                "WHERE key='peak_equity' AND bot_name=? AND strategy=?",
                (bot_name, strategy),
            )
            row = await cur.fetchone()
        return float(row["value"]) if row else 0.0

    # ══════════════════════════════════════════════════════════════════
    # Cooldowns
    # ══════════════════════════════════════════════════════════════════

    async def set_cooldown(
        self, *, bot_name: str, strategy: str, symbol: str, until: datetime,
    ) -> None:
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    "INSERT OR REPLACE INTO cooldowns "
                    "(symbol, bot_name, strategy, until_ts) VALUES (?,?,?,?)",
                    (symbol, bot_name, strategy,
                     until.astimezone(timezone.utc).isoformat()),
                )
                await conn.commit()

    async def get_cooldowns(
        self, bot_name: str, strategy: str,
    ) -> dict[str, datetime]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT symbol, until_ts FROM cooldowns "
                "WHERE bot_name=? AND strategy=?",
                (bot_name, strategy),
            )
            rows = await cur.fetchall()
        out: dict[str, datetime] = {}
        for r in rows:
            try:
                out[r["symbol"]] = datetime.fromisoformat(r["until_ts"])
            except ValueError:
                continue
        return out

    async def is_in_cooldown(
        self, *, bot_name: str, strategy: str, symbol: str, now: datetime,
    ) -> bool:
        cools = await self.get_cooldowns(bot_name, strategy)
        until = cools.get(symbol)
        if until is None:
            return False
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        return now < until

    async def clear_expired_cooldowns(
        self, bot_name: str, strategy: str, now: datetime,
    ) -> int:
        until_ts = now.astimezone(timezone.utc).isoformat()
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "DELETE FROM cooldowns "
                    "WHERE bot_name=? AND strategy=? AND until_ts <= ?",
                    (bot_name, strategy, until_ts),
                )
                await conn.commit()
                return cur.rowcount

    # ══════════════════════════════════════════════════════════════════
    # Reserved Groups (MIT-Independence)
    # ══════════════════════════════════════════════════════════════════

    async def reserve_group(
        self, *, bot_name: str, strategy: str, group: str, day: date,
    ) -> None:
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    "INSERT OR IGNORE INTO reserved_groups "
                    "(group_name, day, bot_name, strategy) VALUES (?,?,?,?)",
                    (group, day.isoformat(), bot_name, strategy),
                )
                await conn.commit()

    async def reserved_groups(
        self, bot_name: str, strategy: str, day: date,
    ) -> set[str]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT group_name FROM reserved_groups "
                "WHERE bot_name=? AND strategy=? AND day=?",
                (bot_name, strategy, day.isoformat()),
            )
            rows = await cur.fetchall()
        return {r["group_name"] for r in rows}

    async def reset_day(
        self, bot_name: str, strategy: str, day: date,
    ) -> None:
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    "DELETE FROM reserved_groups "
                    "WHERE bot_name=? AND strategy=? AND day=?",
                    (bot_name, strategy, day.isoformat()),
                )
                await conn.commit()

    # ══════════════════════════════════════════════════════════════════
    # Reader
    # ══════════════════════════════════════════════════════════════════

    async def get_trades(
        self,
        *,
        bot_name: str,
        strategy: str,
        symbol: str | None = None,
        since: datetime | str | None = None,
        until: datetime | str | None = None,
        only_closed: bool = False,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = ["bot_name=?", "strategy=?"]
        params: list[Any] = [bot_name, strategy]
        if symbol:
            where.append("symbol=?"); params.append(symbol)
        if since is not None:
            where.append("entry_ts >= ?"); params.append(_iso(since))
        if until is not None:
            where.append("entry_ts <= ?"); params.append(_iso(until))
        if only_closed:
            where.append("exit_ts IS NOT NULL")
        sql = "SELECT * FROM trades WHERE " + " AND ".join(where)
        sql += " ORDER BY entry_ts DESC"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        async with self._conn() as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_signals(
        self,
        *,
        bot_name: str,
        strategy: str,
        symbol: str | None = None,
        filtered_only: bool = False,
        since: datetime | str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = ["bot_name=?", "strategy=?"]
        params: list[Any] = [bot_name, strategy]
        if symbol:
            where.append("symbol=?"); params.append(symbol)
        if filtered_only:
            where.append("filtered_by IS NOT NULL AND filtered_by != ''")
        if since is not None:
            where.append("ts >= ?"); params.append(_iso(since))
        sql = "SELECT * FROM signals WHERE " + " AND ".join(where)
        sql += " ORDER BY ts DESC"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        async with self._conn() as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_anomalies(
        self,
        *,
        bot_name: str,
        strategy: str,
        severity: str | None = None,
        since: datetime | str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        where: list[str] = ["bot_name=?", "strategy=?"]
        params: list[Any] = [bot_name, strategy]
        if severity:
            where.append("severity=?"); params.append(severity)
        if since is not None:
            where.append("ts >= ?"); params.append(_iso(since))
        sql = "SELECT * FROM anomaly_events WHERE " + " AND ".join(where)
        sql += " ORDER BY ts DESC"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        async with self._conn() as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_latest_equity_curve(
        self, bot_name: str, strategy: str, limit: int = 500,
    ) -> list[dict[str, Any]]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT * FROM equity_snapshots "
                "WHERE bot_name=? AND strategy=? "
                "ORDER BY ts DESC LIMIT ?",
                (bot_name, strategy, int(limit)),
            )
            rows = await cur.fetchall()
        return list(reversed([dict(r) for r in rows]))

    async def daily_pnl(
        self, bot_name: str, strategy: str, day: date,
    ) -> float:
        async with self._conn() as conn:
            cur = await conn.execute(
                """
                SELECT COALESCE(SUM(pnl), 0.0) AS s FROM trades
                WHERE bot_name=? AND strategy=? AND exit_ts IS NOT NULL
                AND substr(exit_ts, 1, 10)=?
                """,
                (bot_name, strategy, day.isoformat()),
            )
            row = await cur.fetchone()
        return float(row["s"]) if row else 0.0

    async def trades_today(
        self, bot_name: str, strategy: str, day: date,
    ) -> dict[str, int]:
        async with self._conn() as conn:
            cur = await conn.execute(
                """
                SELECT symbol, COUNT(*) AS cnt FROM trades
                WHERE bot_name=? AND strategy=? AND exit_ts IS NOT NULL
                AND substr(exit_ts, 1, 10)=?
                GROUP BY symbol
                """,
                (bot_name, strategy, day.isoformat()),
            )
            rows = await cur.fetchall()
        return {r["symbol"]: int(r["cnt"]) for r in rows}

    # ══════════════════════════════════════════════════════════════════
    # Monitoring
    # ══════════════════════════════════════════════════════════════════

    async def get_health_snapshot(
        self, bot_name: str, strategy: str,
    ) -> dict[str, Any]:
        """Alle Monitoring-Daten für einen Bot in einem DB-Roundtrip."""
        today = datetime.now(timezone.utc).date().isoformat()
        one_hour_ago = _iso(datetime.now(timezone.utc) - timedelta(hours=1))
        async with self._conn() as conn:
            eq = await (
                await conn.execute(
                    "SELECT equity, cash, drawdown_pct, peak_equity, ts "
                    "FROM equity_snapshots WHERE bot_name=? AND strategy=? "
                    "ORDER BY ts DESC LIMIT 1",
                    (bot_name, strategy),
                )
            ).fetchone()
            opn = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM positions "
                    "WHERE bot_name=? AND strategy=?",
                    (bot_name, strategy),
                )
            ).fetchone()
            td = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c, COALESCE(SUM(pnl), 0.0) AS pnl
                    FROM trades WHERE bot_name=? AND strategy=?
                    AND exit_ts IS NOT NULL AND substr(exit_ts, 1, 10)=?
                    """,
                    (bot_name, strategy, today),
                )
            ).fetchone()
            sigs = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM signals "
                    "WHERE bot_name=? AND strategy=? AND substr(ts,1,10)=?",
                    (bot_name, strategy, today),
                )
            ).fetchone()
            sigs_filtered = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM signals
                    WHERE bot_name=? AND strategy=? AND substr(ts,1,10)=?
                    AND filtered_by IS NOT NULL AND filtered_by != ''
                    """,
                    (bot_name, strategy, today),
                )
            ).fetchone()
            anom = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM anomaly_events "
                    "WHERE bot_name=? AND strategy=? AND ts >= ?",
                    (bot_name, strategy, one_hour_ago),
                )
            ).fetchone()
            hb = None
            try:
                hb = await (
                    await conn.execute(
                        "SELECT * FROM bot_heartbeat "
                        "WHERE bot_name=? AND strategy=?",
                        (bot_name, strategy),
                    )
                ).fetchone()
            except Exception:
                pass

        symbol_status: dict[str, Any] = {}
        if hb and hb["symbol_status_json"]:
            try:
                parsed = json.loads(hb["symbol_status_json"])
                if isinstance(parsed, dict):
                    symbol_status = parsed
            except (TypeError, ValueError):
                pass

        return {
            "bot_name": bot_name,
            "strategy": strategy,
            "equity": float(eq["equity"]) if eq else None,
            "cash": float(eq["cash"]) if eq else None,
            "drawdown_pct": float(eq["drawdown_pct"]) if eq else None,
            "peak_equity": float(eq["peak_equity"]) if eq else None,
            "last_equity_ts": eq["ts"] if eq else None,
            "last_bar_ts": hb["last_bar_ts"] if hb else None,
            "last_watchdog_ts": hb["last_watchdog_ts"] if hb else None,
            "last_bar_lag_ms": (
                float(hb["last_bar_lag_ms"])
                if hb and hb["last_bar_lag_ms"] is not None else None
            ),
            "broker_connected": bool(hb["broker_connected"]) if hb else False,
            "broker_adapter": hb["broker_adapter"] if hb else "",
            "circuit_breaker": bool(hb["circuit_breaker"]) if hb else False,
            "symbol_status": symbol_status,
            "bot_last_seen": hb["updated_at"] if hb else None,
            "open_positions": int(opn["c"]) if opn else 0,
            "trades_today": int(td["c"]) if td else 0,
            "pnl_today": float(td["pnl"]) if td else 0.0,
            "signals_today": int(sigs["c"]) if sigs else 0,
            "signals_filtered_today": int(sigs_filtered["c"]) if sigs_filtered else 0,
            "anomalies_last_hour": int(anom["c"]) if anom else 0,
        }

    async def get_strategy_status(
        self, bot_name: str, strategy: str,
    ) -> dict[str, Any]:
        """Aggregierter Bot-Status für das Dashboard."""
        today = datetime.now(timezone.utc).date()
        async with self._conn() as conn:
            eq = await (
                await conn.execute(
                    "SELECT equity, drawdown_pct, peak_equity, ts "
                    "FROM equity_snapshots WHERE bot_name=? AND strategy=? "
                    "ORDER BY ts DESC LIMIT 1",
                    (bot_name, strategy),
                )
            ).fetchone()
            opn = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM positions "
                    "WHERE bot_name=? AND strategy=?",
                    (bot_name, strategy),
                )
            ).fetchone()
            todays = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c, COALESCE(SUM(pnl), 0.0) AS pnl
                    FROM trades WHERE bot_name=? AND strategy=?
                    AND exit_ts IS NOT NULL AND substr(exit_ts, 1, 10)=?
                    """,
                    (bot_name, strategy, today.isoformat()),
                )
            ).fetchone()
        return {
            "bot_name": bot_name,
            "strategy": strategy,
            "equity": float(eq["equity"]) if eq else None,
            "drawdown_pct": float(eq["drawdown_pct"]) if eq else None,
            "peak_equity": float(eq["peak_equity"]) if eq else None,
            "last_equity_ts": eq["ts"] if eq else None,
            "open_positions": int(opn["c"]) if opn else 0,
            "trades_today": int(todays["c"]) if todays else 0,
            "pnl_today": float(todays["pnl"]) if todays else 0.0,
        }

    async def get_bot_heartbeats(
        self,
        active_only: bool = False,
        active_threshold_seconds: int = 180,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM bot_heartbeat"
        params: list[Any] = []
        if active_only:
            threshold_ts = _iso(
                datetime.now(timezone.utc)
                - timedelta(seconds=active_threshold_seconds)
            )
            sql += " WHERE updated_at > ?"
            params.append(threshold_ts)
        sql += " ORDER BY updated_at DESC"
        try:
            async with self._conn() as conn:
                cur = await conn.execute(sql, params)
                rows = await cur.fetchall()
        except Exception:
            return []
        out: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            status: dict[str, Any] = {}
            raw = item.get("symbol_status_json")
            if raw:
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, dict):
                        status = parsed
                except (TypeError, ValueError):
                    pass
            item["broker_connected"] = bool(item.get("broker_connected", 0))
            item["circuit_breaker"] = bool(item.get("circuit_breaker", 0))
            item["symbol_status"] = status
            out.append(item)
        return out

    async def get_liveness_view(self) -> list[dict[str, Any]]:
        """Aggregierte Sicht auf Heartbeat + Portfolio pro Bot-Instanz."""
        instances = await self.get_bot_instances()
        out: list[dict[str, Any]] = []
        for inst in instances:
            bn = inst["bot_name"]
            strat = inst["strategy"]
            snap = await self.get_health_snapshot(bn, strat)
            out.append({
                "bot_name": bn,
                "strategy": strat,
                "last_watchdog_ts": snap.get("last_watchdog_ts"),
                "last_bar_ts": snap.get("last_bar_ts"),
                "last_bar_lag_ms": snap.get("last_bar_lag_ms"),
                "bot_last_seen": snap.get("bot_last_seen"),
                "broker_connected": bool(snap.get("broker_connected", False)),
                "circuit_breaker": bool(snap.get("circuit_breaker", False)),
                "equity": snap.get("equity"),
                "peak_equity": snap.get("peak_equity"),
                "drawdown_pct": snap.get("drawdown_pct"),
                "open_positions": int(snap.get("open_positions", 0) or 0),
                "signals_today": int(snap.get("signals_today", 0) or 0),
                "signals_filtered_today": int(snap.get("signals_filtered_today", 0) or 0),
                "anomalies_last_hour": int(snap.get("anomalies_last_hour", 0) or 0),
            })
        return out

    async def get_bot_instances(self) -> list[dict[str, str]]:
        """Alle bekannten (bot_name, strategy)-Paare aus allen Tabellen."""
        async with self._conn() as conn:
            cur = await conn.execute(
                """
                SELECT DISTINCT bot_name, strategy FROM (
                    SELECT bot_name, strategy FROM trades
                    UNION SELECT bot_name, strategy FROM equity_snapshots
                    UNION SELECT bot_name, strategy FROM positions
                    UNION SELECT bot_name, strategy FROM bot_heartbeat
                )
                """
            )
            rows = await cur.fetchall()
        return [{"bot_name": r["bot_name"], "strategy": r["strategy"]} for r in rows]


# ── Helpers ───────────────────────────────────────────────────────────

def _iso(ts: datetime | str) -> str:
    """Normalisiert Timestamps zu ISO-8601 UTC."""
    if isinstance(ts, str):
        return ts
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat()
