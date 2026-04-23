"""PersistentState – zentrale SQLite-Datenbank für alle Bots.

Single-Source-of-Truth für:
  - Trade-Historie (``trades``)
  - Equity-Zeitreihe (``equity_snapshots``)
  - Offene Positionen (``positions``)
  - Signal-Stream (``signals``)
  - Anomaly-Events (``anomaly_events``)
  - Tages-PnL-/Trade-Counter (``daily`` – read-only, abgeleitet aus trades)
  - Account-Peak (``account``, strategy-aware)
  - Cooldowns (``cooldowns``, strategy-aware)
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

Atomare Schreib-Sequenzen: ``open_trade_atomic`` und ``close_trade_atomic``
bündeln jeweils Trade-INSERT/UPDATE + Position-UPSERT/DELETE + ggf.
reserved_groups in einer einzigen DB-Transaktion (alles oder nichts).
"""
from __future__ import annotations

import asyncio
import json
import warnings
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
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
        key      TEXT NOT NULL,
        strategy TEXT NOT NULL DEFAULT '',
        value    REAL,
        PRIMARY KEY (key, strategy)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS cooldowns (
        symbol   TEXT NOT NULL,
        strategy TEXT NOT NULL DEFAULT '',
        until_ts TEXT NOT NULL,
        PRIMARY KEY (symbol, strategy)
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
        bot_name TEXT,
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
        trade_id INTEGER REFERENCES trades(id) ON DELETE SET NULL,
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
        entry_signal TEXT,
        entry_reason TEXT,
        broker_order_id TEXT,
        order_reference TEXT,
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
    """
    CREATE TABLE IF NOT EXISTS bot_heartbeat (
        strategy TEXT PRIMARY KEY,
        bot_name TEXT,
        last_bar_ts TEXT,
        last_bar_lag_ms REAL,
        broker_connected INTEGER NOT NULL DEFAULT 0,
        broker_adapter TEXT NOT NULL DEFAULT '',
        circuit_breaker INTEGER NOT NULL DEFAULT 0,
        symbol_status_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
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
    "CREATE INDEX IF NOT EXISTS idx_bot_heartbeat_updated_at "
    "ON bot_heartbeat(updated_at)",
    # Neuer Index für daily_pnl/trades_today-Queries auf trades-Tabelle
    "CREATE INDEX IF NOT EXISTS idx_trades_strategy_exit_day "
    "ON trades(strategy, substr(exit_ts,1,10))",
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
                # reserved_groups: strategy-Spalte + neuer PK (ephemere Daten).
                await self._migrate_reserved_groups(conn)
                # cooldowns: strategy-Spalte hinzufügen (PK → composite).
                await self._migrate_cooldowns(conn)
                # account: strategy-Spalte hinzufügen (PK → composite).
                await self._migrate_account(conn)
                # positions: trade_id FK hinzufügen.
                await self._migrate_positions_trade_id(conn)
                # positions: Zusatz-Metadaten (Signal/Reason/Refs) hinzufügen.
                await self._migrate_positions_metadata(conn)
                # trades/bot_heartbeat: bot_name-Spalte hinzufügen.
                await self._migrate_trade_bot_name(conn)
                await self._migrate_heartbeat_bot_name(conn)

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

    async def _migrate_reserved_groups(self, conn: Any) -> None:
        """reserved_groups ohne strategy-Spalte → mit strategy + neuem PK.

        Die Tabelle enthält nur tagesaktuelle Daten (MIT-Independence-
        Gruppen) – ein Datenverlust durch DROP/RECREATE ist unkritisch.
        """
        cur = await conn.execute("PRAGMA table_info(reserved_groups)")
        cols = {row[1] for row in await cur.fetchall()}
        if not cols:
            return  # Tabelle existiert noch nicht – normaler CREATE-Pfad
        if "strategy" in cols:
            return  # bereits migriert

        log.info("state.migrate_reserved_groups_add_strategy")
        # reserved_groups ist ephemer (täglich gecleart) – einfaches
        # DROP/RECREATE ohne Datenmigration ist sicher.
        await conn.execute("DROP TABLE reserved_groups")

    async def _migrate_cooldowns(self, conn: Any) -> None:
        """cooldowns (PK=symbol) → (symbol, strategy) ohne Datenverlust."""
        cur = await conn.execute("PRAGMA table_info(cooldowns)")
        cols = {row[1] for row in await cur.fetchall()}
        if not cols:
            return  # Tabelle existiert noch nicht
        if "strategy" in cols:
            return  # bereits migriert

        log.info("state.migrate_cooldowns_add_strategy")
        await conn.execute("ALTER TABLE cooldowns RENAME TO cooldowns_legacy")
        await conn.execute(
            """
            CREATE TABLE cooldowns (
                symbol   TEXT NOT NULL,
                strategy TEXT NOT NULL DEFAULT '',
                until_ts TEXT NOT NULL,
                PRIMARY KEY (symbol, strategy)
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO cooldowns (symbol, strategy, until_ts)
            SELECT symbol, '', until_ts FROM cooldowns_legacy
            """
        )
        await conn.execute("DROP TABLE cooldowns_legacy")

    async def _migrate_account(self, conn: Any) -> None:
        """account (PK=key) → (key, strategy) ohne Datenverlust."""
        cur = await conn.execute("PRAGMA table_info(account)")
        cols = {row[1] for row in await cur.fetchall()}
        if not cols:
            return  # Tabelle existiert noch nicht
        if "strategy" in cols:
            return  # bereits migriert

        log.info("state.migrate_account_add_strategy")
        await conn.execute("ALTER TABLE account RENAME TO account_legacy")
        await conn.execute(
            """
            CREATE TABLE account (
                key      TEXT NOT NULL,
                strategy TEXT NOT NULL DEFAULT '',
                value    REAL,
                PRIMARY KEY (key, strategy)
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO account (key, strategy, value)
            SELECT key, '', value FROM account_legacy
            """
        )
        await conn.execute("DROP TABLE account_legacy")

    async def _migrate_positions_trade_id(self, conn: Any) -> None:
        """positions: trade_id-Spalte hinzufügen (FK zu trades)."""
        cur = await conn.execute("PRAGMA table_info(positions)")
        cols = {row[1] for row in await cur.fetchall()}
        if not cols:
            return  # Tabelle existiert noch nicht
        if "trade_id" in cols:
            return  # bereits migriert

        log.info("state.migrate_positions_add_trade_id")
        # SQLite kann FK nicht inline per ALTER hinzufügen – rebuild.
        await conn.execute("ALTER TABLE positions RENAME TO positions_legacy")
        await conn.execute(
            """
            CREATE TABLE positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_id INTEGER REFERENCES trades(id) ON DELETE SET NULL,
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
                entry_signal TEXT,
                entry_reason TEXT,
                broker_order_id TEXT,
                order_reference TEXT,
                UNIQUE (strategy, symbol)
            )
            """
        )
        await conn.execute(
            """
            INSERT INTO positions (
                id, strategy, symbol, side, entry_ts, entry_price, qty,
                stop_price, last_update_ts, current_price,
                unrealized_pnl, unrealized_pnl_pct, held_minutes,
                entry_signal, entry_reason, broker_order_id, order_reference
            )
            SELECT id, strategy, symbol, side, entry_ts, entry_price, qty,
                   stop_price, last_update_ts, current_price,
                   unrealized_pnl, unrealized_pnl_pct, held_minutes,
                   NULL, NULL, NULL, NULL
            FROM positions_legacy
            """
        )
        await conn.execute("DROP TABLE positions_legacy")

    async def _migrate_positions_metadata(self, conn: Any) -> None:
        """positions: zusätzliche Entry-Metadaten-Spalten ergänzen."""
        cur = await conn.execute("PRAGMA table_info(positions)")
        cols = {row[1] for row in await cur.fetchall()}
        if not cols:
            return

        if "entry_signal" not in cols:
            await conn.execute("ALTER TABLE positions ADD COLUMN entry_signal TEXT")
        if "entry_reason" not in cols:
            await conn.execute("ALTER TABLE positions ADD COLUMN entry_reason TEXT")
        if "broker_order_id" not in cols:
            await conn.execute("ALTER TABLE positions ADD COLUMN broker_order_id TEXT")
        if "order_reference" not in cols:
            await conn.execute("ALTER TABLE positions ADD COLUMN order_reference TEXT")

    async def _migrate_trade_bot_name(self, conn: Any) -> None:
        """trades: bot_name-Spalte ergänzen."""
        cur = await conn.execute("PRAGMA table_info(trades)")
        cols = {row[1] for row in await cur.fetchall()}
        if cols and "bot_name" not in cols:
            await conn.execute("ALTER TABLE trades ADD COLUMN bot_name TEXT")

    async def _migrate_heartbeat_bot_name(self, conn: Any) -> None:
        """bot_heartbeat: bot_name-Spalte ergänzen."""
        cur = await conn.execute("PRAGMA table_info(bot_heartbeat)")
        cols = {row[1] for row in await cur.fetchall()}
        if cols and "bot_name" not in cols:
            await conn.execute("ALTER TABLE bot_heartbeat ADD COLUMN bot_name TEXT")

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
        bot_name: Optional[str] = None,
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
                        strategy, bot_name, symbol, side, entry_ts, entry_price, qty,
                        stop_price, signal_strength, mit_qty_factor,
                        ev_estimate, group_name, features_json, reason
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        strategy, bot_name, symbol, side, ts, float(entry_price),
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
        trade_id: Optional[int] = None,
        entry_signal: Optional[str] = None,
        entry_reason: Optional[str] = None,
        broker_order_id: Optional[str] = None,
        order_reference: Optional[str] = None,
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
                            trade_id, strategy, symbol, side, entry_ts,
                            entry_price, qty, stop_price, last_update_ts,
                            current_price, unrealized_pnl, unrealized_pnl_pct,
                            held_minutes, entry_signal, entry_reason,
                            broker_order_id, order_reference
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            trade_id, strategy, symbol, side, entry_iso,
                            entry_price, qty, stop_price, last,
                            current_price, unrealized_pnl,
                            unrealized_pnl_pct, held_minutes,
                            entry_signal, entry_reason,
                            broker_order_id, order_reference,
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
                            stop_price, last, current_price, unrealized_pnl,
                            unrealized_pnl_pct, held_minutes,
                            entry_signal, entry_reason,
                            broker_order_id, order_reference,
                            row["id"],
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

    async def upsert_bot_heartbeat(
        self,
        *,
        strategy: str,
        bot_name: str = "",
        last_bar_ts: Optional[datetime | str] = None,
        last_bar_lag_ms: Optional[float] = None,
        broker_connected: bool = False,
        broker_adapter: str = "",
        circuit_breaker: bool = False,
        symbol_status: Optional[dict] = None,
    ) -> None:
        """UPSERT der Heartbeat-Zeile fuer eine Strategie."""
        last_bar_ts_iso: Optional[str] = None
        if last_bar_ts is not None:
            if isinstance(last_bar_ts, str):
                last_bar_ts_iso = last_bar_ts
            else:
                last_bar_ts_iso = _iso(last_bar_ts)
        status_json = "{}"
        if symbol_status is not None:
            try:
                status_json = json.dumps(symbol_status, default=str)
            except (TypeError, ValueError):
                status_json = "{}"

        async with self._conn() as conn:
            await conn.execute(
                """
                INSERT OR REPLACE INTO bot_heartbeat (
                    strategy,
                    bot_name,
                    last_bar_ts,
                    last_bar_lag_ms,
                    broker_connected,
                    broker_adapter,
                    circuit_breaker,
                    symbol_status_json,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                """,
                (
                    strategy,
                    str(bot_name or ""),
                    last_bar_ts_iso,
                    float(last_bar_lag_ms) if last_bar_lag_ms is not None else None,
                    int(bool(broker_connected)),
                    str(broker_adapter or ""),
                    int(bool(circuit_breaker)),
                    status_json,
                ),
            )
            await conn.commit()

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
    # Writer: daily – DEPRECATED (daily-Tabelle wird nicht mehr befüllt,
    # alle Reads laufen jetzt direkt über trades-Tabelle)
    # ══════════════════════════════════════════════════════════════════

    async def update_daily_record(
        self,
        *,
        day: date,
        strategy: str,
        pnl_delta: float = 0.0,
        symbol: Optional[str] = None,
    ) -> None:
        """DEPRECATED – no-op Stub. Daten werden aus trades rekonstruiert."""
        warnings.warn(
            "update_daily_record() is deprecated – daily data is now "
            "derived from the trades table",
            DeprecationWarning,
            stacklevel=2,
        )

    # ── Legacy-API (bleibt für Abwärtskompatibilität) ───────────────

    async def add_trade(
        self, day: date, symbol: str, pnl: float, strategy: str = "",
    ) -> None:
        """DEPRECATED – no-op Stub."""
        warnings.warn(
            "add_trade() is deprecated – daily data is now "
            "derived from the trades table",
            DeprecationWarning,
            stacklevel=2,
        )

    async def daily_pnl(self, day: date, strategy: Optional[str] = None) -> float:
        """PnL eines Tages, abgeleitet aus der trades-Tabelle."""
        async with self._conn() as conn:
            if strategy is None:
                cur = await conn.execute(
                    """
                    SELECT COALESCE(SUM(pnl), 0.0) AS s FROM trades
                    WHERE exit_ts IS NOT NULL AND substr(exit_ts,1,10)=?
                    """,
                    (day.isoformat(),),
                )
            else:
                cur = await conn.execute(
                    """
                    SELECT COALESCE(SUM(pnl), 0.0) AS s FROM trades
                    WHERE strategy=? AND exit_ts IS NOT NULL
                    AND substr(exit_ts,1,10)=?
                    """,
                    (strategy, day.isoformat()),
                )
            row = await cur.fetchone()
            return float(row["s"]) if row else 0.0

    async def trades_today(
        self, day: date, strategy: Optional[str] = None,
    ) -> dict[str, int]:
        """Trade-Count pro Symbol eines Tages, abgeleitet aus trades-Tabelle."""
        async with self._conn() as conn:
            if strategy is None:
                cur = await conn.execute(
                    """
                    SELECT symbol, COUNT(*) AS cnt FROM trades
                    WHERE exit_ts IS NOT NULL AND substr(exit_ts,1,10)=?
                    GROUP BY symbol
                    """,
                    (day.isoformat(),),
                )
            else:
                cur = await conn.execute(
                    """
                    SELECT symbol, COUNT(*) AS cnt FROM trades
                    WHERE strategy=? AND exit_ts IS NOT NULL
                    AND substr(exit_ts,1,10)=?
                    GROUP BY symbol
                    """,
                    (strategy, day.isoformat()),
                )
            rows = await cur.fetchall()
        return {r["symbol"]: int(r["cnt"]) for r in rows}

    # ══════════════════════════════════════════════════════════════════
    # Account-Peak
    # ══════════════════════════════════════════════════════════════════

    async def update_peak_equity(self, equity: float,
                                strategy: str = "") -> float:
        async with self._lock:
            async with self._conn() as conn:
                cur = await conn.execute(
                    "SELECT value FROM account "
                    "WHERE key='peak_equity' AND strategy=?",
                    (strategy,),
                )
                row = await cur.fetchone()
                current_peak = float(row["value"]) if row else 0.0
                new_peak = max(current_peak, float(equity))
                if new_peak != current_peak:
                    await conn.execute(
                        "INSERT OR REPLACE INTO account(key, strategy, value) "
                        "VALUES ('peak_equity', ?, ?)",
                        (strategy, new_peak),
                    )
                    await conn.commit()
                return new_peak

    async def get_peak_equity(self, strategy: str = "") -> float:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT value FROM account "
                "WHERE key='peak_equity' AND strategy=?",
                (strategy,),
            )
            row = await cur.fetchone()
            return float(row["value"]) if row else 0.0

    # ══════════════════════════════════════════════════════════════════
    # Cooldowns
    # ══════════════════════════════════════════════════════════════════

    async def set_cooldown(self, symbol: str, until: datetime,
                          strategy: str = "") -> None:
        async with self._lock:
            async with self._conn() as conn:
                await conn.execute(
                    "INSERT OR REPLACE INTO cooldowns(symbol, strategy, until_ts) "
                    "VALUES (?,?,?)",
                    (symbol, strategy,
                     until.astimezone(timezone.utc).isoformat()),
                )
                await conn.commit()

    async def get_cooldowns(self, strategy: str = "") -> dict[str, datetime]:
        async with self._conn() as conn:
            cur = await conn.execute(
                "SELECT symbol, until_ts FROM cooldowns WHERE strategy=?",
                (strategy,),
            )
            rows = await cur.fetchall()
        out: dict[str, datetime] = {}
        for r in rows:
            try:
                out[r["symbol"]] = datetime.fromisoformat(r["until_ts"])
            except ValueError:
                continue
        return out

    async def is_in_cooldown(self, symbol: str, now: datetime,
                             strategy: str = "") -> bool:
        cools = await self.get_cooldowns(strategy=strategy)
        until = cools.get(symbol)
        if until is None:
            return False
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        return now < until

    async def clear_expired_cooldowns(self, now: datetime,
                                      strategy: str = "") -> int:
        cools = await self.get_cooldowns(strategy=strategy)
        expired = [s for s, u in cools.items()
                   if (u.tzinfo or timezone.utc) and now >= u]
        if not expired:
            return 0
        async with self._lock:
            async with self._conn() as conn:
                for s in expired:
                    await conn.execute(
                        "DELETE FROM cooldowns "
                        "WHERE symbol=? AND strategy=?",
                        (s, strategy),
                    )
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
        bot_name: Optional[str] = None,
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
        if bot_name:
            where.append("COALESCE(bot_name, strategy)=?"); params.append(bot_name)
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

    async def get_bot_heartbeats(
        self,
        active_only: bool = False,
        active_threshold_seconds: int = 180,
    ) -> list[dict[str, Any]]:
        """Liest Heartbeat-Zeilen, optional gefiltert auf aktive Bots."""
        sql = "SELECT * FROM bot_heartbeat"
        params: list[Any] = []
        if active_only:
            threshold = max(1, int(active_threshold_seconds))
            sql += " WHERE updated_at > datetime('now', ?)"
            params.append(f"-{threshold} seconds")
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
            raw_status = item.get("symbol_status_json")
            status: dict[str, Any] = {}
            if raw_status:
                try:
                    parsed = json.loads(raw_status)
                    if isinstance(parsed, dict):
                        status = parsed
                except (TypeError, ValueError):
                    status = {}
            item["broker_connected"] = bool(item.get("broker_connected", 0))
            item["circuit_breaker"] = bool(item.get("circuit_breaker", 0))
            item["symbol_status"] = status
            out.append(item)
        return out

    async def get_strategies(self) -> list[str]:
        """Liefert alle Strategien, die Trades oder Equity-Einträge haben."""
        async with self._conn() as conn:
            cur = await conn.execute(
                """
                SELECT DISTINCT strategy FROM (
                    SELECT strategy FROM trades
                    UNION SELECT strategy FROM equity_snapshots
                    UNION SELECT strategy FROM positions
                    UNION SELECT strategy FROM bot_heartbeat
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
            heartbeat = await (
                await conn.execute(
                    "SELECT bot_name FROM bot_heartbeat WHERE strategy=?",
                    (strategy,),
                )
            ).fetchone()
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
            "bot_name": (
                heartbeat["bot_name"] if heartbeat and heartbeat["bot_name"]
                else strategy
            ),
            "equity": float(eq["equity"]) if eq else None,
            "drawdown_pct": float(eq["drawdown_pct"]) if eq else None,
            "peak_equity": float(eq["peak_equity"]) if eq else None,
            "last_equity_ts": eq["ts"] if eq else None,
            "open_positions": int(opn["c"]) if opn else 0,
            "trades_today": int(todays["c"]) if todays else 0,
            "pnl_today": float(todays["pnl"]) if todays else 0.0,
        }

    # ══════════════════════════════════════════════════════════════════
    # Atomare Write-Sequenzen
    # ══════════════════════════════════════════════════════════════════

    async def open_trade_atomic(
        self,
        *,
        strategy: str,
        bot_name: Optional[str] = None,
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
        current_price: Optional[float] = None,
        entry_signal: Optional[str] = None,
        broker_order_id: Optional[str] = None,
        order_reference: Optional[str] = None,
        reserve_group_name: Optional[str] = None,
        reserve_day: Optional[date] = None,
    ) -> int:
        """Atomare Eröffnung: trades INSERT + positions UPSERT + reserved_groups INSERT
        in einer einzigen DB-Transaktion. Gibt trade_id zurück.
        Entweder alles oder nichts – kein Partial-State möglich."""
        ts = _iso(entry_ts)
        last = _iso(datetime.now(timezone.utc))
        c_price = current_price if current_price is not None else float(entry_price)

        async with self._lock:
            async with self._conn() as conn:
                # 1) Trade anlegen
                cur = await conn.execute(
                    """
                    INSERT INTO trades (
                        strategy, bot_name, symbol, side, entry_ts, entry_price, qty,
                        stop_price, signal_strength, mit_qty_factor,
                        ev_estimate, group_name, features_json, reason
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        strategy, bot_name, symbol, side, ts, float(entry_price),
                        float(qty), stop_price, signal_strength,
                        mit_qty_factor, ev_estimate, group_name,
                        features_json, reason,
                    ),
                )
                trade_id = int(cur.lastrowid or 0)

                # 2) Position UPSERT (mit trade_id FK)
                pos_row = await (
                    await conn.execute(
                        "SELECT id FROM positions "
                        "WHERE strategy=? AND symbol=?",
                        (strategy, symbol),
                    )
                ).fetchone()
                if pos_row is None:
                    await conn.execute(
                        """
                        INSERT INTO positions (
                            trade_id, strategy, symbol, side, entry_ts,
                            entry_price, qty, stop_price, last_update_ts,
                            current_price, unrealized_pnl,
                            unrealized_pnl_pct, held_minutes,
                            entry_signal, entry_reason,
                            broker_order_id, order_reference
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            trade_id, strategy, symbol, side, ts,
                            float(entry_price), float(qty), stop_price,
                            last, c_price, 0.0, 0.0, 0,
                            entry_signal, reason,
                            broker_order_id, order_reference,
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
                            float(qty), stop_price, last, c_price,
                            entry_signal, reason,
                            broker_order_id, order_reference,
                            pos_row["id"],
                        ),
                    )

                # 3) Optionale MIT-Independence Gruppenreservierung
                if reserve_group_name and reserve_day is not None:
                    await conn.execute(
                        """
                        INSERT OR IGNORE INTO reserved_groups
                            (group_name, day, strategy)
                        VALUES (?,?,?)
                        """,
                        (reserve_group_name, reserve_day.isoformat(),
                         strategy),
                    )

                await conn.commit()

        log.info("state.trade_opened_atomic", id=trade_id,
                 strategy=strategy, symbol=symbol, side=side)
        return trade_id

    async def close_trade_atomic(
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
        """Atomarer Exit: trades UPDATE + positions DELETE in einer Transaktion."""
        ts = _iso(exit_ts)
        async with self._lock:
            async with self._conn() as conn:
                # 1) Trade finden
                if trade_id is not None:
                    row = await (
                        await conn.execute(
                            "SELECT id, strategy, symbol FROM trades "
                            "WHERE id=? AND exit_ts IS NULL",
                            (int(trade_id),),
                        )
                    ).fetchone()
                else:
                    row = await (
                        await conn.execute(
                            """
                            SELECT id, strategy, symbol FROM trades
                            WHERE strategy=? AND symbol=? AND exit_ts IS NULL
                            ORDER BY entry_ts DESC LIMIT 1
                            """,
                            (strategy or "", symbol or ""),
                        )
                    ).fetchone()
                if row is None:
                    log.warning("state.close_trade_atomic_no_match",
                                trade_id=trade_id, strategy=strategy,
                                symbol=symbol)
                    return

                tid = int(row["id"])
                strat = row["strategy"]
                sym = row["symbol"]

                # 2) Trade schließen
                await conn.execute(
                    """
                    UPDATE trades
                    SET exit_ts=?, exit_price=?, pnl=?, pnl_pct=?, reason=?
                    WHERE id=?
                    """,
                    (ts, float(exit_price), pnl, pnl_pct, reason, tid),
                )

                # 3) Position entfernen
                await conn.execute(
                    "DELETE FROM positions "
                    "WHERE strategy=? AND symbol=?",
                    (strat, sym),
                )

                await conn.commit()

        log.info("state.trade_closed_atomic", id=tid, pnl=pnl,
                 reason=reason)

    # ══════════════════════════════════════════════════════════════════
    # Monitoring-Integration (Read-Cache Quelle)
    # ══════════════════════════════════════════════════════════════════

    async def get_health_snapshot(self, strategy: str) -> dict[str, Any]:
        """Liefert alle Monitoring-Daten für eine Strategie in einem Roundtrip.
        Wird von HealthState/MetricsCollector als Cache-Quelle genutzt (≤5s TTL).
        """
        today = datetime.now(timezone.utc).date().isoformat()
        one_hour_ago = _iso(
            datetime.now(timezone.utc) - timedelta(hours=1)
        )
        async with self._conn() as conn:
            # Equity + Drawdown
            eq = await (
                await conn.execute(
                    "SELECT equity, cash, drawdown_pct, peak_equity, ts "
                    "FROM equity_snapshots WHERE strategy=? "
                    "ORDER BY ts DESC LIMIT 1",
                    (strategy,),
                )
            ).fetchone()

            # Open positions count
            opn = await (
                await conn.execute(
                    "SELECT COUNT(*) AS c FROM positions WHERE strategy=?",
                    (strategy,),
                )
            ).fetchone()

            # Trades + PnL today
            td = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c, COALESCE(SUM(pnl), 0.0) AS pnl
                    FROM trades WHERE strategy=? AND exit_ts IS NOT NULL
                    AND substr(exit_ts,1,10)=?
                    """,
                    (strategy, today),
                )
            ).fetchone()

            # Signals today (alle) + gefilterte heute
            sigs = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM signals
                    WHERE strategy=? AND substr(ts,1,10)=?
                    """,
                    (strategy, today),
                )
            ).fetchone()
            sigs_filtered = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM signals
                    WHERE strategy=? AND substr(ts,1,10)=?
                    AND filtered_by IS NOT NULL AND filtered_by != ''
                    """,
                    (strategy, today),
                )
            ).fetchone()

            # Anomalies last hour
            anom = await (
                await conn.execute(
                    """
                    SELECT COUNT(*) AS c FROM anomaly_events
                    WHERE strategy=? AND ts >= ?
                    """,
                    (strategy, one_hour_ago),
                )
            ).fetchone()

            heartbeat = None
            try:
                heartbeat = await (
                    await conn.execute(
                        "SELECT * FROM bot_heartbeat WHERE strategy=?",
                        (strategy,),
                    )
                ).fetchone()
            except Exception:
                heartbeat = None

        symbol_status: dict[str, Any] = {}
        if heartbeat and heartbeat["symbol_status_json"]:
            try:
                parsed = json.loads(heartbeat["symbol_status_json"])
                if isinstance(parsed, dict):
                    symbol_status = parsed
            except (TypeError, ValueError):
                symbol_status = {}

        return {
            "strategy": strategy,
            "bot_name": (
                heartbeat["bot_name"] if heartbeat and heartbeat["bot_name"]
                else strategy
            ),
            "equity": float(eq["equity"]) if eq else None,
            "cash": float(eq["cash"]) if eq else None,
            "drawdown_pct": float(eq["drawdown_pct"]) if eq else None,
            "peak_equity": float(eq["peak_equity"]) if eq else None,
            "last_equity_ts": eq["ts"] if eq else None,
            "last_bar_ts": heartbeat["last_bar_ts"] if heartbeat else None,
            "last_bar_lag_ms": (
                float(heartbeat["last_bar_lag_ms"])
                if heartbeat and heartbeat["last_bar_lag_ms"] is not None
                else None
            ),
            "broker_connected": (
                bool(heartbeat["broker_connected"]) if heartbeat else False
            ),
            "broker_adapter": heartbeat["broker_adapter"] if heartbeat else "",
            "circuit_breaker": (
                bool(heartbeat["circuit_breaker"]) if heartbeat else False
            ),
            "symbol_status": symbol_status,
            "bot_last_seen": heartbeat["updated_at"] if heartbeat else None,
            "open_positions": int(opn["c"]) if opn else 0,
            "trades_today": int(td["c"]) if td else 0,
            "pnl_today": float(td["pnl"]) if td else 0.0,
            "signals_today": int(sigs["c"]) if sigs else 0,
            "signals_filtered_today": int(sigs_filtered["c"]) if sigs_filtered else 0,
            "anomalies_last_hour": int(anom["c"]) if anom else 0,
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
