"""Schnell-Diagnose der state.db – zeigt Tabelleninhalte und Bot-Aktivität."""
import asyncio
import pathlib
import sys

try:
    import aiosqlite
except ImportError:
    sys.exit("aiosqlite fehlt")

DB = pathlib.Path(__file__).parent.parent / "fluxtrader_data" / "state.db"


async def main() -> None:
    if not DB.exists():
        print(f"DB nicht gefunden: {DB}")
        return

    async with aiosqlite.connect(DB) as conn:
        conn.row_factory = aiosqlite.Row

        # 1) Zeilenzahlen
        tables = [
            "equity_snapshots", "trades", "positions", "signals",
            "anomaly_events", "cooldowns", "reserved_groups", "daily", "account",
        ]
        print("=== Tabelleninhalt ===")
        for t in tables:
            try:
                r = await (await conn.execute(f"SELECT COUNT(*) AS c FROM {t}")).fetchone()
                print(f"  {t:<22}: {r['c']:>6} Zeilen")
            except Exception as e:
                print(f"  {t:<22}: FEHLER ({e})")

        # 2) Letzte Equity-Snapshots
        print("\n=== Letzte equity_snapshots (5) ===")
        rows = await (await conn.execute(
            "SELECT ts, strategy, equity, drawdown_pct "
            "FROM equity_snapshots ORDER BY ts DESC LIMIT 5"
        )).fetchall()
        for r in rows:
            print(f"  {r['ts']}  {r['strategy']:<12}  equity={r['equity']:.2f}  dd={r['drawdown_pct']:.2f}%")

        # 3) Account-Peak je Strategie
        print("\n=== Account-Peak (account-Tabelle) ===")
        rows = await (await conn.execute(
            "SELECT key, strategy, value FROM account ORDER BY strategy, key"
        )).fetchall()
        if rows:
            for r in rows:
                print(f"  {r['key']:<20}  strategy={r['strategy']!r:<12}  value={r['value']}")
        else:
            print("  (leer)")

        # 4) Signale der letzten 24h
        print("\n=== Signale letzte 24h (gruppiert) ===")
        rows = await (await conn.execute(
            """SELECT strategy, action, COALESCE(filtered_by,'') AS filtered_by,
               COUNT(*) AS c
               FROM signals WHERE ts >= datetime('now','-1 day')
               GROUP BY strategy, action, filtered_by ORDER BY c DESC"""
        )).fetchall()
        if rows:
            for r in rows:
                fb = f"  filtered={r['filtered_by']}" if r['filtered_by'] else ""
                print(f"  {r['strategy']:<12}  {r['action']:<10}{fb}  n={r['c']}")
        else:
            print("  (keine Signale in den letzten 24h)")

        # 5) Trades gesamt + laufende
        print("\n=== Trades (gesamt / offen) ===")
        r = await (await conn.execute(
            "SELECT COUNT(*) AS all_, "
            "SUM(CASE WHEN exit_ts IS NULL THEN 1 ELSE 0 END) AS open_ "
            "FROM trades"
        )).fetchone()
        print(f"  Gesamt={r['all_']}  Offen={r['open_']}")

        # 6) Letzte 10 Trades (alle Zeiten)
        rows = await (await conn.execute(
            "SELECT strategy, symbol, side, entry_ts, exit_ts, pnl "
            "FROM trades ORDER BY entry_ts DESC LIMIT 10"
        )).fetchall()
        if rows:
            print()
            for r in rows:
                status = "OPEN" if r['exit_ts'] is None else f"pnl={r['pnl']}"
                print(f"  {r['strategy']:<12}  {r['symbol']:<6}  {r['side']}  {r['entry_ts']}  {status}")
        else:
            print("  (noch keine Trades)")

        # 7) Aktive Positionen
        print("\n=== Offene Positionen ===")
        rows = await (await conn.execute(
            "SELECT strategy, symbol, side, entry_price, qty, current_price, unrealized_pnl "
            "FROM positions"
        )).fetchall()
        if rows:
            for r in rows:
                print(f"  {r['strategy']:<12}  {r['symbol']:<6}  {r['side']}  "
                      f"entry={r['entry_price']}  qty={r['qty']}  "
                      f"cur={r['current_price']}  upnl={r['unrealized_pnl']}")
        else:
            print("  (keine)")

        # 8) Cooldowns
        print("\n=== Cooldowns ===")
        rows = await (await conn.execute(
            "SELECT symbol, strategy, until_ts FROM cooldowns ORDER BY until_ts DESC"
        )).fetchall()
        if rows:
            for r in rows:
                print(f"  {r['symbol']:<6}  strategy={r['strategy']!r}  until={r['until_ts']}")
        else:
            print("  (keine)")

        # 9) Anomalien der letzten 24h
        print("\n=== Anomalien letzte 24h ===")
        rows = await (await conn.execute(
            """SELECT strategy, check_name, severity, COUNT(*) AS c
               FROM anomaly_events WHERE ts >= datetime('now','-1 day')
               GROUP BY strategy, check_name, severity ORDER BY c DESC"""
        )).fetchall()
        if rows:
            for r in rows:
                print(f"  {r['strategy']:<12}  {r['check_name']:<30}  {r['severity']}  n={r['c']}")
        else:
            print("  (keine)")


asyncio.run(main())
