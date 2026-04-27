"""Zeigt offene Trades ohne exit_ts (Inkonsistenz-Check)."""
import asyncio, pathlib
import aiosqlite

DB = pathlib.Path(__file__).parent.parent / "fluxtrader_data" / "fluxtrader.db"

async def main():
    async with aiosqlite.connect(DB) as conn:
        conn.row_factory = aiosqlite.Row
        rows = await (await conn.execute(
            "SELECT id, strategy, symbol, side, entry_ts, entry_price, qty "
            "FROM trades WHERE exit_ts IS NULL"
        )).fetchall()
        print("Offene Trades ohne exit_ts:")
        for r in rows:
            print(f"  id={r['id']}  {r['strategy']}  {r['symbol']}  {r['side']}"
                  f"  entry={r['entry_ts']}  price={r['entry_price']}  qty={r['qty']}")
        if not rows:
            print("  (keine)")

asyncio.run(main())
