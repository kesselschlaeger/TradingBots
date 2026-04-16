# Paper-Adapter (in-memory)

Der `PaperAdapter` simuliert einen Broker vollständig im Arbeitsspeicher –
ohne Netzwerk, ohne API-Keys, deterministisch. Er ist der Standard-Broker für
Tests und Backtests.

## Eigenschaften

- Market-Orders werden sofort gefüllt (kein Order-Book-Matching)
- Konfigurierbarer Slippage (prozentual) und Commission
- Serielle Ausführung über `asyncio.Lock` (keine Race Conditions im Backtest)
- `set_market_price()` erlaubt der Backtest-Engine, den aktuellen Preis zu setzen
- Trade-Log (`broker.trade_log`) sammelt alle Fills für die Tearsheet-Auswertung

## Konfiguration

```yaml
broker:
  type: paper
  paper: true
```

Keine weiteren Parameter nötig. `initial_cash` wird aus `initial_capital` übernommen.

## Programmatische Nutzung

```python
from execution.paper_adapter import PaperAdapter
from core.models import OrderRequest, OrderSide

paper = PaperAdapter(
    initial_cash=100_000.0,
    slippage_pct=0.0002,    # 2 Basispunkte
    commission_pct=0.00005, # 0.5 Basispunkte
)

# Preis setzen (Backtest-Engine macht das automatisch)
paper.set_market_price("AAPL", 175.50)

# Order platzieren
import asyncio
order_id = asyncio.run(paper.submit_order(
    OrderRequest(symbol="AAPL", side=OrderSide.BUY, qty=100)
))

# Position prüfen
pos = asyncio.run(paper.get_position("AAPL"))
print(pos.qty, pos.entry_price)

# Account
acct = asyncio.run(paper.get_account())
print(acct["equity"])
```

## Slippage-Modell

```
Long  (BUY):  filled_price = market_price × (1 + slippage_pct)
Short (SELL): filled_price = market_price × (1 - slippage_pct)
Commission:   filled_price × qty × commission_pct (von Cash abgezogen)
```

## Equity-Berechnung

```
equity = cash
       + Σ (pos.qty × current_price)      für Long-Positionen
       + Σ (entry_price - current_price) × qty  für Short-Positionen
```

## Reset zwischen Tests

```python
paper.reset()  # Cash auf initial_cash, alle Positionen/Orders gelöscht
```
