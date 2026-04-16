# Alpaca Markets

FluxTrader nutzt die offizielle `alpaca-py` Bibliothek für Trading und Marktdaten.

## Voraussetzungen

```bash
pip install -e ".[alpaca]"
```

Alpaca-Account erstellen unter [alpaca.markets](https://alpaca.markets).
Paper-Trading ist kostenlos, Live-Trading erfordert Margin-Konto (US-Kunden).

## API-Keys

Zwei getrennte Key-Paare: **Paper-Keys** und **Live-Keys**.

```dotenv
# .env
APCA_API_KEY_ID=PKxxxxxxxxxxxxxxx       # Paper-Key beginnt mit PK
APCA_API_SECRET_KEY=xxxxxxxxxxxxx...
```

!!! danger "Paper ≠ Live"
    Paper-Keys funktionieren **nur** auf dem Paper-Endpunkt. Live-Keys
    funktionieren nur auf dem Live-Endpunkt. `paper: true/false` in der Config
    muss zum Key passen – sonst schlägt die Authentifizierung fehl.

## Config

```yaml
broker:
  type: alpaca
  paper: true                   # true = Paper-Endpunkt, false = Live
  alpaca_data_feed: iex         # iex (kostenlos) oder sip (kostenpflichtig)

data:
  provider: alpaca
  timeframe: "5Min"
```

## Daten-Feed

| Feed | Kosten | Latenz | Eignung |
|---|---|---|---|
| `iex` | Kostenlos | ~15 Min Delay möglich | Paper-Trading, Tests |
| `sip` | Kostenpflichtig (ab Basic Plan) | Real-Time | Live-Trading |

!!! tip "Live-Trading"
    Für Live-Trading unbedingt `alpaca_data_feed: sip` setzen, sonst könnten
    Preissignale verzögert sein.

## Order-Typen

`AlpacaAdapter` nutzt **Bracket-Orders** für ORB:

```python
# Intern erzeugte Order (vereinfacht):
MarketOrderRequest(
    symbol="NVDA",
    qty=50,
    side=OrderSide.BUY,
    time_in_force=TimeInForce.DAY,
    order_class=OrderClass.BRACKET,
    stop_loss=StopLossRequest(stop_price=485.00),
    take_profit=TakeProfitRequest(limit_price=510.00),
)
```

Alpaca verwaltet SL und TP serversseitig – der Bot muss keine weiteren
Exit-Orders senden.

## Rate-Limiting

Der Adapter enthält exponentiellen Backoff bei HTTP 429:

```
Versuch 1: sofort
Versuch 2: 1s Pause
Versuch 3: 2s Pause (× 2 bei 429-Fehler)
```

Max. 3 Versuche, dann Exception.

## Shortable-Check

```python
# Vor Short-Entry:
shortable = await broker.is_shortable("TSLA")
# True wenn asset.shortable AND asset.easy_to_borrow
```

## Bekannte Einschränkungen

- Fractional Shares: `qty` muss eine ganze Zahl sein (FluxTrader nutzt `int`)
- Pattern Day Trader Rule: Weniger als 25.000 $ Eigenkapital → max. 3 Day-Trades/5 Tage auf US-Margin-Konten
- After-Hours-Orders: FluxTrader schließt vor 16:00 ET; After-Hours-Support wäre eine Erweiterung
