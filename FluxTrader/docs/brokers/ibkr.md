# Interactive Brokers (IBKR)

FluxTrader verbindet sich mit TWS oder IB Gateway via `ib_insync`.

## Voraussetzungen

```bash
pip install -e ".[ibkr]"
```

- TWS (Trader Workstation) oder IB Gateway muss laufen
- API-Verbindung in TWS aktiviert: `File → Global Configuration → API → Settings`
  - "Enable ActiveX and Socket Clients": ✓
  - "Socket port": 7496 (Live) / 7497 (Paper) für TWS, 4001 (Live) / 4002 (Paper) für Gateway

## Config

```yaml
broker:
  type: ibkr
  paper: true
  ibkr_host: "127.0.0.1"
  ibkr_port: 4002           # Paper Gateway
  ibkr_client_id: 1         # Muss eindeutig sein pro Bot-Instanz
  ibkr_bot_id: FLUX         # Prefix für orderRef (max 8 Zeichen)
```

!!! warning "Client-ID Konflikt"
    Jede Bot-Instanz braucht eine **eindeutige** `client_id`. Doppelte IDs
    lösen IBKR-Error 326 aus und werden sofort mit einer RuntimeError geworfen.

## Portauswahl

| Verbindung | Live | Paper |
|---|---|---|
| TWS | 7496 | 7497 |
| IB Gateway | 4001 | 4002 |

## Order-Struktur

IBKR implementiert Bracket-Orders über Parent/Child-Orders:

```
Parent: MarketOrder (transmit=False wenn SL/TP vorhanden)
  ├── Child 1: StopOrder  (parentId=parent.orderId, transmit=False wenn TP vorhanden)
  └── Child 2: LimitOrder (parentId=parent.orderId, transmit=True)
```

`transmit=True` beim letzten Child-Order löst die gesamte Bracket-Gruppe aus.

## Order-Referenzen

Jede Order enthält eine `orderRef` für die Zuordnung:

```
Format: {bot_id}|{symbol}|{side}|{MMDD-HHMM}
Beispiel: FLUX|NVDA|BUY|0312-1005

SL-Child:  FLUX|NVDA|BUY|0312-1005|SL
TP-Child:  FLUX|NVDA|BUY|0312-1005|TP
```

`cancel_all_orders()` filtert nach `bot_id`-Prefix – andere Bots/manuell
platzierte Orders werden nicht berührt.

## Verbindungs-Handling

Der Adapter verbindet synchron bei Initialisierung (3 Versuche à 5s Pause)
und prüft bei jedem API-Call ob die Verbindung noch steht:

```python
def _ensure_connected(self) -> None:
    if not self.ib.isConnected():
        self._connect_sync()  # Reconnect
```

## Positionen

```python
# Alle offenen Aktien-Positionen
positions = await broker.get_positions()
# {symbol: Position(qty, side, entry_price, current_price, unrealized_pnl)}
```

Long-/Short-Erkennung über positives/negatives `position`-Feld in ib_insync.

## Bekannte Einschränkungen

- `is_shortable()` gibt immer `True` zurück – tatsächliche Short-Verfügbarkeit
  prüft IBKR beim Order-Submit und meldet ggf. einen Fehler
- Trailing-Stop-Modify (`modify_stop`) über IBKR-API ist möglich, aber
  noch nicht implementiert – der `TradeManager` berechnet das neue Stop-Level,
  der Live-Runner muss es manuell via `placeOrder` mit `orderId` des SL-Childs
  updaten (Erweiterungspunkt)
- Timezone: `orderRef` nutzt Eastern Time für den Timestamp-Teil
