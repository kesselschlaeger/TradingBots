# Architektur

## Designprinzipien

| Prinzip | Ausprägung |
|---|---|
| **Broker-Agnostik** | Strategien importieren keinen Broker-Code |
| **No-Duplicate-Logic** | ATR, Stops, Kelly leben exakt einmal in `core/` |
| **Gleiche Klasse Backtest/Live** | `ORBStrategy` wird unverändert in beiden Modi genutzt |
| **Dependency Injection** | Cross-Symbol-Kontext (SPY, VIX, Account) via `MarketContextService` |
| **Async-First** | Alle Broker-Calls sind `async`; sync SDKs werden via `run_in_executor` gewrapped |
| **Pydantic v2 Config** | Typsichere Konfiguration mit Env-Variable-Merge |

---

## Vollständiger Daten-Flow

```mermaid
sequenceDiagram
    participant DP as DataProvider
    participant CTX as MarketContextService
    participant STR as Strategy (ORB/OBB)
    participant TM as TradeManager
    participant BP as BrokerPort
    participant ST as PersistentState

    DP->>CTX: set_spy_df / set_vix
    Note over CTX: Writer-API (nur Runner)

    loop Jeder Bar
        DP->>STR: on_bar(bar)
        STR->>CTX: push_bar(bar) + snapshot()
        CTX-->>STR: spy_df, vix, account, reserved_groups
        STR-->>STR: _generate_signals()
        STR->>TM: Signal → ManagedTrade.register()
        STR->>BP: execute_signal(Signal)
        BP-->>STR: order_id
        TM->>TM: on_price() → trailing stop?
        TM->>BP: close (SL/TP/EOD)
        BP->>ST: add_trade(pnl)
    end
```

---

## Schichtenmodell

```mermaid
graph TB
    subgraph L1["Layer 1 – Kern (kein I/O)"]
        M[core/models.py]
        I[core/indicators.py]
        R[core/risk.py]
        F[core/filters.py]
    end
    subgraph L2["Layer 2 – Zustand & Config"]
        C[core/context.py<br/>MarketContextService]
        TM[core/trade_manager.py]
        CFG[core/config.py]
        LOG[core/logging.py]
    end
    subgraph L3["Layer 3 – Strategie (broker-agnostisch)"]
        BS[strategy/base.py]
        REG[strategy/registry.py]
        ORB[strategy/orb.py]
        OBB[strategy/obb.py]
    end
    subgraph L4["Layer 4 – Ausführung"]
        PORT[execution/port.py<br/>BrokerPort ABC]
        PA[execution/paper_adapter.py]
        AA[execution/alpaca_adapter.py]
        IA[execution/ibkr_adapter.py]
    end
    subgraph L5["Layer 5 – Daten"]
        BASE[data/providers/base.py]
        ADP[data/providers/alpaca_provider.py]
        YFP[data/providers/yfinance_provider.py]
    end
    subgraph L6["Layer 6 – Runner"]
        BT[backtest/engine.py]
        LR[live/runner.py]
        SC[live/scheduler.py]
        ST[live/state.py]
        NT[live/notifier.py]
    end

    L1 --> L2 --> L3 --> L4
    L5 --> L6
    L2 --> L6
    L3 --> L6
    L4 --> L6
```

---

## MarketContextService (DI-Hub)

Der `MarketContextService` ist der zentrale Shared-State zwischen Runner und Strategien.
Strategien dürfen **nur lesen** – schreiben darf ausschließlich der Runner/Engine.

```mermaid
graph LR
    subgraph Writer["Writer (Runner/Engine)"]
        W1[set_now]
        W2[update_account]
        W3[set_spy_df]
        W4[set_vix]
        W5[set_open_symbols]
        W6[reserve_group]
        W7[push_bar]
    end
    subgraph Service["MarketContextService"]
        S[(State)]
    end
    subgraph Reader["Reader (Strategy)"]
        R1[snapshot]
        R2[account]
        R3[spy_df]
        R4[vix]
        R5[open_symbols]
        R6[reserved_groups]
        R7[bars(symbol)]
    end

    Writer --> Service --> Reader
```

**Singleton-Zugriff:**
```python
from core.context import get_context_service, set_context_service, reset_context_service

# Runner setzt einmal:
set_context_service(MarketContextService(initial_capital=100_000))

# Strategie liest:
ctx = get_context_service()
spy = ctx.spy_df
```

---

## Signal-Vertrag

Strategien kommunizieren ausschließlich über `Signal`-Objekte. Keine direkten Broker-Calls.

```python
@dataclass(frozen=True)
class Signal:
    strategy_id: str       # "orb" | "obb"
    symbol:      str
    direction:   int       # +1 Long, -1 Short, 0 Flat/Exit
    strength:    float     # 0.0–1.0 (beeinflusst Sizing)
    stop_price:  float     # 0.0 bei OBB (kein SL)
    target_price: Optional[float]
    timestamp:   datetime
    metadata:    dict      # entry_price, orb_high/low, qty_hint, exit_next_open, …
```

**Metadata-Keys nach Strategie:**

=== "ORB"
    | Key | Typ | Bedeutung |
    |---|---|---|
    | `entry_price` | float | Aktueller Kurs beim Signal |
    | `orb_high` / `orb_low` / `orb_range` | float | Opening-Range-Levels |
    | `volume_ratio` | float | Rel. Volume vs. Time-of-Day-MA |
    | `qty_factor` | float | MIT-Kelly-Skalierungsfaktor (0.25–1.0) |
    | `reserve_group` | str | MIT-Korrelationsgruppe (z.B. "semi_ai") |
    | `reason` | str | Menschenlesbarer Signal-Text |

=== "OBB"
    | Key | Typ | Bedeutung |
    |---|---|---|
    | `entry_price` | float | Schlusskurs des Signal-Bars |
    | `lookback_high` / `lookback_low` | float | 50-Bar-Extrema |
    | `qty_hint` | int | Berechnete Stückzahl (Fixed-Fraction) |
    | `exit_next_open` | bool | `True` → TradeManager schließt am nächsten Open |
    | `reason` | str | Menschenlesbarer Signal-Text |

---

## Zwei Sizing-Paradigmen

```mermaid
flowchart TD
    SIG[Signal empfangen] --> QH{qty_hint in metadata?}
    QH -- Ja --> OBB_QTY[qty = qty_hint<br/>OBB Fixed-Fraction]
    QH -- Nein --> STOP{stop_price > 0?}
    STOP -- Ja --> R_SIZE[R-basiertes Sizing<br/>equity × risk_pct ÷ R]
    STOP -- Nein --> FF[Fixed-Fraction-Fallback<br/>equity × max_pos_pct ÷ entry]
    OBB_QTY & R_SIZE & FF --> QF[× qty_factor aus MIT-Overlay]
    QF --> ORDER[OrderRequest → BrokerPort]
```

### Kontostandquelle und Equity-Flow

Wichtig: **Nicht der DataProvider**, sondern der **BrokerAdapter** liefert
den Kontostand (`equity`, `cash`, `buying_power`).

```mermaid
flowchart LR
    DP[DataProvider<br/>Bars/Quotes] --> RUN[Runner/Engine]
    BRK[BrokerAdapter<br/>get_account] --> RUN
    RUN --> EXE[execute_signal(..., account_equity)]
    EXE --> SIZE[Qty-Berechnung]
```

Praktisch bedeutet das:

| Modus | Kontostand kommt von | Verwendung |
|---|---|---|
| Live (Alpaca/IBKR) | `broker.get_account()` gegen Broker-API | Vor Signal-Execution für Sizing + regelmäßige Context-Synchronisierung |
| Backtest/Paper | `PaperAdapter.get_account()` (intern aus Cash + Positionen) | Pro Bar für Equity-Curve und Sizing |

### ORB vs. OBB (Sizing im Code)

| Strategie | Signal-Inhalt | Sizing-Pfad in `BrokerPort.execute_signal()` |
|---|---|---|
| ORB | setzt `stop_price` + `target_price` | R-basiert via `position_size(equity, risk_pct, entry, stop, ...)` |
| OBB | setzt `qty_hint`, kein SL/TP (`stop_price=0`, `target_price=None`) | `qty_hint` hat Vorrang, kommt aus OBB Fixed-Fraction |

Zusatzdetails:

- `strength` skaliert das effektive Risiko im ORB-Pfad (`risk_pct * strength`).
- `qty_factor` aus Metadata skaliert die Stückzahl am Ende in beiden Pfaden.
- Fehlt Stop **und** `qty_hint`, greift ein Fixed-Fraction-Fallback.

---

## Exit-Verantwortlichkeiten

| Mechanism | Zuständig | Wann |
|---|---|---|
| Bracket-Order SL/TP | Broker (Alpaca/IBKR serverseitig) | Sofort nach Entry |
| Intrabar-Exit (Backtest) | `TradeManager.check_bar_exit()` | Jeder Bar im Backtest |
| Trailing Stop | `TradeManager.on_price()` | Jeder Tick (wenn aktiviert) |
| EOD Flat | `TradeManager.should_eod_close()` | Täglich um `eod_close_time` ET |
| OBB Exit-Next-Open | `BarByBarEngine._handle_exit_next_open()` | Erster Bar des Folgetages |

---

## Async/Sync-Grenze

Alle Broker-SDKs (alpaca-py, ib_insync) sind synchron. Die Grenze wird über `run_in_executor` gezogen:

```python
# Pattern in AlpacaAdapter / IBKRAdapter
loop = asyncio.get_event_loop()
result = await loop.run_in_executor(None, sync_sdk_call)
```

Strategien und der Runner sind vollständig `async` – synchrone Aufrufe verlassen die Event-Loop nie direkt.
