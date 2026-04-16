# Backtest

Der Backtest nutzt exakt dieselbe Strategy-Klasse wie der Live-Runner –
kein separater Backtest-Code, kein Drift.

## Architektur

```mermaid
graph LR
    YF[YFinanceDataProvider] -->|dict[sym, DataFrame]| E
    CTX[MarketContextService] --> E
    E[BarByBarEngine] -->|on_bar| STR[ORBStrategy / OBBStrategy]
    STR -->|Signal| E
    E -->|execute_signal| PA[PaperAdapter]
    PA -->|fills| TL[Trade-Log]
    E -->|check_bar_exit| TM[TradeManager]
    TM -->|exit| E
    TL --> RPT[Tearsheet-Report]
```

## Datenquellen

### yfinance (empfohlen für Backtest)

```yaml
data:
  provider: yfinance
  timeframe: "5Min"
  lookback_days: 60
```

!!! warning "yfinance Intraday-Limit"
    yfinance liefert Intraday-Bars nur für die letzten ~60 Tage.
    Für längere Backtests Daily-Bars nutzen (`timeframe: "1Day"`).

### Alpaca Historical Data

```yaml
data:
  provider: alpaca
  timeframe: "5Min"
  lookback_days: 365
```

Alpaca liefert bis zu 2 Jahre historische 1-Min-Bars (kostenpflichtig ab SIP).

## Backtest starten

```bash
python main.py backtest --config configs/orb_backtest.yaml
```

## Slippage & Commission

Configurable im Code (nicht in YAML):

```python
from backtest.engine import BacktestConfig
from backtest.slippage import SlippageModel, CommissionModel

cfg = BacktestConfig(
    initial_capital=100_000,
    slippage=SlippageModel(
        fixed_cents=0.01,    # 1 Cent pro Aktie
        percentage=0.0005,   # 5 Basispunkte
    ),
    commission=CommissionModel(
        per_share=0.005,     # $0.005 pro Aktie (Interactive Brokers-Style)
        minimum=1.0,         # Mindest-Commission $1
    ),
)
```

## Exit-Logik im Backtest

Da Alpaca/IBKR Bracket-Orders serverseitig verwalten, simuliert der `BarByBarEngine`
dies innerhalb jedes Bars:

```
Für jeden Bar:
  1. exit_next_open? → Schließe Position zum Open-Preis dieses Bars
  2. set_market_price(close)
  3. check_bar_exit(high, low, close):
     - Long:  low  <= current_stop → STOP (zum stop_price)
     - Long:  high >= target       → TARGET (zum target_price)
     - Short: high >= current_stop → STOP
     - Short: low  <= target       → TARGET
  4. should_eod_close? → Schließe zum Close-Preis
  5. Strategie aufrufen → ggf. neues Signal
```

!!! info "Pessimistisches Backtest-Assumption"
    Wenn im selben Bar sowohl SL als auch TP gerissen werden (was auf tiefer
    Timeframe-Auflösung unrealistisch ist), gewinnt der SL (worst-case).

## Tearsheet

```python
from backtest.report import build_tearsheet, format_tearsheet

ts = build_tearsheet(result.equity_curve, result.trades, initial_capital=100_000)
print(format_tearsheet(ts))
```

### Metriken

| Metrik | Formel |
|---|---|
| **Total Return** | `(final / initial - 1) × 100%` |
| **CAGR** | `(final/initial)^(365/span_days) - 1` |
| **Max Drawdown** | `max((peak - equity) / peak)` rolling |
| **Sharpe** | `√(ann) × mean(returns) / std(returns)` |
| **Sortino** | `√(ann) × mean(returns) / std(downside_returns)` |
| **Win Rate** | `count(pnl > 0) / count(all_trades)` |
| **Profit Factor** | `sum(wins) / abs(sum(losses))` |
| **Expectancy** | `win_rate × avg_win + (1-win_rate) × avg_loss` |

## Programmatischer Backtest

```python
import asyncio
from datetime import datetime, timedelta, timezone

from backtest.engine import BacktestConfig, BarByBarEngine
from backtest.report import build_tearsheet, format_tearsheet
from core.context import MarketContextService, set_context_service
from data.providers.yfinance_provider import YFinanceDataProvider
from execution.paper_adapter import PaperAdapter
from strategy.orb import ORBStrategy

async def main():
    ctx = MarketContextService(initial_capital=100_000)
    ctx.update_account(equity=100_000, cash=100_000, buying_power=400_000)
    set_context_service(ctx)

    strat = ORBStrategy({"use_mit_probabilistic_overlay": True}, context=ctx)
    paper = PaperAdapter(initial_cash=100_000)
    prov = YFinanceDataProvider()

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)

    data = await prov.get_bars_bulk(["AAPL", "NVDA"], start, end, "5Min")
    spy = await prov.get_bars("SPY", start, end, "5Min")
    ctx.set_spy_df(spy)

    engine = BarByBarEngine(strat, paper, ctx, BacktestConfig())
    result = await engine.run(data=data, spy_df=spy)

    ts = build_tearsheet(result.equity_curve, result.trades, 100_000)
    print(format_tearsheet(ts))

asyncio.run(main())
```

## SPY & VIX für den Backtest

```python
# SPY für Trend-Filter
spy_df = await prov.get_bars("SPY", start, end, "5Min")

# VIX für Regime-Filter (Daily, aus yfinance)
import yfinance as yf
vix = yf.download("^VIX", start=start, end=end, interval="1d")["Close"]
vix3m = yf.download("^VIX3M", start=start, end=end, interval="1d")["Close"]

result = await engine.run(data=data, spy_df=spy_df,
                           vix_series=vix, vix3m_series=vix3m)
```
