# Strategien

FluxTrader bringt fünf produktionserprobte Strategien mit.

| Strategie | Timeframe | Style | Haltedauer | Broker-Anforderung |
|---|---|---|---|---|
| [ORB](orb.md) | 5-Min Intraday | Trend-Following Breakout | Intraday (Stunden) | SL + TP (Bracket-Order) |
| [Quick Flip](quick_flip.md) | 5-Min Intraday | Reversal nach Liquidity-Sweep | Intraday (Minuten–Stunden) | SL + TP (Market-Order) |
| [ICT Order Block](ict_ob.md) | 5-Min / 4H / 1H MTF | SMC Order-Block Breakout | Intraday (Stunden) | SL + TP (R-basiert) |
| [OBB](obb.md) | Daily | Momentum Swing | 1 Handelstag (Overnight) | Market-Entry, kein SL |
| [Botti](botti.md) | Daily (+ MTF-Filter) | Trend Following + Mean Reversion | Tage–Wochen | SL + TP (ATR-basiert) |
| [Botti Pair](botti_pair.md) | Daily | Pair-Trading (Kalman Z-Score) | Tage–Wochen | Market-Order (Long + Short Leg) |

## Eigene Strategie entwickeln

Jede Strategie erbt von `BaseStrategy` und registriert sich über `@register`:

```python
from strategy.base import BaseStrategy
from strategy.registry import register
from core.models import Bar, Signal

@register("meine_strategie")
class MeineStrategie(BaseStrategy):

    @property
    def name(self) -> str:
        return "meine_strategie"

    def _generate_signals(self, bar: Bar) -> list[Signal]:
        # Reiner Python-Code, kein I/O, kein Broker
        ...
        return [Signal(...)]
```

Der Runner erkennt die Strategie automatisch über ihren Namen in der YAML-Config.

!!! info "Invarianten"
    - **Kein Broker-Import** in Strategie-Dateien
    - **Kein Netzwerk** – alle externen Daten kommen vom `MarketContextService`
    - `_generate_signals()` ist eine reine Funktion (gleiche Inputs → gleiche Outputs)
