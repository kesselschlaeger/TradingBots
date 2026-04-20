# Strategien

FluxTrader bringt drei produktionserprobte Strategien mit.

| Strategie | Timeframe | Style | Broker-Anforderung |
|---|---|---|---|
| [ORB](orb.md) | 5-Min Intraday | Trend-Following Breakout | SL + TP (Bracket-Order) |
| [OBB](obb.md) | Daily | Momentum Swing | Market-Entry, kein SL |
| [Botti](botti.md) | Daily (+ MTF-Filter) | Trend Following + Mean Reversion | SL + TP (ATR-basiert) |

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
