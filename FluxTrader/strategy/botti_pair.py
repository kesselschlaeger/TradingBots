"""SPY/QQQ Pair-Trading via Kalman-gefiltertem Z-Score.

State: KalmanSpreadEstimator (eine Instanz pro Strategie-Leben).
Broker-agnostisch: kein Alpaca-Import, kein HTTP, kein I/O.
"""
from __future__ import annotations

from collections import deque

from core.context import MarketContext
from core.indicators import KalmanSpreadEstimator
from core.logging import get_logger
from core.models import Bar, FeatureVector, PairSignal
from strategy.base import PairStrategy
from strategy.registry import register

log = get_logger(__name__)


@register("botti_pair")
class BottiPairStrategy(PairStrategy):
    """SPY/QQQ Pair-Trading via Kalman-gefiltertem Z-Score.

    Entry: |z| > z_entry  →  ENTER (long underperformer, short outperformer)
    Exit:  |z| < z_exit   →  EXIT
    Sonst: HOLD
    """

    def __init__(self, config: dict, context=None):
        super().__init__(config, context=context)
        self._kalman = KalmanSpreadEstimator(
            q=float(config.get("kalman_q", 1e-5)),
            r=float(config.get("kalman_r", 0.01)),
        )
        lookback = int(config.get("pair_lookback", 20))
        self._spread_window: deque[float] = deque(maxlen=lookback)

    @property
    def name(self) -> str:
        return "botti_pair"

    @property
    def symbol_a(self) -> str:
        return str(self.config.get("symbol_a", "SPY"))

    @property
    def symbol_b(self) -> str:
        return str(self.config.get("symbol_b", "QQQ"))

    def reset(self) -> None:
        self._kalman.reset()
        self._spread_window.clear()

    def _generate_pair_signal(
        self,
        bar_a: Bar,
        bar_b: Bar,
        snapshot: MarketContext,
    ) -> PairSignal:
        spread = bar_b.close - bar_a.close
        self._spread_window.append(spread)
        key = self.pair_key

        # Rolling Std aus internem Window
        if len(self._spread_window) < 3:
            self._record_status(
                key, "WAIT_WARMUP",
                f"Spread-Window {len(self._spread_window)}/3",
            )
            return self._hold_signal(bar_a, 0.0)

        import numpy as np
        rolling_std = float(np.std(list(self._spread_window)))
        z = self._kalman.z_score(spread, rolling_std)

        features = FeatureVector(z_score=z)

        z_entry = float(self.config.get("z_entry", 2.0))
        z_exit = float(self.config.get("z_exit", 0.5))
        qty_pct = float(self.config.get("pair_position_pct", 0.05))

        if z > z_entry:
            self._record_status(
                key, "SIGNAL",
                f"ENTER long {self.symbol_a} / short {self.symbol_b} | z={z:.2f}",
            )
            # QQQ outperforms → short QQQ, long SPY
            return PairSignal(
                strategy=self.name,
                symbol=self.symbol_a,
                features=features,
                timestamp=bar_a.timestamp,
                long_symbol=self.symbol_a,
                short_symbol=self.symbol_b,
                z_score=z,
                action="ENTER",
                qty_pct=qty_pct,
            )
        if z < -z_entry:
            self._record_status(
                key, "SIGNAL",
                f"ENTER long {self.symbol_b} / short {self.symbol_a} | z={z:.2f}",
            )
            # SPY outperforms → short SPY, long QQQ
            return PairSignal(
                strategy=self.name,
                symbol=self.symbol_b,
                features=features,
                timestamp=bar_a.timestamp,
                long_symbol=self.symbol_b,
                short_symbol=self.symbol_a,
                z_score=z,
                action="ENTER",
                qty_pct=qty_pct,
            )
        if abs(z) < z_exit:
            self._record_status(key, "SIGNAL", f"EXIT | z={z:.2f}")
            return PairSignal(
                strategy=self.name,
                symbol=self.symbol_a,
                features=features,
                timestamp=bar_a.timestamp,
                long_symbol=self.symbol_a,
                short_symbol=self.symbol_b,
                z_score=z,
                action="EXIT",
                qty_pct=qty_pct,
            )

        self._record_status(
            key, "WAIT_Z",
            f"z={z:.2f} in [{-z_entry:.2f}..{z_entry:.2f}]",
        )
        return self._hold_signal(bar_a, z)

    def _hold_signal(self, bar: Bar, z: float) -> PairSignal:
        return PairSignal(
            strategy=self.name,
            symbol=self.symbol_a,
            features=FeatureVector(z_score=z),
            timestamp=bar.timestamp,
            long_symbol=self.symbol_a,
            short_symbol=self.symbol_b,
            z_score=z,
            action="HOLD",
            qty_pct=0.0,
        )
