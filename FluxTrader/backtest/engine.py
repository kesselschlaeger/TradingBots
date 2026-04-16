"""BarByBarEngine – deterministische Backtest-Schleife.

Iteriert chronologisch über alle Bars aller Symbole, ruft pro Bar die
gleiche ``ORBStrategy`` / ``OBBStrategy`` wie der Live-Runner. Exits werden
durch den ``TradeManager`` (SL/TP innerhalb des Bars, EOD, exit_next_open)
und durch den ``PaperAdapter`` ausgeführt – exakt eine Ausführungsroutine
für Backtest und Paper.

Optimierungen (gegenüber naiver Implementierung):
  - Indikator-Frames einmal vorab berechnet (O(n) statt O(n²))
  - Timeline aus numpy-Arrays statt iterrows + Bar-Objekt-Sort
  - Sync-Shortcuts für PaperAdapter (kein async/Lock im Hot-Loop)
  - Intrabar-Checks inline ohne unnötige Coroutine-Erstellung
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional

import numpy as np
import pandas as pd

from backtest.slippage import CommissionModel, SlippageModel
from core.context import MarketContextService, set_context_service
from core.filters import to_et_time
from core.indicators import compute_indicator_frame
from core.logging import get_logger
from core.models import Bar, OrderRequest, OrderSide, Signal, Trade
from core.trade_manager import ManagedTrade, TradeManager
from execution.paper_adapter import PaperAdapter
from strategy.base import BaseStrategy

log = get_logger(__name__)


@dataclass
class BacktestConfig:
    initial_capital: float = 10_000.0
    risk_pct: float = 0.01
    max_equity_at_risk: float = 0.05
    max_position_value_pct: float = 0.25
    eod_close_time: Optional[time] = time(15, 55)
    use_trailing: bool = False
    trail_after_r: float = 1.0
    trail_distance_r: float = 0.6
    slippage: SlippageModel = field(default_factory=SlippageModel)
    commission: CommissionModel = field(default_factory=CommissionModel)


@dataclass
class BacktestResult:
    trades: list[Trade]
    equity_curve: pd.Series
    final_equity: float
    initial_capital: float
    bars_processed: int


# ── Timeline-Builder (Variante C) ────────────────────────────────────────

def _build_timeline(
    data: dict[str, pd.DataFrame],
) -> tuple[list, dict[str, tuple[np.ndarray, ...]]]:
    """Erzeuge chronologisch sortierte Timeline + numpy-Arrays pro Symbol.

    Gibt zurück:
        timeline : sorted list of (timestamp, symbol, row_index)
        sym_arrays: {symbol: (opens, highs, lows, closes, volumes)}
    """
    entries: list[tuple] = []
    sym_arrays: dict[str, tuple[np.ndarray, ...]] = {}

    for sym, df in data.items():
        opens = df["Open"].values
        highs = df["High"].values
        lows = df["Low"].values
        closes = df["Close"].values
        vols = (
            df["Volume"].values.astype(np.int64)
            if "Volume" in df.columns
            else np.zeros(len(df), dtype=np.int64)
        )
        sym_arrays[sym] = (opens, highs, lows, closes, vols)

        ts_list = df.index.tolist()
        entries.extend((ts, sym, i) for i, ts in enumerate(ts_list))

    entries.sort(key=lambda x: x[0])
    return entries, sym_arrays


class BarByBarEngine:
    """Backtest-Engine für eine einzelne Strategie.

    Strategien bleiben unverändert: dieselbe Klasse wie im Live-Runner.
    Cross-Symbol-Kontext (SPY/VIX/Account) kommt aus dem injizierten
    MarketContextService.
    """

    def __init__(self,
                 strategy: BaseStrategy,
                 broker: PaperAdapter,
                 context: MarketContextService,
                 config: BacktestConfig):
        self.strategy = strategy
        self.broker = broker
        self.context = context
        self.cfg = config
        self.tm = TradeManager(
            trail_after_r=config.trail_after_r,
            trail_distance_r=config.trail_distance_r,
            use_trailing=config.use_trailing,
            eod_close_time=config.eod_close_time,
        )
        self._equity_curve: list[tuple[datetime, float]] = []
        self._pending_exit_next_open: dict[str, ManagedTrade] = {}
        set_context_service(context)

    async def run(self,
                  data: dict[str, pd.DataFrame],
                  spy_df: Optional[pd.DataFrame] = None,
                  vix_series: Optional[pd.Series] = None,
                  vix3m_series: Optional[pd.Series] = None,
                  ) -> BacktestResult:
        if spy_df is not None:
            self.context.set_spy_df(spy_df)

        # ── Variante A: Indikator-Frames einmal vorab berechnen ──────
        precomputed: dict[str, pd.DataFrame] = {}
        for sym, df in data.items():
            precomputed[sym] = compute_indicator_frame(df)
        self.context.set_indicator_frames(precomputed)

        # ── Variante C: Effiziente Timeline aus numpy-Arrays ─────────
        timeline, sym_arrays = _build_timeline(precomputed)

        if not timeline:
            return BacktestResult(
                trades=[], equity_curve=pd.Series(dtype=float),
                final_equity=self.cfg.initial_capital,
                initial_capital=self.cfg.initial_capital,
                bars_processed=0,
            )

        n_bars = len(timeline)
        log.info("backtest.start", total_bars=n_bars,
                 symbols=list(precomputed.keys()))

        last_day: Optional[object] = None

        for ts, sym, ridx in timeline:
            py_ts = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
            o, h, l, c, v = sym_arrays[sym]
            bar = Bar(
                symbol=sym, timestamp=py_ts,
                open=float(o[ridx]), high=float(h[ridx]),
                low=float(l[ridx]), close=float(c[ridx]),
                volume=int(v[ridx]),
            )

            # Cursor setzen (Strategie liest precomputed frame bis hierhin)
            self.context.set_bar_cursor(sym, ridx)
            self.context.set_now(py_ts)
            self._update_vix(py_ts, vix_series, vix3m_series)

            # Day-Change → reserved groups zurücksetzen
            day = py_ts.date() if hasattr(py_ts, "date") else py_ts
            if last_day is not None and day != last_day:
                self.context.clear_reserved_groups()
            last_day = day

            # 1) Pending OBB-Exit am nächsten Open (inline, kein await)
            pending = self._pending_exit_next_open.pop(sym, None)
            if pending is not None:
                self._flatten_sync(sym, bar.open, "OBB_NEXT_OPEN")

            # 2) Mark-to-Market
            self.broker.set_market_price(sym, bar.close)

            # 3) SL/TP innerhalb des Bars (sync fast-path)
            exit_result = self.tm.check_bar_exit(sym, bar.high, bar.low,
                                                  bar.close)
            if exit_result is not None:
                reason, exit_price = exit_result
                self._flatten_sync(sym, exit_price, reason)

            # 4) EOD-Close
            if self.tm.should_eod_close(py_ts) and self.tm.get(sym) is not None:
                self._flatten_sync(sym, bar.close, "EOD")

            # 5) Trailing-Stop-Update
            if self.cfg.use_trailing:
                self.tm.on_price(sym, bar.close)

            # 6) Strategie aufrufen
            signals = self.strategy.on_bar(bar)
            for sig in signals:
                self._execute_signal_sync(sig, bar)

            # 7) Equity-Tracking (sync, kein await)
            account = self.broker.get_account_sync()
            equity = float(account["equity"])
            self.context.update_account(
                equity=equity, cash=account["cash"],
                buying_power=account["buying_power"],
            )
            self._equity_curve.append((py_ts, equity))

        # Final close (selten, async OK)
        await self.broker.close_all_positions()
        final_account = await self.broker.get_account()

        eq_series = pd.Series(
            [e for _, e in self._equity_curve],
            index=pd.DatetimeIndex([t for t, _ in self._equity_curve], tz="UTC"),
            name="equity",
        )
        return BacktestResult(
            trades=list(self.broker.trade_log),
            equity_curve=eq_series,
            final_equity=float(final_account["equity"]),
            initial_capital=self.cfg.initial_capital,
            bars_processed=n_bars,
        )

    # ── Sync-Hilfsmethoden (Variante D: kein async im Hot-Loop) ──────

    def _execute_signal_sync(self, sig: Signal, bar: Bar) -> None:
        if sig.direction == 0:
            return

        existing = self.broker.get_position_sync(sig.symbol)
        if existing is not None:
            return

        account = self.broker.get_account_sync()
        equity = float(account["equity"])

        # Sizing-Logik (identisch zu BrokerPort.execute_signal)
        entry = float(sig.metadata.get("entry_price", sig.stop_price))
        qty_hint = sig.metadata.get("qty_hint")
        if qty_hint is not None:
            qty = int(qty_hint)
        elif sig.stop_price and sig.stop_price > 0:
            from core.risk import position_size
            qty = position_size(
                equity=equity,
                risk_pct=self.cfg.risk_pct * max(sig.strength, 0.0),
                entry=entry,
                stop=sig.stop_price,
                max_equity_at_risk=self.cfg.max_equity_at_risk,
                max_position_value_pct=self.cfg.max_position_value_pct,
            )
        else:
            from core.risk import fixed_fraction_size
            qty = fixed_fraction_size(equity, entry,
                                      self.cfg.max_position_value_pct)

        qty_factor = float(sig.metadata.get("qty_factor", 1.0))
        qty = max(0, int(qty * max(qty_factor, 0.0)))
        if qty < 1:
            return

        side = OrderSide.BUY if sig.direction > 0 else OrderSide.SELL
        req = OrderRequest(
            symbol=sig.symbol, side=side, qty=qty,
            order_type=sig.metadata.get("order_type", "market"),
            limit_price=sig.metadata.get("limit_price"),
            stop_loss=sig.stop_price if sig.stop_price > 0 else None,
            take_profit=sig.target_price,
            time_in_force=sig.metadata.get("time_in_force", "day"),
            client_order_id=sig.metadata.get("client_order_id"),
        )
        order_id = self.broker.submit_order_sync(req)
        if not order_id:
            return

        # Open-Symbols + Group-Reservation
        self.context.set_open_symbols(
            list(self.broker.get_positions_sync().keys())
        )
        group = sig.metadata.get("reserve_group")
        if group:
            self.context.reserve_group(group)

        # Im TradeManager registrieren
        trade_side = "long" if sig.direction > 0 else "short"
        managed = ManagedTrade(
            symbol=sig.symbol, side=trade_side,
            entry=float(sig.metadata.get("entry_price", bar.close)),
            stop=float(sig.stop_price or 0.0),
            target=float(sig.target_price) if sig.target_price else None,
            qty=float(self._last_position_qty_sync(sig.symbol)),
            strategy_id=sig.strategy_id,
            opened_at=bar.timestamp,
            metadata=dict(sig.metadata),
        )
        self.tm.register(managed)
        if sig.metadata.get("exit_next_open"):
            self._pending_exit_next_open[sig.symbol] = managed

    def _flatten_sync(self, symbol: str, price: float, reason: str) -> None:
        pos = self.broker.get_position_sync(symbol)
        if pos is None:
            self.tm.forget(symbol)
            return
        side = OrderSide.SELL if pos.side == "long" else OrderSide.BUY
        exec_price = self.cfg.slippage.apply(price, side)
        self.broker.set_market_price(symbol, exec_price)
        self.broker.submit_order_sync(OrderRequest(
            symbol=symbol, side=side, qty=int(pos.qty),
            order_type="market",
            client_order_id=f"close|{reason}|{symbol}",
        ))
        self.tm.forget(symbol)
        self._pending_exit_next_open.pop(symbol, None)
        self.context.set_open_symbols(
            list(self.broker.get_positions_sync().keys())
        )

    def _last_position_qty_sync(self, symbol: str) -> float:
        pos = self.broker.get_position_sync(symbol)
        if pos is None:
            internal = getattr(self.broker, "_positions", {}).get(symbol)
            return float(internal.qty) if internal else 0.0
        return float(pos.qty)

    # ── Sonstige Hilfsmethoden ───────────────────────────────────────

    def _update_vix(self, ts: datetime,
                    vix: Optional[pd.Series],
                    vix3m: Optional[pd.Series]) -> None:
        spot = vix3m_val = None
        if vix is not None and len(vix):
            try:
                spot = float(vix.asof(ts))
            except (KeyError, ValueError):
                spot = None
        if vix3m is not None and len(vix3m):
            try:
                vix3m_val = float(vix3m.asof(ts))
            except (KeyError, ValueError):
                vix3m_val = None
        if spot is not None or vix3m_val is not None:
            self.context.set_vix(spot, vix3m_val)
