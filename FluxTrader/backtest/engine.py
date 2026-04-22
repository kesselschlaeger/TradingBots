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
import uuid
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Optional

import numpy as np
import pandas as pd

from backtest.slippage import CommissionModel, SlippageModel
from core.context import MarketContextService, set_context_service
from core.filters import to_et_time
from core.indicators import compute_indicator_frame, ensure_daily
from core.logging import get_logger
from core.models import Bar, EnrichedTrade, OrderRequest, OrderSide, Signal, Trade
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
    # Steuert paper.fill- und trade.register-Logs: im Backtest i.d.R.
    # stoerend (tausende Einzelzeilen), daher default off.
    log_order_events: bool = False


@dataclass
class BacktestResult:
    trades: list[Trade]
    equity_curve: pd.Series
    final_equity: float
    initial_capital: float
    bars_processed: int
    enriched_trades: list[EnrichedTrade] = field(default_factory=list)
    start_ts: Optional[datetime] = None
    end_ts: Optional[datetime] = None
    strategy_name: str = ""
    allow_shorts: bool = True
    mit_enabled: bool = False


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


# ── Exit-Reason-Mapping ──────────────────────────────────────────────────

_EXIT_REASON_MAP = {
    "STOP": "stop_loss",
    "TARGET": "take_profit",
    "EOD": "eod",
    "OBB_NEXT_OPEN": "next_open",
    "MANUAL": "manual",
}


def _resolve_exit_reason(raw_reason: str, managed: Optional[ManagedTrade]) -> str:
    """Mapped Engine-Reason auf menschenlesbaren Exit-Grund."""
    if raw_reason == "STOP" and managed and managed.trailed:
        return "trailing_stop"
    return _EXIT_REASON_MAP.get(raw_reason, raw_reason.lower())


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
        # Fill- und Register-Logs werden gemeinsam über log_order_events
        # gesteuert. PaperAdapter hat das Attribut; Live-Broker ignorieren
        # es harmlos (setattr ist no-op-safe).
        try:
            broker.log_fills = bool(config.log_order_events)
        except AttributeError:
            pass
        self.tm = TradeManager(
            trail_after_r=config.trail_after_r,
            trail_distance_r=config.trail_distance_r,
            use_trailing=config.use_trailing,
            eod_close_time=config.eod_close_time,
            log_registers=bool(config.log_order_events),
        )
        self._equity_curve: list[tuple[datetime, float]] = []
        self._pending_exit_next_open: dict[str, ManagedTrade] = {}
        set_context_service(context)

        # ── EnrichedTrade-Tracking ──────────────────────────────────
        # Key: symbol (nur eine Position pro Symbol gleichzeitig)
        self._trade_tracking: dict[str, dict] = {}
        self._enriched_trades: list[EnrichedTrade] = []
        # SPY-Close-Index für Benchmark-Berechnung (wird in run() befüllt)
        self._spy_closes: Optional[pd.Series] = None

    async def run(self,
                  data: dict[str, pd.DataFrame],
                  spy_df: Optional[pd.DataFrame] = None,
                  vix_series: Optional[pd.Series] = None,
                  vix3m_series: Optional[pd.Series] = None,
                  ) -> BacktestResult:
        if spy_df is not None:
            # Trend-Filter arbeitet auf Daily-Basis (EMA-Periode in Tagen).
            # Intraday-SPY wird einmalig zu Daily aggregiert; spy_df_asof()
            # stellt pro Bar den Look-Ahead-freien Slice bereit.
            self.context.set_spy_df(ensure_daily(spy_df))

        # SPY-Closes für Benchmark-Return (Original-Timeframe)
        if spy_df is not None and "Close" in spy_df.columns:
            self._spy_closes = spy_df["Close"]
        elif "SPY" in data and "Close" in data["SPY"].columns:
            self._spy_closes = data["SPY"]["Close"]
        else:
            self._spy_closes = None
            if spy_df is None:
                log.warning("backtest.no_spy_data",
                            hint="benchmark_return_pct wird 0.0")

        # ── Variante A: Indikator-Frames einmal vorab berechnen ──────
        precomputed: dict[str, pd.DataFrame] = {}
        for sym, df in data.items():
            precomputed[sym] = compute_indicator_frame(df)
        self.context.set_indicator_frames(precomputed)

        # ── Variante C: Effiziente Timeline aus numpy-Arrays ─────────
        timeline, sym_arrays = _build_timeline(precomputed)

        strat_cfg = getattr(self.strategy, "config", {}) or {}
        meta = dict(
            strategy_name=getattr(self.strategy, "name", type(self.strategy).__name__),
            allow_shorts=bool(strat_cfg.get("allow_shorts", True)),
            mit_enabled=bool(strat_cfg.get("use_mit_probabilistic_overlay", False)),
        )

        if not timeline:
            return BacktestResult(
                trades=[], equity_curve=pd.Series(dtype=float),
                final_equity=self.cfg.initial_capital,
                initial_capital=self.cfg.initial_capital,
                bars_processed=0,
                **meta,
            )

        def _py_ts(ts):
            return ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts

        start_ts = _py_ts(timeline[0][0])
        end_ts = _py_ts(timeline[-1][0])

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
            # Broker nutzt Bar-Zeit für Trade.timestamp / filled_at statt
            # wall-clock -- wichtig für Tearsheet und Equity-Curve.
            self.broker.set_sim_clock(py_ts)
            self._update_vix(py_ts, vix_series, vix3m_series)

            # Day-Change → reserved groups zurücksetzen
            day = py_ts.date() if hasattr(py_ts, "date") else py_ts
            if last_day is not None and day != last_day:
                self.context.clear_reserved_groups()
            last_day = day

            # ── MAE/MFE-Update für offenen Trade dieses Symbols ──────
            self._update_trade_tracking(sym, bar)

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

        # Verbleibende offene Positionen schließen (EnrichedTrade!)
        for sym in list(self._trade_tracking.keys()):
            price = self.broker._market_prices.get(sym, 0.0)
            if price > 0:
                self._flatten_sync(sym, price, "MANUAL")
        # Safety-Net: falls PaperAdapter noch Positionen hat
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
            enriched_trades=list(self._enriched_trades),
            start_ts=start_ts,
            end_ts=end_ts,
            **meta,
        )

    # ── MAE/MFE-Tracking pro offenen Trade ──────────────────────────────

    def _start_trade_tracking(
        self, symbol: str, trade_id: str, entry_price: float,
        entry_ts: datetime, shares: int, stop: float, side: str,
        sig: Signal, equity: float,
    ) -> None:
        """Initialisiert MAE/MFE-Tracking bei Trade-Eröffnung."""
        spy_close = self._spy_close_at(entry_ts)
        self._trade_tracking[symbol] = {
            "trade_id": trade_id,
            "entry_price": entry_price,
            "entry_ts": entry_ts,
            "shares": shares,
            "stop": stop,
            "side": side,
            "min_price": entry_price,
            "max_price": entry_price,
            "spy_entry_close": spy_close,
            "equity_at_entry": equity,
            "signal": sig,
        }

    def _update_trade_tracking(self, symbol: str, bar: Bar) -> None:
        """Update High/Low-Extrema für MAE/MFE-Berechnung."""
        tracking = self._trade_tracking.get(symbol)
        if tracking is None:
            return
        tracking["min_price"] = min(tracking["min_price"], bar.low)
        tracking["max_price"] = max(tracking["max_price"], bar.high)

    def _build_enriched_trade(
        self, symbol: str, exit_price: float, exit_ts: datetime,
        exit_reason: str, pnl_net: float, commission: float,
    ) -> Optional[EnrichedTrade]:
        """Assembliert EnrichedTrade aus Tracking-Daten + ManagedTrade."""
        tracking = self._trade_tracking.get(symbol)
        if tracking is None:
            return None

        entry_price = tracking["entry_price"]
        shares = tracking["shares"]
        stop = tracking["stop"]
        side = tracking["side"]
        entry_ts = tracking["entry_ts"]
        sig: Signal = tracking["signal"]

        initial_risk_r = abs(entry_price - stop) if stop > 0 else 0.0
        initial_risk_usd = shares * initial_risk_r

        # PnL brutto (ohne Kosten)
        if side == "long":
            pnl_gross = (exit_price - entry_price) * shares
        else:
            pnl_gross = (entry_price - exit_price) * shares

        cost_basis = entry_price * shares
        pnl_pct = (pnl_net / cost_basis * 100.0) if cost_basis > 0 else 0.0
        r_multiple = (pnl_net / initial_risk_usd) if initial_risk_usd > 0 else 0.0

        slippage_total = pnl_gross - pnl_net - commission
        if slippage_total < 0:
            slippage_total = 0.0

        # Haltedauer
        hold_days = max((exit_ts.date() - entry_ts.date()).days, 0) \
            if hasattr(exit_ts, "date") and hasattr(entry_ts, "date") else 0
        hold_trading_days = int(np.busday_count(
            entry_ts.date(), exit_ts.date(),
        )) if hold_days > 0 else 0

        # MAE / MFE
        min_p = tracking["min_price"]
        max_p = tracking["max_price"]
        if side == "long":
            mae_pct = ((min_p - entry_price) / entry_price * 100.0
                       if entry_price > 0 else 0.0)
            mfe_pct = ((max_p - entry_price) / entry_price * 100.0
                       if entry_price > 0 else 0.0)
        else:
            mae_pct = ((entry_price - max_p) / entry_price * 100.0
                       if entry_price > 0 else 0.0)
            mfe_pct = ((entry_price - min_p) / entry_price * 100.0
                       if entry_price > 0 else 0.0)
        mae_r = (abs(mae_pct / 100.0 * entry_price) * shares / initial_risk_usd
                 if initial_risk_usd > 0 else 0.0)
        mfe_r = (abs(mfe_pct / 100.0 * entry_price) * shares / initial_risk_usd
                 if initial_risk_usd > 0 else 0.0)
        # MAE ist negativ (adverse), MFE ist positiv (favorable)
        if mae_pct > 0:
            mae_pct = 0.0
            mae_r = 0.0
        if mfe_pct < 0:
            mfe_pct = 0.0
            mfe_r = 0.0

        # Benchmark-Return
        spy_entry = tracking["spy_entry_close"]
        spy_exit = self._spy_close_at(exit_ts)
        if spy_entry and spy_exit and spy_entry > 0:
            benchmark_return_pct = (spy_exit - spy_entry) / spy_entry * 100.0
        else:
            benchmark_return_pct = 0.0
        alpha_pct = pnl_pct - benchmark_return_pct

        # Signal-Metadaten
        entry_reason = sig.metadata.get("reason", "")
        entry_signal_action = sig.metadata.get("action", "")
        if not entry_signal_action:
            entry_signal_action = "BUY" if sig.direction > 0 else "SELL"
        atr_val = float(sig.metadata.get("atr_at_entry",
                        sig.metadata.get("atr", 0.0)))
        vix_val = tracking.get("vix_at_entry", 0.0)
        strength = float(getattr(sig, "strength", 0.0))

        return EnrichedTrade(
            trade_id=tracking["trade_id"],
            strategy=sig.strategy,
            symbol=symbol,
            entry_date=entry_ts,
            entry_price=entry_price,
            shares=shares,
            entry_reason=entry_reason,
            entry_signal=entry_signal_action,
            stop_at_entry=stop,
            initial_risk_r=initial_risk_r,
            initial_risk_usd=initial_risk_usd,
            atr_at_entry=atr_val,
            vix_at_entry=vix_val,
            equity_at_entry=tracking["equity_at_entry"],
            exit_date=exit_ts,
            exit_price=exit_price,
            exit_reason=exit_reason,
            hold_days=hold_days,
            hold_trading_days=hold_trading_days,
            pnl_gross=pnl_gross,
            pnl_net=pnl_net,
            pnl_pct=pnl_pct,
            r_multiple=r_multiple,
            commission=commission,
            slippage=slippage_total,
            strength=strength,
            ml_confidence=0.5,
            mae_pct=mae_pct,
            mfe_pct=mfe_pct,
            mae_r=mae_r,
            mfe_r=mfe_r,
            benchmark_return_pct=benchmark_return_pct,
            alpha_pct=alpha_pct,
        )

    def _spy_close_at(self, ts: datetime) -> Optional[float]:
        """SPY-Close zum Zeitpunkt ts (look-ahead-frei via asof)."""
        if self._spy_closes is None or self._spy_closes.empty:
            return None
        try:
            val = self._spy_closes.asof(ts)
            return float(val) if not (isinstance(val, float) and np.isnan(val)) else None
        except (KeyError, ValueError, TypeError):
            return None

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

        # Tatsächlichen Fill-Preis aus Broker lesen
        order_obj = self.broker._orders.get(order_id)
        filled_price = order_obj.filled_price if order_obj else bar.close

        # Im TradeManager registrieren
        trade_side = "long" if sig.direction > 0 else "short"
        managed = ManagedTrade(
            symbol=sig.symbol, side=trade_side,
            entry=filled_price,
            stop=float(sig.stop_price or 0.0),
            target=float(sig.target_price) if sig.target_price else None,
            qty=float(self._last_position_qty_sync(sig.symbol)),
            strategy_id=sig.strategy,
            opened_at=bar.timestamp,
            metadata=dict(sig.metadata),
        )
        self.tm.register(managed)
        if sig.metadata.get("exit_next_open"):
            self._pending_exit_next_open[sig.symbol] = managed

        # ── EnrichedTrade-Tracking starten ────────────────────────
        trade_id = uuid.uuid4().hex[:16]
        vix_spot = self.context._vix_spot
        self._trade_tracking[sig.symbol] = {
            "trade_id": trade_id,
            "entry_price": filled_price,
            "entry_ts": bar.timestamp,
            "shares": int(managed.qty),
            "stop": float(sig.stop_price or 0.0),
            "side": trade_side,
            "min_price": min(bar.low, filled_price),
            "max_price": max(bar.high, filled_price),
            "spy_entry_close": self._spy_close_at(bar.timestamp),
            "equity_at_entry": equity,
            "signal": sig,
            "vix_at_entry": float(vix_spot) if vix_spot is not None else 0.0,
        }

    def _flatten_sync(self, symbol: str, price: float, reason: str) -> None:
        pos = self.broker.get_position_sync(symbol)
        managed = self.tm.get(symbol)
        tracking = self._trade_tracking.get(symbol)

        if pos is None:
            self.tm.forget(symbol)
            self._trade_tracking.pop(symbol, None)
            return

        side = OrderSide.SELL if pos.side == "long" else OrderSide.BUY
        exec_price = self.cfg.slippage.apply(price, side)
        self.broker.set_market_price(symbol, exec_price)

        trade_log_before = len(self.broker._trade_log)
        self.broker.submit_order_sync(OrderRequest(
            symbol=symbol, side=side, qty=int(pos.qty),
            order_type="market",
            client_order_id=f"close|{reason}|{symbol}",
        ))

        # PnL + Commission aus dem Trade-Log lesen
        pnl_net = 0.0
        commission = 0.0
        if len(self.broker._trade_log) > trade_log_before:
            close_trade = self.broker._trade_log[-1]
            pnl_net = float(close_trade.pnl)
            commission = float(close_trade.fees)

        # EnrichedTrade assemblieren (VOR tm.forget!)
        exit_label = _resolve_exit_reason(reason, managed)
        exit_ts = self.broker._now()
        if tracking is not None:
            enriched = self._build_enriched_trade(
                symbol=symbol,
                exit_price=exec_price,
                exit_ts=exit_ts,
                exit_reason=exit_label,
                pnl_net=pnl_net,
                commission=commission,
            )
            if enriched is not None:
                self._enriched_trades.append(enriched)
            self._trade_tracking.pop(symbol, None)

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
