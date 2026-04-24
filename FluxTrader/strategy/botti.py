"""Botti-Strategie (Trend Following + Mean Reversion auf Daily-Bars).

Migriert aus Trading_Bot/trader_v6.py:
  - Golden Cross / Fast Cross / Early GC / Pullback-Entry -> botti_trend
  - Bollinger-Band Mean Reversion -> botti_mr
  - Death Cross -> Exit-Signal
  - VIX-Regime, Drawdown-Breaker, Sector-Cluster-Guard, Volume-Guard

Broker-agnostisch: kein Alpaca-Import, kein HTTP, kein I/O.

TODO: Pair-Trading (SPY/QQQ Z-Score) -> eigene Multi-Symbol-Strategie
TODO: ML-Training (train_ml_model) -> eigenes tools/train_botti_ml.py
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from core.filters import (
    correlation_group,
    drawdown_breaker,
    sector_cluster_ok,
    vix_size_factor,
)
from core.indicators import (
    adx,
    atr,
    bollinger_bands,
    ema,
    macd,
    rsi,
    sma,
)
from core.logging import get_logger
from core.models import Bar, FeatureVector, Signal
from core.risk import atr_stop, position_size
from strategy.base import BaseStrategy
from strategy.registry import register

log = get_logger(__name__)


# ─────────────────────────── Default-Config ─────────────────────────────────

BOTTI_DEFAULT_PARAMS: dict = {
    # Trend (SMA-Crossover)
    "sma_short": 20,
    "sma_long": 30,
    "use_fast_cross": True,
    "fast_cross_type": "EMA",
    "fast_cross_short": 9,
    "fast_cross_long": 21,
    "use_early_golden_cross": True,
    "early_gc_proximity_pct": 0.02,
    "early_gc_rsi_min": 55,
    "use_pullback_entry_daily": True,
    "pullback_daily_lookback": 15,
    "pullback_daily_ema": 20,
    "pullback_daily_proximity": 0.015,
    "pullback_daily_rsi_min": 50,

    # Bestaetigungsfilter
    "use_rsi_filter": True,
    "rsi_buy_min": 50,
    "rsi_buy_max": 70,
    "use_volume_filter": True,
    "volume_sma_period": 20,
    "use_macd_filter": True,
    "macd_fast": 12,
    "macd_slow": 26,
    "macd_signal_period": 9,

    # Mean Reversion (Bollinger)
    "use_mean_reversion": True,
    "bb_period": 20,
    "bb_std": 2.0,
    "mr_rsi_max": 35,
    "mr_profit_target_pct": 0.05,

    # Risk
    "atr_period": 14,
    "risk_per_trade": 0.02,
    "max_equity_at_risk": 0.80,
    "initial_sl_atr_mult": 2.5,
    "trailing_atr_mult": 3.0,
    "partial_profit_pct": 0.25,
    "allow_reentry": True,
    "reentry_atr_mult": 1.5,

    # Filter
    "adx_period": 14,
    "adx_threshold": 15,
    "vix_high_threshold": 30,
    "vix_size_reduction": 0.5,
    "max_drawdown_pct": 0.15,
    "max_per_sector": 2,
    "max_volume_pct": 0.01,
    "sector_groups": {},

    # Multi-Timeframe (Backtest nutzt Daily-Proxy; Live Phase 2: echte Intraday-Bars)
    "use_multi_timeframe": False,
    "lower_ema_period": 20,
    "lower_rsi_min": 50,
    "pullback_entry": True,
    "mtf_pullback_proximity": 0.015,
    "mtf_breakout_lookback": 5,

    # ML (Phase 2 - nicht migriert)
    "use_ml": False,
    "ml_prob_threshold": 0.6,

    # Misc
    "initial_capital": 10_000.0,
}


# ─────────────────────────── Hilfs-Aggregation ──────────────────────────────

def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """Wandle gepufferte Bars in einen DataFrame mit UTC-Index."""
    if not bars:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    idx = pd.DatetimeIndex([b.timestamp for b in bars], tz="UTC")
    return pd.DataFrame({
        "Open": [b.open for b in bars],
        "High": [b.high for b in bars],
        "Low": [b.low for b in bars],
        "Close": [b.close for b in bars],
        "Volume": [b.volume for b in bars],
    }, index=idx)


def _compute_botti_indicators(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Berechne alle Botti-relevanten Indikatoren auf dem DataFrame."""
    out = df.copy()

    # Trend SMAs
    out["SMA_short"] = sma(out["Close"], cfg["sma_short"])
    out["SMA_long"] = sma(out["Close"], cfg["sma_long"])

    # ATR & ADX
    out["ATR"] = atr(out, cfg["atr_period"])
    out["ADX"] = adx(out, cfg["adx_period"])

    # RSI
    out["RSI"] = rsi(out["Close"], 14)

    # MACD
    macd_line, macd_sig, macd_hist = macd(
        out["Close"], cfg["macd_fast"], cfg["macd_slow"], cfg["macd_signal_period"],
    )
    out["MACD"] = macd_line
    out["MACD_signal"] = macd_sig
    out["MACD_hist"] = macd_hist

    # Bollinger Bands
    bb_upper, bb_mid, bb_lower = bollinger_bands(
        out["Close"], cfg["bb_period"], cfg["bb_std"],
    )
    out["BB_upper"] = bb_upper
    out["BB_mid"] = bb_mid
    out["BB_lower"] = bb_lower

    # Volume SMA
    out["Vol_SMA"] = sma(out["Volume"].astype(float), cfg["volume_sma_period"])

    # Fast Cross MAs
    if cfg.get("use_fast_cross", False):
        ft = cfg.get("fast_cross_type", "EMA")
        fs = cfg.get("fast_cross_short", 9)
        fl = cfg.get("fast_cross_long", 21)
        if ft == "EMA":
            out["FC_FAST"] = ema(out["Close"], fs)
            out["FC_SLOW"] = ema(out["Close"], fl)
        else:
            out["FC_FAST"] = sma(out["Close"], fs)
            out["FC_SLOW"] = sma(out["Close"], fl)

    return out


# ─────────────────────────── Strategie-Klasse ────────────────────────────────

@register("botti")
class BottiStrategy(BaseStrategy):
    """Trend Following + Mean Reversion (bar-zentriert, DI ueber Context-Service).

    Arbeitet auf Daily-Bars. Signaltypen:
      - botti_trend: Golden Cross, Fast Cross, Early GC, Pullback, Death Cross
      - botti_mr: Bollinger Mean Reversion
    """

    def __init__(self, config: dict, context=None):
        merged = dict(BOTTI_DEFAULT_PARAMS)
        merged.update(config or {})
        super().__init__(merged, context=context)

    @property
    def name(self) -> str:
        return "botti"

    def _is_ready(self) -> bool:
        min_bars = max(
            int(self.config.get("sma_long", 30)) + 2,
            int(self.config.get("min_bars", 35)),
        )
        return len(self.bars) >= min_bars

    # ── Core Signal-Logic ──────────────────────────────────────────────

    def _generate_signals(self, bar: Bar) -> list[Signal]:
        cfg = self.config
        symbol = bar.symbol

        # Bars -> DataFrame mit Indikatoren
        symbol_bars = [b for b in self.bars if b.symbol == symbol]
        df = _bars_to_df(symbol_bars)
        if df.empty or len(df) < int(cfg.get("sma_long", 30)) + 2:
            self._record_status(symbol, "NO_DATA", "zu wenige Daily-Bars")
            return []
        df = _compute_botti_indicators(df, cfg)

        last = df.iloc[-1]
        prev = df.iloc[-2]

        # ── Drawdown Circuit Breaker ─────────────────────────────────
        account = self.context.account
        peak = account.peak_equity
        equity = account.equity
        if equity <= 0:
            equity = float(cfg.get("initial_capital", 10_000.0))
        if peak <= 0:
            peak = equity
        if drawdown_breaker(equity, peak, float(cfg.get("max_drawdown_pct", 0.15))):
            log.debug("botti.drawdown_breaker", symbol=symbol,
                      equity=equity, peak=peak)
            dd_pct = (peak - equity) / peak if peak > 0 else 0.0
            self._record_status(
                symbol, "DD_BREAKER",
                f"Drawdown {dd_pct:.1%} ≥ {float(cfg.get('max_drawdown_pct', 0.15)):.1%}",
            )
            return []

        # ── VIX Size Factor ──────────────────────────────────────────
        vix_spot, _ = self.context.vix
        vix_factor = 1.0
        if vix_spot is not None:
            vix_factor = vix_size_factor(
                vix_spot, float(cfg.get("vix_high_threshold", 30)),
            )

        # ── Sector Cluster Guard ─────────────────────────────────────
        sector_map = cfg.get("sector_groups", {})
        if sector_map:
            if not sector_cluster_ok(
                symbol, self.context.open_symbols,
                sector_map, int(cfg.get("max_per_sector", 2)),
            ):
                log.debug("botti.sector_blocked", symbol=symbol)
                self._record_status(
                    symbol, "SECTOR_BLOCK",
                    f"max {int(cfg.get('max_per_sector', 2))} pro Sektor erreicht",
                )
                return []

        # ── Signal-Logik ─────────────────────────────────────────────
        signal_type, reason = self._classify_signal(df, last, prev, cfg)

        if signal_type == "SELL":
            self._record_status(symbol, "SIGNAL", f"SELL: {reason}")
            return [self._make_exit_signal(bar, reason)]

        if signal_type == "BUY_MR":
            self._record_status(symbol, "SIGNAL", f"BUY_MR: {reason}")
            return [self._make_mr_signal(bar, df, last, cfg, vix_factor, reason)]

        if signal_type == "BUY":
            # Multi-Timeframe-Filter (Daily-Proxy) – nur für Trend-BUYs.
            # Mean Reversion ist davon bewusst ausgenommen (v6-Konvention).
            if cfg.get("use_multi_timeframe", False):
                ok, mtf_reason = _daily_mtf_proxy(df, cfg)
                if not ok:
                    log.debug("botti.mtf_filtered", symbol=symbol,
                              reason=mtf_reason)
                    self._record_status(symbol, "MTF_BLOCK", mtf_reason)
                    return []
                reason = f"{reason} | MTF: {mtf_reason}"
            self._record_status(symbol, "SIGNAL", f"BUY: {reason}")
            return [self._make_trend_signal(bar, df, last, cfg, vix_factor, reason)]

        # signal_type == "HOLD" – granularer Statuscode
        self._record_status(symbol, *_hold_status(reason))
        return []

    # ── Signal-Klassifikation (pure Logik) ───────────────────────────

    @staticmethod
    def _classify_signal(
        df: pd.DataFrame,
        last: pd.Series,
        prev: pd.Series,
        cfg: dict,
    ) -> tuple[str, str]:
        """Bestimmt Signaltyp und Begruendung aus Indikator-Werten."""

        # ── Death Cross: immer SELL ──────────────────────────────────
        if (pd.notna(prev["SMA_short"]) and pd.notna(prev["SMA_long"])
                and prev["SMA_short"] >= prev["SMA_long"]
                and last["SMA_short"] < last["SMA_long"]):
            return "SELL", "Death Cross"

        # ── Mean Reversion (Bollinger) ───────────────────────────────
        if cfg.get("use_mean_reversion", False):
            bb_ok = pd.notna(last["BB_lower"]) and last["Close"] <= last["BB_lower"]
            rsi_ok = pd.notna(last["RSI"]) and last["RSI"] < cfg["mr_rsi_max"]
            vol_ok = pd.notna(last["Vol_SMA"]) and last["Volume"] > last["Vol_SMA"]
            if bb_ok and rsi_ok and vol_ok:
                return "BUY_MR", f"BB Lower Touch | RSI={last['RSI']:.0f}"

        # ── Fast Cross (EMA/SMA) ────────────────────────────────────
        if cfg.get("use_fast_cross", False) and "FC_FAST" in df.columns:
            fc_pf = prev.get("FC_FAST", float("nan"))
            fc_ps = prev.get("FC_SLOW", float("nan"))
            fc_lf = last.get("FC_FAST", float("nan"))
            fc_ls = last.get("FC_SLOW", float("nan"))
            if (pd.notna(fc_pf) and pd.notna(fc_ps)
                    and pd.notna(fc_lf) and pd.notna(fc_ls)
                    and float(fc_pf) <= float(fc_ps) and float(fc_lf) > float(fc_ls)):
                ok, reason = _confirmation_filters(last, cfg)
                if ok:
                    ft = cfg.get("fast_cross_type", "EMA")
                    fs = cfg.get("fast_cross_short", 9)
                    fl = cfg.get("fast_cross_long", 21)
                    return "BUY", f"Fast {ft} Cross ({fs}/{fl}) | {reason}"

        # ── Early Golden Cross ──────────────────────────────────────
        if cfg.get("use_early_golden_cross", False):
            sig, reason = _early_golden_cross(last, prev, cfg)
            if sig == "BUY":
                return sig, reason

        # ── Uptrend-Filter (fuer Trend-Signale ab hier) ─────────────
        if not (pd.notna(last["SMA_short"]) and pd.notna(last["SMA_long"])
                and last["SMA_short"] > last["SMA_long"]):
            return "HOLD", "no uptrend"
        if pd.notna(last["ADX"]) and last["ADX"] < cfg["adx_threshold"]:
            return "HOLD", f"ADX {last['ADX']:.1f} < {cfg['adx_threshold']}"

        # ── Pullback-Entry nach Cross ───────────────────────────────
        if cfg.get("use_pullback_entry_daily", False):
            sig, reason = _pullback_signal(df, cfg)
            if sig == "BUY":
                return sig, reason

        # ── Golden Cross ────────────────────────────────────────────
        golden = (pd.notna(prev["SMA_short"]) and pd.notna(prev["SMA_long"])
                  and prev["SMA_short"] <= prev["SMA_long"]
                  and last["SMA_short"] > last["SMA_long"])
        if not golden:
            return "HOLD", ""

        ok, reason = _confirmation_filters(last, cfg)
        if ok:
            return "BUY", f"Golden Cross + {reason}"
        return "HOLD", reason

    # ── Signal-Builder ──────────────────────────────────────────────

    @staticmethod
    def _build_features(last: pd.Series, cfg: dict) -> FeatureVector:
        close = float(last["Close"]) if pd.notna(last["Close"]) else 1.0
        sma_s = float(last.get("SMA_short", 0.0) or 0.0)
        sma_l = float(last.get("SMA_long", 0.0) or 0.0)
        atr_val = float(last.get("ATR", 0.0) or 0.0)
        vol_sma = float(last.get("Vol_SMA", 0.0) or 0.0)
        volume = float(last.get("Volume", 0.0) or 0.0)
        return FeatureVector(
            sma_diff=(sma_s - sma_l) / (sma_l + 1e-9) if sma_l else 0.0,
            adx=float(last.get("ADX", 0.0) or 0.0),
            atr_pct=atr_val / close if close > 0 else 0.0,
            rsi=float(last.get("RSI", 0.0) or 0.0),
            macd_hist=float(last.get("MACD_hist", 0.0) or 0.0),
            volume_ratio=volume / vol_sma if vol_sma > 0 else 1.0,
        )

    @staticmethod
    def _resolve_reserve_group(symbol: str, cfg: dict) -> Optional[str]:
        """Bestimme den Sektor-Gruppennamen für ``symbol`` oder None.

        Grundlage ist ``sector_groups`` aus der Config (identisch zum
        Sector-Cluster-Guard). Der Wert landet als ``reserve_group`` in
        den Signal-Metadaten, damit ``trade_manager.register_and_persist``
        ihn in die ``trades.group_name``-Spalte UND ``reserved_groups`` für
        MIT-Independence persistiert.
        """
        groups = cfg.get("sector_groups") or {}
        if not isinstance(groups, dict) or not groups:
            return None
        name = correlation_group(symbol, groups)
        return name or None

    def _make_trend_signal(
        self, bar: Bar, df: pd.DataFrame, last: pd.Series,
        cfg: dict, vix_factor: float, reason: str,
    ) -> Signal:
        atr_val = float(last["ATR"]) if pd.notna(last["ATR"]) else 0.0
        entry = float(last["Close"])
        stop = atr_stop("long", entry, atr_val,
                        float(cfg.get("initial_sl_atr_mult", 2.5)))
        target = entry + float(cfg.get("trailing_atr_mult", 3.0)) * atr_val

        metadata: dict = {
            "entry_price": entry,
            "atr": atr_val,
            "vix_factor": vix_factor,
            "reason": reason,
            "cross_type": reason.split("|")[0].strip() if "|" in reason else reason,
        }
        group = self._resolve_reserve_group(bar.symbol, cfg)
        if group:
            metadata["reserve_group"] = group

        return Signal(
            strategy="botti_trend",
            symbol=bar.symbol,
            features=self._build_features(last, cfg),
            direction=1,
            strength=min(1.0, float(last.get("ADX", 25.0) or 25.0) / 50.0),
            stop_price=stop,
            target_price=target,
            timestamp=bar.timestamp,
            metadata=metadata,
        )

    def _make_mr_signal(
        self, bar: Bar, df: pd.DataFrame, last: pd.Series,
        cfg: dict, vix_factor: float, reason: str,
    ) -> Signal:
        entry = float(last["Close"])
        bb_mid = float(last["BB_mid"]) if pd.notna(last["BB_mid"]) else entry * 1.05
        mr_target = entry * (1 + float(cfg.get("mr_profit_target_pct", 0.05)))
        atr_val = float(last["ATR"]) if pd.notna(last["ATR"]) else 0.0
        stop = atr_stop("long", entry, atr_val,
                        float(cfg.get("initial_sl_atr_mult", 2.5)))

        metadata: dict = {
            "entry_price": entry,
            "atr": atr_val,
            "vix_factor": vix_factor,
            "mr_target": mr_target,
            "bb_mid": bb_mid,
            "reason": reason,
        }
        group = self._resolve_reserve_group(bar.symbol, cfg)
        if group:
            metadata["reserve_group"] = group

        return Signal(
            strategy="botti_mr",
            symbol=bar.symbol,
            features=self._build_features(last, cfg),
            direction=1,
            strength=0.6,
            stop_price=stop,
            target_price=mr_target,
            timestamp=bar.timestamp,
            metadata=metadata,
        )

    @staticmethod
    def _make_exit_signal(bar: Bar, reason: str) -> Signal:
        return Signal(
            strategy="botti_trend",
            symbol=bar.symbol,
            direction=0,
            strength=1.0,
            stop_price=0.0,
            target_price=None,
            timestamp=bar.timestamp,
            metadata={"reason": reason},
        )


# ─────────────────────────── Status-Code-Mapping ────────────────────────────

def _hold_status(reason: str) -> tuple[str, str]:
    """Übersetzt die interne HOLD-Begründung in (code, reason) fürs Monitoring."""
    if not reason:
        return "WAIT_SETUP", "kein Setup"
    r = reason.lower()
    if "no uptrend" in r:
        return "NO_UPTREND", reason
    if r.startswith("adx"):
        return "WEAK_TREND", reason
    if "rsi" in r and "out of range" in r:
        return "RSI_BLOCK", reason
    if "macd" in r:
        return "MACD_BLOCK", reason
    if "volume" in r:
        return "VOLUME_BLOCK", reason
    return "WAIT_SETUP", reason


# ─────────────────────────── Reine Hilfsfunktionen ──────────────────────────

def _confirmation_filters(last: pd.Series, cfg: dict) -> tuple[bool, str]:
    """RSI + MACD + Volume Bestaetigungsfilter. Returns (ok, detail_string)."""
    parts: list[str] = []

    if cfg.get("use_rsi_filter", True):
        rsi_val = last.get("RSI", float("nan"))
        if pd.isna(rsi_val) or not (cfg["rsi_buy_min"] <= rsi_val <= cfg["rsi_buy_max"]):
            return False, f"RSI {rsi_val:.0f} out of range"
        parts.append(f"RSI={float(rsi_val):.0f}")

    if cfg.get("use_macd_filter", True):
        mh = last.get("MACD_hist", float("nan"))
        if pd.isna(mh) or float(mh) <= 0:
            return False, "MACD hist negative"
        parts.append("MACD+")

    if cfg.get("use_volume_filter", True):
        vol_sma = last.get("Vol_SMA", float("nan"))
        if pd.isna(vol_sma) or last["Volume"] < vol_sma:
            return False, "Volume below SMA"
        parts.append("Vol+")

    return True, " | ".join(parts) if parts else "confirmed"


def _early_golden_cross(
    last: pd.Series, prev: pd.Series, cfg: dict,
) -> tuple[str, str]:
    """Pre-Cross-Signal: SMA_short naehert sich SMA_long von unten."""
    s_short = last.get("SMA_short", float("nan"))
    s_long = last.get("SMA_long", float("nan"))
    p_short = prev.get("SMA_short", float("nan"))
    p_long = prev.get("SMA_long", float("nan"))

    if not (pd.notna(s_short) and pd.notna(s_long) and float(s_short) < float(s_long)):
        return "HOLD", ""

    sma_dist = (float(s_long) - float(s_short)) / (float(s_long) + 1e-9)
    approaching = sma_dist <= cfg.get("early_gc_proximity_pct", 0.02)
    converging = (pd.notna(p_short) and pd.notna(p_long)
                  and (float(s_short) - float(p_short)) > (float(s_long) - float(p_long)))

    rsi_val = last.get("RSI", float("nan"))
    rsi_ok = pd.notna(rsi_val) and float(rsi_val) >= cfg.get("early_gc_rsi_min", 55)

    mh_now = last.get("MACD_hist", float("nan"))
    mh_prev = prev.get("MACD_hist", float("nan"))
    macd_strong = (pd.notna(mh_now) and pd.notna(mh_prev)
                   and float(mh_now) > 0 and float(mh_now) > float(mh_prev))

    vol_ok = True
    if cfg.get("use_volume_filter", True):
        vol_sma = last.get("Vol_SMA", float("nan"))
        vol_ok = pd.notna(vol_sma) and last["Volume"] > vol_sma

    if approaching and converging and rsi_ok and macd_strong and vol_ok:
        return "BUY", (f"Early Golden Cross | SMA {sma_dist:.1%} unter SMA_long"
                       f" | RSI={float(rsi_val):.0f}")
    return "HOLD", ""


def _recent_cross(df: pd.DataFrame, lookback: int) -> bool:
    """True wenn in den letzten lookback Bars ein GC oder Fast Cross stattfand."""
    n = min(lookback + 1, len(df))
    window = df.iloc[-n:]
    for i in range(1, len(window)):
        p = window.iloc[i - 1]
        c = window.iloc[i]
        # Golden Cross
        if (pd.notna(p["SMA_short"]) and pd.notna(p["SMA_long"])
                and pd.notna(c["SMA_short"]) and pd.notna(c["SMA_long"])
                and float(p["SMA_short"]) <= float(p["SMA_long"])
                and float(c["SMA_short"]) > float(c["SMA_long"])):
            return True
        # Fast Cross
        if "FC_FAST" in df.columns:
            pf = p.get("FC_FAST", float("nan"))
            ps = p.get("FC_SLOW", float("nan"))
            cf = c.get("FC_FAST", float("nan"))
            cs = c.get("FC_SLOW", float("nan"))
            if (pd.notna(pf) and pd.notna(ps) and pd.notna(cf) and pd.notna(cs)
                    and float(pf) <= float(ps) and float(cf) > float(cs)):
                return True
    return False


def _pullback_signal(df: pd.DataFrame, cfg: dict) -> tuple[str, str]:
    """Pullback-Entry nach Golden/Fast Cross auf Daily-Bars."""
    if len(df) < 3:
        return "HOLD", ""

    lookback = cfg.get("pullback_daily_lookback", 15)
    if not _recent_cross(df, lookback):
        return "HOLD", ""

    last = df.iloc[-1]
    prev = df.iloc[-2]

    if pd.isna(last["SMA_short"]) or pd.isna(last["SMA_long"]):
        return "HOLD", ""
    if float(last["SMA_short"]) <= float(last["SMA_long"]):
        return "HOLD", ""

    prox = cfg.get("pullback_daily_proximity", 0.015)
    ema_p = cfg.get("pullback_daily_ema", 20)
    sma_val = float(last["SMA_short"])
    ema_val = float(ema(df["Close"], ema_p).iloc[-1])

    touched_sma = float(last["Low"]) <= sma_val * (1 + prox)
    touched_ema = float(last["Low"]) <= ema_val * (1 + prox)
    if not (touched_sma or touched_ema):
        return "HOLD", ""

    bullish = (float(last["Close"]) > float(last["Open"])
               or float(last["Close"]) > float(prev["Close"]))
    if not bullish:
        return "HOLD", ""

    rsi_min = cfg.get("pullback_daily_rsi_min", 50)
    rsi_val = last.get("RSI", float("nan"))
    if pd.isna(rsi_val) or float(rsi_val) < rsi_min:
        return "HOLD", ""

    if cfg.get("use_macd_filter", True):
        mh = last.get("MACD_hist", float("nan"))
        if pd.isna(mh) or float(mh) <= 0:
            return "HOLD", ""

    if cfg.get("use_volume_filter", True):
        vol_sma = last.get("Vol_SMA", float("nan"))
        if pd.isna(vol_sma) or last["Volume"] < vol_sma:
            return "HOLD", ""

    ref = "SMA" if touched_sma else f"EMA{ema_p}"
    return "BUY", f"Pullback-Entry -> {ref} | RSI={float(rsi_val):.0f}"


def _daily_mtf_proxy(df: pd.DataFrame, cfg: dict) -> tuple[bool, str]:
    """Backtest-Proxy fuer den Intraday-MTF-Filter aus v6.

    Simuliert den Lower-Timeframe-Check auf Basis des Daily-Bars:
      1) Pullback-Proxy: Bar-Low <= EMA<n> * (1 + proximity)  (EMA intraday beruehrt)
      2) Momentum: RSI(14) > lower_rsi_min
      3) Trigger: MACD-Hist > 0 UND steigend  ODER  Close > max(High der letzten N Bars)

    Alle drei Bedingungen muessen erfuellt sein (bei pullback_entry=True).
    Bei pullback_entry=False: Close > EMA statt Pullback-Touch.

    Look-ahead-frei: nutzt nur Werte aus ``df`` bis einschliesslich des aktuellen
    (abgeschlossenen) Daily-Bars. Entry erfolgt zum Close dieses Bars.
    """
    if len(df) < 30:
        return False, "zu wenige Daily-Daten fuer MTF-Proxy"

    ema_p = int(cfg.get("lower_ema_period", 20))
    proximity = float(cfg.get("mtf_pullback_proximity", 0.015))
    lookback = int(cfg.get("mtf_breakout_lookback", 5))

    ema_series = ema(df["Close"], ema_p)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    ema_val = float(ema_series.iloc[-1])
    if not np.isfinite(ema_val) or ema_val <= 0:
        return False, "EMA nicht verfuegbar"

    # 1) Pullback-Proxy: Low touchierte EMA waehrend der Tageskerze
    touched_ema = float(last["Low"]) <= ema_val * (1 + proximity)

    # 2) RSI-Momentum
    rsi_val = last.get("RSI", float("nan"))
    momentum_ok = (pd.notna(rsi_val)
                   and float(rsi_val) > float(cfg.get("lower_rsi_min", 50)))

    # 3a) MACD-Histogramm positiv UND steigend
    mh_now = last.get("MACD_hist", float("nan"))
    mh_prev = prev.get("MACD_hist", float("nan"))
    macd_ok = (pd.notna(mh_now) and pd.notna(mh_prev)
               and float(mh_now) > 0 and float(mh_now) > float(mh_prev))

    # 3b) Breakout ueber Hoch der letzten <lookback> Bars (exklusive aktuell)
    breakout_ok = False
    recent_high = float("nan")
    if len(df) > lookback:
        recent_high = float(df["High"].iloc[-(lookback + 1):-1].max())
        breakout_ok = float(last["Close"]) > recent_high

    if cfg.get("pullback_entry", True):
        if touched_ema and momentum_ok and (macd_ok or breakout_ok):
            parts = [f"Pullback EMA{ema_p}", f"RSI={float(rsi_val):.0f}"]
            if macd_ok:
                parts.append("MACD↑")
            if breakout_ok:
                parts.append(f"Breakout>{recent_high:.2f}")
            return True, " | ".join(parts)
        if not touched_ema:
            return False, f"kein Pullback zur EMA{ema_p}"
        if not momentum_ok:
            return False, f"RSI {float(rsi_val):.0f} < {cfg.get('lower_rsi_min', 50)}"
        return False, "kein Trigger (MACD↑ oder Breakout)"

    # pullback_entry=False: Close muss ueber EMA liegen
    if momentum_ok and float(last["Close"]) > ema_val and (macd_ok or breakout_ok):
        trigger = "MACD↑" if macd_ok else f"Breakout>{recent_high:.2f}"
        return True, f"Momentum RSI={float(rsi_val):.0f} | {trigger}"
    return False, "kein Momentum-Setup"
