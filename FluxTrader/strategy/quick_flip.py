"""Quick Flip Scalper – Reversal-nach-Sweep an der Opening Range.

Regelbasierte Variante der klassischen Opening Range. Anders als ORB (das auf
Breakout-Continuation setzt) sucht Quick Flip nach einem Stop-Hunt über/unter
der Opening-Range-Box und tradet die Gegenbewegung, sobald ein
Reversal-Candlestick-Pattern erscheint.

Ablauf (vier sequenzielle Schritte, state-machine-gesteuert):
  1. Opening Range der ersten 15 Min boxen (or_high/or_low).
  2. Liquidity Candle erkennen: Candle-Range >= 25 % Daily ATR.
     - rote Liquidity-Candle -> Long-Setup (bearischer Sweep)
     - grüne Liquidity-Candle -> Short-Setup (bullischer Sweep)
  3. Reversal-Candle auf 5m: Hammer/Engulfing jenseits der OR-Box.
  4. Entry/Stop/Target aus Candle-Geometrie + OR-Box. R:R-Filter.

Broker-agnostisch: kein Netzwerk, kein Broker-Import (CLAUDE.md Regel 1).
"""
from __future__ import annotations

from datetime import datetime, time
from typing import Optional

import numpy as np
import pandas as pd

from core.filters import (
    correlation_group,
    entry_cutoff_ok,
    gap_filter,
    is_market_hours,
    is_within_trade_window,
    mit_independence_blocked,
    to_et,
    to_et_time,
    trend_filter_from_spy,
    vix_size_factor,
)
from core.indicators import (
    atr,
    detect_reversal_pattern,
    is_liquidity_candle,
    opening_range_levels,
    resample_ohlcv,
)
from core.logging import get_logger
from core.models import Bar, FeatureVector, Signal
from core.risk import position_size
from strategy.base import BaseStrategy
from strategy.registry import register

log = get_logger(__name__)


# ─────────────────────────── Default-Config ─────────────────────────────────

QUICK_FLIP_DEFAULT_PARAMS: dict = {
    "opening_range_minutes": 15,           # Länge der Opening-Range-Box in Minuten
    "liquidity_atr_threshold": 0.25,       # Min. Candle-Range als Anteil der 14-Tage-ATR für Liquidity-Signal
    "max_trade_window_minutes": 90,        # Maximales Handelsfenster nach Open in Minuten
    "min_rr_ratio": 1.5,                   # Mindest-Risk-Reward-Ratio; Trade wird übersprungen wenn darunter
    "buffer_ticks": 0.05,                  # Stop-Puffer in $ jenseits Reversal-Candle-Extremum
    "min_signal_strength": 0.35,           # Mindeststärke des Signals (0–1) für Ausgabe
    "allow_shorts": True,                  # Short-Trades erlauben (False = nur Long)
    "use_trend_filter": True,              # SPY-Trendfilter aktivieren (True empfohlen)
    "trend_ema_period": 20,                # EMA-Periode für SPY-Trendfilter
    "use_gap_filter": True,                # Gap-Filter bei Marktöffnung aktivieren
    "max_gap_pct": 0.03,                   # Maximaler Gap zur Vortagsclose in % (>3 % = kein Trade)
    "use_mit_overlay": True,               # MIT-Independence-Overlay für Position Sizing aktivieren
    "use_vix_filter": True,                # VIX-Regime-Filter aktivieren
    "vix_high_threshold": 30.0,            # Ab diesem VIX-Wert Position Size reduzieren
    "vix_size_factor": 0.5,                # Größen-Multiplikator bei hohem VIX (0.5 = halbe Größe)
    "entry_cutoff_time": time(10, 45),     # Kein neuer Trade-Entry nach dieser Uhrzeit ET
    "risk_per_trade": 0.005,               # Risiko je Trade als Anteil der Equity (0.5 %)
    # --- Candlestick-Pattern-Parameter (tunable für Backtest-Optimierung) ---
    "hammer_shadow_ratio": 2.0,            # Min. unterer Schatten als Vielfaches des Body (Hammer)
    "hammer_upper_shadow_ratio": 0.3,      # Max. oberer Schatten als Anteil des Body bei Hammer
    "engulfing_min_body_ratio": 0.6,       # Min. Body-Größe der Engulfing-Candle relativ zum Vorgänger
    # --- MIT Independence (Cross-Symbol-Kluster-Schutz) ---
    "mit_correlation_groups": {
        "index_etfs": ["SPY", "QQQ", "IWM", "DIA"],
        "semi_ai": ["NVDA", "AMD", "AVGO"],
        "mega_cap_tech": ["AAPL", "MSFT", "META", "AMZN", "GOOGL"],
    },
    # --- Warmup / Buffer ---
    "min_bars": 20,                        # Minimale Bar-Anzahl im Buffer vor erstem Signal
    "max_bars_buffer": 2000,               # Max. Bar-Anzahl im Strategy-Buffer
}


# ─────────────────────────── Hilfs-Aggregation ──────────────────────────────

def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """Wandele gepufferte Bars in einen DataFrame mit UTC-Index."""
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


# ─────────────────────────── Strategie-Klasse ──────────────────────────────

@register("quick_flip")
class QuickFlipStrategy(BaseStrategy):
    """Quick Flip Scalper – Reversal nach Liquidity-Sweep an der Opening Range.

    State Machine (`_day_cache["state"]`):
      idle           -> OR noch nicht vollständig.
      or_locked      -> OR-Box steht. Warte auf Liquidity-Candle.
      liquidity_seen -> Liquidity erkannt. Warte auf Reversal-Candle.
      done           -> Trade ausgeführt oder Zeitfenster abgelaufen.

    Bewusste Abweichung vom ORB-Pattern: ORB nutzt implizites Cache-Key-Checking,
    was bei 2 Schritten funktioniert. Quick Flip hat 4 sequenzielle Schritte —
    dort ist eine explizite State Machine robuster und debuggbarer. Dieses
    Muster ist als Vorlage für zukünftige sequenzielle Strategien gedacht.
    """

    def __init__(self, config: dict, context=None):
        merged = dict(QUICK_FLIP_DEFAULT_PARAMS)
        merged.update(config or {})
        # entry_cutoff_time kann als String aus YAML kommen -> time konvertieren
        cutoff = merged.get("entry_cutoff_time")
        if isinstance(cutoff, str):
            parts = cutoff.split(":")
            merged["entry_cutoff_time"] = time(int(parts[0]), int(parts[1]))
        super().__init__(merged, context=context)
        self._day_cache: dict = self._fresh_day_cache()

    @property
    def name(self) -> str:
        return "quick_flip"

    def _is_ready(self) -> bool:
        min_bars = int(self.config.get("min_bars", 20))
        if self.bars:
            cursor = self.context.bar_cursor(self.bars[-1].symbol)
            if cursor >= 0:
                return cursor + 1 >= min_bars
        return len(self.bars) >= min_bars

    def reset(self) -> None:
        super().reset()
        self._day_cache = self._fresh_day_cache()

    # ── Core Signal-Logic ──────────────────────────────────────────────

    def _generate_signals(self, bar: Bar) -> list[Signal]:
        cfg = self.config
        symbol = bar.symbol
        current_time = bar.timestamp

        # ── Precomputed-Frame Fast-Path (analog ORB) ─────────────────
        full_df = self.context.indicator_frame(symbol)
        if full_df is not None:
            cursor = self.context.bar_cursor(symbol)
            if cursor < 5:
                self._record_status(symbol, "NO_DATA",
                                    f"warmup cursor={cursor}")
                return []
            df_5m = full_df.iloc[:cursor + 1]
        else:
            df_5m = _bars_to_df([b for b in self.bars if b.symbol == symbol]
                                or self.context.bars(symbol))
            if df_5m.empty or len(df_5m) < 6:
                self._record_status(symbol, "NO_DATA",
                                    f"bars={len(df_5m)}")
                return []

        # Market-Hours Gate
        if not is_market_hours(current_time):
            self._record_status(symbol, "OUTSIDE_HOURS",
                                "außerhalb Handelszeiten")
            return []

        # Day-Reset
        day_et = to_et(current_time)
        today = day_et.date() if hasattr(day_et, "date") else day_et
        if self._day_cache.get("date") != today:
            self._day_cache = self._fresh_day_cache()
            self._day_cache["date"] = today
            self._day_cache["open_time_et"] = self._market_open_for(day_et)

        # Entry-Cutoff (absolute ET-Uhrzeit)
        if not entry_cutoff_ok(current_time, cfg.get("entry_cutoff_time")):
            self._day_cache["state"] = "done"
            self._record_status(symbol, "ENTRY_CUTOFF",
                                "nach Entry-Cutoff")
            return []

        # Trade-Window abgelaufen?
        if self._check_time_window_expired(current_time):
            self._day_cache["state"] = "done"
            self._record_status(symbol, "WINDOW_EXPIRED",
                                "Handelsfenster abgelaufen")
            return []

        state = self._day_cache.get("state", "idle")
        if state == "done":
            return []

        # ── State: idle -> Daily ATR + OR-Lock ──────────────────────
        et_time = to_et_time(current_time)
        orb_minutes = int(cfg.get("opening_range_minutes", 15))
        or_end_minutes = 9 * 60 + 30 + orb_minutes
        cur_minutes = et_time.hour * 60 + et_time.minute

        if state == "idle":
            if self._day_cache.get("daily_atr") is None:
                daily_atr = self._compute_daily_atr(df_5m)
                if daily_atr is None:
                    log.warning("quick_flip.daily_atr_unavailable",
                                symbol=symbol)
                    self._record_status(symbol, "NO_DAILY_ATR",
                                        "Daily ATR nicht berechenbar")
                    return []
                self._day_cache["daily_atr"] = daily_atr

            if cur_minutes < or_end_minutes:
                self._record_status(symbol, "WAIT_OR",
                                    f"OR-Periode, {orb_minutes}m")
                return []

            self._lock_opening_range(df_5m)
            if self._day_cache.get("or_high") is None:
                self._record_status(symbol, "NO_OR",
                                    "OR-Box nicht berechenbar")
                return []
            self._day_cache["state"] = "or_locked"
            state = "or_locked"

        # ── Gap-Filter (einmalig, sobald OR steht) ──────────────────
        if state == "or_locked" and cfg.get("use_gap_filter", True) \
                and not self._day_cache.get("gap_checked"):
            gap_ok, gap_pct = self._gap_check(
                df_5m, float(cfg.get("max_gap_pct", 0.03)),
            )
            self._day_cache["gap_checked"] = True
            if not gap_ok:
                self._day_cache["state"] = "done"
                log.debug("quick_flip.gap_block", symbol=symbol, gap_pct=gap_pct)
                self._record_status(symbol, "GAP_BLOCK",
                                    f"gap {gap_pct*100:.2f}%")
                return []

        # ── State: or_locked -> Liquidity Candle ────────────────────
        if state == "or_locked":
            found, direction = self._detect_liquidity_candle(df_5m)
            if not found:
                self._record_status(symbol, "WAIT_LIQUIDITY",
                                    "warte auf Liquidity-Candle")
                return []
            if direction == "short" and not cfg.get("allow_shorts", True):
                self._record_status(symbol, "SHORTS_DISABLED",
                                    "Shorts deaktiviert – Setup verworfen")
                self._day_cache["state"] = "done"
                return []
            self._day_cache["liquidity_direction"] = direction
            self._day_cache["liquidity_ts"] = current_time
            self._day_cache["state"] = "liquidity_seen"
            state = "liquidity_seen"

        # ── State: liquidity_seen -> Reversal-Candle + Signal ───────
        if state == "liquidity_seen":
            direction = self._day_cache["liquidity_direction"]
            signal = self._detect_reversal_candle(df_5m, direction, bar)
            if signal is None:
                self._record_status(symbol, "WAIT_REVERSAL",
                                    f"warte auf Reversal ({direction})")
                return []
            self._day_cache["state"] = "done"
            self._record_status(symbol, "SIGNAL",
                                signal.metadata.get("reason", ""))
            return [signal]

        return []

    # ── Helper: State / Cache ──────────────────────────────────────

    @staticmethod
    def _fresh_day_cache() -> dict:
        return {
            "date": None,
            "state": "idle",
            "open_time_et": None,
            "or_high": None,
            "or_low": None,
            "or_range": None,
            "daily_atr": None,
            "liquidity_direction": None,
            "liquidity_ts": None,
            "gap_checked": False,
        }

    @staticmethod
    def _market_open_for(dt_et) -> datetime:
        """Market-Open (9:30 ET) für den Handelstag von dt_et."""
        et_dt = to_et(dt_et)
        return et_dt.replace(hour=9, minute=30, second=0, microsecond=0)

    def _check_time_window_expired(self, now: datetime) -> bool:
        cfg = self.config
        window = int(cfg.get("max_trade_window_minutes", 90))
        open_time = self._day_cache.get("open_time_et")
        if open_time is None:
            open_time = self._market_open_for(now)
            self._day_cache["open_time_et"] = open_time
        return not is_within_trade_window(now, open_time, window)

    # ── Helper: Indikator-Berechnung ──────────────────────────────

    def _compute_daily_atr(self, df_5m: pd.DataFrame) -> Optional[float]:
        """Berechnet Daily ATR(14) aus resampelten 5m-Bars in ET.

        Letzter (heutiger, unvollständiger) Tag wird vor ATR-Berechnung
        entfernt, damit kein Look-Ahead entsteht. Braucht >= 14 komplette
        Vortage.
        """
        if df_5m.empty:
            return None
        df = df_5m.copy()
        idx_et = to_et(df.index)
        df.index = idx_et
        daily = df.resample("1D").agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }).dropna(subset=["Open"])
        today_norm = idx_et[-1].normalize()
        completed = daily[daily.index.normalize() < today_norm]
        if len(completed) < 14:
            return None
        series = atr(completed, 14).dropna()
        if series.empty:
            return None
        return float(series.iloc[-1])

    def _lock_opening_range(self, df_5m: pd.DataFrame) -> None:
        """Berechnet or_high/or_low aus den ersten 15 Min des heutigen Tages."""
        cfg = self.config
        orb_minutes = int(cfg.get("opening_range_minutes", 15))
        today = self._day_cache["date"]
        df = df_5m.copy()
        df.index = to_et(df.index)
        day_mask = np.asarray([d.date() == today for d in df.index], dtype=bool)
        day_df = df[day_mask]
        if day_df.empty:
            return
        hi, lo, rng = opening_range_levels(day_df, orb_minutes)
        if rng <= 0:
            return
        self._day_cache["or_high"] = hi
        self._day_cache["or_low"] = lo
        self._day_cache["or_range"] = rng

    def _detect_liquidity_candle(
        self, df_5m: pd.DataFrame,
    ) -> tuple[bool, str]:
        """Erkennt Liquidity-Candle auf dem 15m-Chart (resampled aus 5m).

        Returns (found, direction). direction="long" nach roter Candle
        (bearischer Sweep -> Reversal nach oben erwartet),
        direction="short" nach grüner Candle.
        """
        cfg = self.config
        daily_atr = self._day_cache.get("daily_atr")
        if daily_atr is None or daily_atr <= 0:
            return False, ""
        threshold = float(cfg.get("liquidity_atr_threshold", 0.25))

        df = df_5m.copy()
        df.index = to_et(df.index)
        today = self._day_cache["date"]
        day_mask = np.asarray([d.date() == today for d in df.index], dtype=bool)
        day_df = df[day_mask]
        if day_df.empty:
            return False, ""

        df_15m = resample_ohlcv(day_df, "15M")
        if df_15m.empty:
            return False, ""

        # Ersten 15m-Bar (Opening Range selbst) ausschließen
        orb_minutes = int(cfg.get("opening_range_minutes", 15))
        or_end = pd.Timestamp(today).tz_localize(df_15m.index.tz) \
            + pd.Timedelta(hours=9, minutes=30 + orb_minutes)
        post_or = df_15m[df_15m.index >= or_end]
        if post_or.empty:
            return False, ""

        last = post_or.iloc[-1]
        if not is_liquidity_candle(
            float(last["High"]), float(last["Low"]), daily_atr, threshold,
        ):
            return False, ""

        close = float(last["Close"])
        opn = float(last["Open"])
        if close < opn:
            return True, "long"
        if close > opn:
            return True, "short"
        return False, ""

    def _detect_reversal_candle(
        self,
        df_5m: pd.DataFrame,
        direction: str,
        bar: Bar,
    ) -> Optional[Signal]:
        """Prüft letzten 5m-Bar auf Reversal-Pattern jenseits der OR-Box.

        Long-Setup: Reversal unter or_low, Pattern hammer/bullish_engulfing.
        Short-Setup: Reversal über or_high, Pattern inv_hammer/bearish_engulfing.

        Gibt bei Erfolg ein voll befülltes Signal-Objekt zurück, sonst None.
        """
        cfg = self.config
        or_high = self._day_cache.get("or_high")
        or_low = self._day_cache.get("or_low")
        daily_atr = self._day_cache.get("daily_atr")
        if or_high is None or or_low is None:
            return None

        # Nur Bars nach der Liquidity-Candle betrachten
        liq_ts = self._day_cache.get("liquidity_ts")
        if liq_ts is not None and bar.timestamp < liq_ts:
            return None

        # Sweep-Bedingung: letzter Bar muss jenseits der OR-Box gehandelt haben
        last_low = float(bar.low)
        last_high = float(bar.high)
        if direction == "long" and last_low >= or_low:
            return None
        if direction == "short" and last_high <= or_high:
            return None

        pattern = detect_reversal_pattern(
            df_5m,
            direction=direction,
            hammer_shadow_ratio=float(cfg.get("hammer_shadow_ratio", 2.0)),
            hammer_upper_shadow_ratio=float(
                cfg.get("hammer_upper_shadow_ratio", 0.3)),
            engulfing_min_body_ratio=float(
                cfg.get("engulfing_min_body_ratio", 0.6)),
        )
        if pattern is None:
            return None

        # Trend-Filter (optional)
        trend = {"bullish": True, "bearish": True}
        if cfg.get("use_trend_filter", True):
            trend = trend_filter_from_spy(
                self.context.spy_df_asof(bar.timestamp),
                int(cfg.get("trend_ema_period", 20)),
            )
            if direction == "long" and not trend["bullish"]:
                log.debug("quick_flip.trend_block",
                          symbol=bar.symbol, direction=direction)
                return None
            if direction == "short" and not trend["bearish"]:
                log.debug("quick_flip.trend_block",
                          symbol=bar.symbol, direction=direction)
                return None

        # MIT-Independence-Guard (Cross-Symbol)
        if cfg.get("use_mit_overlay", True):
            groups = cfg.get("mit_correlation_groups", {})
            blocked, reason = mit_independence_blocked(
                bar.symbol,
                self.context.open_symbols,
                self.context.reserved_groups,
                groups,
            )
            if blocked:
                log.debug("quick_flip.mit_blocked",
                          symbol=bar.symbol, reason=reason)
                return None

        # Entry / Stop / Target
        buffer_ticks = float(cfg.get("buffer_ticks", 0.05))
        close = float(bar.close)
        if direction == "long":
            entry = close
            stop = last_low - buffer_ticks
            target = float(or_high)
            if stop >= entry or target <= entry:
                return None
        else:
            entry = close
            stop = last_high + buffer_ticks
            target = float(or_low)
            if stop <= entry or target >= entry:
                return None

        risk = abs(entry - stop)
        reward = abs(target - entry)
        if risk <= 0:
            return None
        rr_ratio = reward / risk
        min_rr = float(cfg.get("min_rr_ratio", 1.5))
        if rr_ratio < min_rr:
            log.debug("quick_flip.rr_reject",
                      symbol=bar.symbol, rr=rr_ratio, min_rr=min_rr)
            return None

        # Signal-Stärke aus R:R + Pattern-Qualität
        strength = float(np.clip(
            0.35 + 0.10 * (rr_ratio - min_rr) + (0.10 if pattern in
                                                 ("bullish_engulfing",
                                                  "bearish_engulfing") else 0.0),
            0.0, 1.0,
        ))
        min_strength = float(cfg.get("min_signal_strength", 0.35))
        if strength < min_strength:
            return None

        # Sizing: Basisgröße -> VIX-Skalierung -> MIT-Faktor
        equity = float(self.context.account.equity) or 100_000.0
        base_qty = position_size(
            equity=equity,
            risk_pct=float(cfg.get("risk_per_trade", 0.005)),
            entry=entry,
            stop=stop,
        )

        vix_factor = 1.0
        if cfg.get("use_vix_filter", True):
            vix_spot, _ = self.context.vix
            if vix_spot is not None:
                vix_thr = float(cfg.get("vix_high_threshold", 30.0))
                if float(vix_spot) > vix_thr:
                    vix_factor = float(cfg.get("vix_size_factor", 0.5))

        qty_factor = 1.0   # MIT-Overlay-Platzhalter: derzeit neutral
        final_qty = int(max(0, round(base_qty * vix_factor * qty_factor)))

        # FeatureVector: ATR% + Volume-Ratio (rolling 20)
        vol_ratio = 1.0
        if len(df_5m) >= 20 and "Volume" in df_5m.columns:
            recent_vol = float(df_5m["Volume"].tail(20).mean())
            if recent_vol > 0:
                vol_ratio = float(bar.volume) / recent_vol
        atr_pct = 0.0
        if daily_atr and close > 0:
            atr_pct = float(daily_atr) / close

        features = FeatureVector(
            atr_pct=atr_pct,
            volume_ratio=vol_ratio,
        )

        reason = (
            f"QuickFlip {direction}: {pattern} @ {entry:.2f} "
            f"(OR {or_low:.2f}..{or_high:.2f}, R:R {rr_ratio:.2f})"
        )

        metadata = {
            "entry_price": entry,
            "or_high": float(or_high),
            "or_low": float(or_low),
            "liquidity_direction": direction,
            "reversal_pattern": pattern,
            "daily_atr": float(daily_atr) if daily_atr else 0.0,
            "rr_ratio": float(rr_ratio),
            "vix_factor": float(vix_factor),
            "qty_factor": float(qty_factor),
            "qty_hint": final_qty,
            "reason": reason,
            "reserve_group": correlation_group(
                bar.symbol, cfg.get("mit_correlation_groups", {}),
            ),
        }

        return Signal(
            strategy=self.name,
            symbol=bar.symbol,
            features=features,
            direction=1 if direction == "long" else -1,
            strength=strength,
            stop_price=float(stop),
            target_price=float(target),
            timestamp=bar.timestamp,
            metadata=metadata,
        )

    # ── Helper: Gap-Check (analog ORB) ─────────────────────────────

    @staticmethod
    def _gap_check(df: pd.DataFrame,
                   max_gap_pct: float) -> tuple[bool, float]:
        idx_et = to_et(df.index)
        dates = idx_et.normalize()
        unique = pd.Index(dates).unique()
        if len(unique) < 2:
            return True, 0.0
        today = df[np.asarray(dates == unique[-1], dtype=bool)]
        prev = df[np.asarray(dates == unique[-2], dtype=bool)]
        if today.empty or prev.empty:
            return True, 0.0
        today_open = float(today["Open"].iloc[0])
        prev_close = float(prev["Close"].iloc[-1])
        ok = gap_filter(today_open, prev_close, max_gap_pct)
        gap_pct = abs(today_open - prev_close) / max(prev_close, 1e-9)
        return ok, gap_pct


# Strategie-Referenz:
# "The ONE CANDLE Scalping Strategy I Will Use For Life" – ProRealAlgos
# https://youtube.com/watch?v=XFtayhPIdEs
# Implementiert als regelbasierte Reversal-nach-Sweep-Variante der Opening Range.
# Kernfilter: Liquidity Candle (>= 25 % Daily ATR) + Reversal-Pattern (Hammer / Engulfing).
#
# Architektur-Hinweis – State Machine:
# Diese Strategie führt das explizite _day_cache["state"]-Pattern in FluxTrader ein.
# Bewusste Entscheidung: Bei 4+ sequenziellen Schritten ist implizites Cache-Key-Checking
# (wie in orb.py) fehleranfällig. Dieses Pattern dient als Vorlage für zukünftige
# sequenzielle Strategien im Framework.
