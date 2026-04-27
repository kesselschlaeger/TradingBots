"""Quick Flip Scalper – Reversal-nach-Sweep an der Opening Range.

One-Candle-Scalping-Spec: Die OR-Eröffnungskerze IST die Manipulationskerze.
  rote OR (or_close < or_open)  → Long-Setup  (Stop-Hunt unter or_low)
  grüne OR (or_close > or_open) → Short-Setup (Stop-Hunt über or_high)

Ablauf (State Machine):
  1. OR-Box der ersten opening_range_minutes ab 9:30 ET berechnen.
  2. OR-Range vs. Daily-ATR(14) validieren (Schritt 2 = Manipulations-Check).
  3. Direction aus or_color; Richtungs-/Gap-Filter anwenden → armed.
  4. Auf 5m-Bars nach OR-Close: Sweep + Reversal-Pattern → Signal.

Broker-agnostisch: kein Netzwerk, kein Broker-Import (CLAUDE.md Regel 1).
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from core.filters import (
    correlation_group,
    gap_filter,
    is_market_hours,
    is_within_trade_window,
    mit_independence_blocked,
    to_et,
    to_et_time,
    trend_filter_from_spy,
)
from core.indicators import (
    atr,
    detect_reversal_pattern,
    opening_range_levels,
)
from core.logging import get_logger
from core.models import Bar, FeatureVector, Signal
from core.risk import position_size
from strategy.base import BaseStrategy
from strategy.registry import register

log = get_logger(__name__)

# Mindest-Warmup in Kalendertagen: 14 Handelstage ATR + Feiertags-/DST-Puffer + heutiger Tag
QUICK_FLIP_REQUIRED_WARMUP_DAYS = 25


# ─────────────────────────── Default-Config ─────────────────────────────────

QUICK_FLIP_DEFAULT_PARAMS: dict = {
    "opening_range_minutes": 15,           # Länge der Opening-Range-Box in Minuten
    "liquidity_atr_threshold": 0.25,       # Min. OR-Range als Anteil der 14-Tage-ATR (Manipulations-Check)
    "max_trade_window_minutes": 90,        # Handelsfenster nach Open in Minuten (Spec: exakt 90)
    "min_rr_ratio": 1.5,                   # Mindest-Risk-Reward-Ratio; Trade wird übersprungen wenn darunter
    "buffer_ticks": 0.05,                  # STOP-Puffer in $ jenseits Pattern-Extremum
    "min_signal_strength": 0.35,           # Mindeststärke des Signals (0–1) für Ausgabe
    "allow_shorts": True,                  # Short-Trades erlauben (False = nur Long)
    "use_trend_filter": True,              # SPY-Trendfilter aktivieren (True empfohlen)
    "trend_ema_period": 20,                # EMA-Periode für SPY-Trendfilter
    "use_gap_filter": True,                # Gap-Filter bei Marktöffnung aktivieren
    "max_gap_pct": 0.03,                   # Maximaler Gap zur Vortagsclose in % (> 3 % = kein Trade)
    "use_mit_overlay": True,               # MIT-Independence-Overlay für Position Sizing aktivieren
    "use_vix_filter": True,                # VIX-Regime-Filter aktivieren
    "vix_high_threshold": 30.0,            # Ab diesem VIX-Wert Position Size reduzieren
    "vix_size_factor": 0.5,                # Größen-Multiplikator bei hohem VIX (0.5 = halbe Größe)
    "use_extended_target": False,          # Erweitertes Ziel (gegenüberliegende OR-Seite) nutzen
    "extended_target_min_rr": 2.5,         # Mindest-R:R für Aktivierung des erweiterten Ziels
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
    """Quick Flip Scalper – Reversal nach Sweep an der Opening Range.

    State Machine (pro Symbol via `_day_caches[symbol]["state"]`):
      idle        → OR-Periode läuft noch / ATR-Warmup.
      or_complete → OR-Box steht. ATR-Validierung + Richtungs-/Gap-Filter.
      armed       → Alle Filter ok. Warte auf Sweep + Reversal-Candle.
      done        → Signal emittiert ODER Fenster abgelaufen (terminal).

    Jedes Symbol hält seinen eigenen Tag-Cache in `_day_caches[symbol]`.
    Backtest (N Symbole, chronologisch interleaved) und LiveRunner (N Symbole
    gleichzeitig) teilen damit keinen State mehr.

    Die OR-Eröffnungskerze ist gleichzeitig die Manipulationskerze (Schritt 2).
    Es gibt keine separate Liquidity-Candle-Erkennung mehr.
    """

    def __init__(self, config: dict, context=None):
        merged = dict(QUICK_FLIP_DEFAULT_PARAMS)
        merged.update(config or {})
        super().__init__(merged, context=context)
        self._day_caches: dict[str, dict] = {}   # Key: symbol → Tages-State (per Symbol)

    @property
    def name(self) -> str:
        return "quick_flip"

    def required_warmup_days(self) -> int:
        return QUICK_FLIP_REQUIRED_WARMUP_DAYS

    def _is_ready(self) -> bool:
        min_bars = int(self.config.get("min_bars", 20))
        if self.bars:
            cursor = self.context.bar_cursor(self.bars[-1].symbol)
            if cursor >= 0:
                return cursor + 1 >= min_bars
        return len(self.bars) >= min_bars

    def reset(self) -> None:
        super().reset()
        self._day_caches.clear()

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
                self._record_status(symbol, "NO_DATA", f"warmup cursor={cursor}")
                return []
            df_5m = full_df.iloc[:cursor + 1]
        else:
            sym_bars = [b for b in self.bars if b.symbol == symbol]
            if not sym_bars:
                sym_bars = self.context.bars(symbol)
            df_5m = _bars_to_df(sym_bars)
            if df_5m.empty or len(df_5m) < 6:
                self._record_status(symbol, "NO_DATA", f"bars={len(df_5m)}")
                return []

        # Market-Hours Gate
        if not is_market_hours(current_time):
            self._record_status(symbol, "OUTSIDE_HOURS", "außerhalb Handelszeiten")
            return []

        # Per-Symbol-Cache holen/anlegen
        cache = self._day_caches.setdefault(symbol, self._fresh_day_cache())

        # Day-Reset (nur für dieses Symbol)
        day_et = to_et(current_time)
        today = day_et.date()
        if cache.get("date") != today:
            cache = self._fresh_day_cache()
            cache["date"] = today
            cache["open_time_et"] = self._market_open_for(day_et)
            self._day_caches[symbol] = cache

        # ── Time-Window-Check (ERSTE Prüfung im State-Machine-Block) ─
        if self._check_time_window_expired(current_time, cache):
            cache["state"] = "done"
            self._record_status(symbol, "WINDOW_EXPIRED", "Handelsfenster abgelaufen")
            return []

        state = cache.get("state", "idle")
        if state == "done":
            return []

        # OR-Zeitgrenzen für diesen Tag
        et_time = to_et_time(current_time)
        orb_minutes = int(cfg.get("opening_range_minutes", 15))
        or_end_minutes = 9 * 60 + 30 + orb_minutes
        cur_minutes = et_time.hour * 60 + et_time.minute

        # ── State: idle ──────────────────────────────────────────────
        if state == "idle":
            # ATR schon während OR-Periode vorausberechnen
            if cache.get("daily_atr") is None:
                daily_atr = self._compute_daily_atr(df_5m)
                if daily_atr is not None:
                    cache["daily_atr"] = daily_atr

            if cur_minutes < or_end_minutes:
                self._record_status(symbol, "WAIT_OR", f"OR-Periode, {orb_minutes}m")
                return []

            # OR-Periode abgeschlossen: Box berechnen
            if cache.get("daily_atr") is None:
                self._record_status(symbol, "NO_DAILY_ATR", "Daily ATR nicht berechenbar")
                return []

            self._lock_opening_range(df_5m, cache)
            if cache.get("or_high") is None:
                self._record_status(symbol, "NO_OR", "OR-Box nicht berechenbar")
                return []

            cache["state"] = "or_complete"
            state = "or_complete"

        # ── State: or_complete → ATR-Validierung + Filter ────────────
        if state == "or_complete":
            or_range = cache["or_range"]
            daily_atr = cache["daily_atr"]
            threshold = float(cfg.get("liquidity_atr_threshold", 0.25))

            if or_range < threshold * daily_atr:
                cache["state"] = "done"
                log.debug("quick_flip.or_too_small",
                          symbol=symbol, or_range=or_range,
                          threshold=threshold * daily_atr)
                self._record_status(symbol, "OR_TOO_SMALL",
                                    f"or_range={or_range:.3f} < {threshold*daily_atr:.3f}")
                return []

            or_color = cache.get("or_color", "red")
            direction = "long" if or_color == "red" else "short"

            if direction == "short" and not cfg.get("allow_shorts", True):
                cache["state"] = "done"
                self._record_status(symbol, "SHORTS_DISABLED", "grüne OR, Shorts deaktiviert")
                return []

            cache["direction"] = direction

            # Gap-Filter (einmalig)
            if cfg.get("use_gap_filter", True) and not cache.get("gap_checked"):
                gap_ok, gap_pct = self._gap_check(
                    df_5m, float(cfg.get("max_gap_pct", 0.03)),
                )
                cache["gap_checked"] = True
                if not gap_ok:
                    cache["state"] = "done"
                    log.debug("quick_flip.gap_block", symbol=symbol, gap_pct=gap_pct)
                    self._record_status(symbol, "GAP_BLOCK",
                                        f"gap {gap_pct*100:.2f}%")
                    return []

            cache["state"] = "armed"
            state = "armed"

        # ── State: armed → Sweep + Reversal + Signal ─────────────────
        if state == "armed":
            # Nur Bars nach OR-Schluss betrachten
            if cur_minutes < or_end_minutes:
                self._record_status(symbol, "WAIT_REVERSAL", "warte auf Post-OR-Bar")
                return []

            direction = cache["direction"]
            signal = self._detect_reversal_and_build_signal(df_5m, direction, bar, cache)
            if signal is None:
                self._record_status(symbol, "WAIT_REVERSAL",
                                    f"warte auf Reversal ({direction})")
                return []
            cache["state"] = "done"
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
            "or_open": None,
            "or_close": None,
            "or_color": None,
            "direction": None,
            "daily_atr": None,
            "gap_checked": False,
        }

    @staticmethod
    def _market_open_for(dt_et) -> datetime:
        """Market-Open (9:30 ET) für den Handelstag von dt_et."""
        et_dt = to_et(dt_et)
        return et_dt.replace(hour=9, minute=30, second=0, microsecond=0)

    def _check_time_window_expired(self, now: datetime, cache: dict) -> bool:
        cfg = self.config
        window = int(cfg.get("max_trade_window_minutes", 90))
        open_time = cache.get("open_time_et")
        if open_time is None:
            open_time = self._market_open_for(now)
            cache["open_time_et"] = open_time
        return not is_within_trade_window(now, open_time, window)

    # ── Helper: Indikator-Berechnung ──────────────────────────────

    def _compute_daily_atr(self, df_5m: pd.DataFrame) -> Optional[float]:
        """Daily ATR(14) aus 5m-Bars per ET-Tages-Gruppierung.

        groupby(normalize()) statt resample('1D') vermeidet Phantom-Zeilen
        an Wochenenden und DST-Übergängen, da nur tatsächlich vorhandene
        ET-Handelstage aggregiert werden.
        """
        if df_5m.empty:
            return None
        idx_et = to_et(df_5m.index)
        df_et = df_5m.copy()
        df_et.index = idx_et
        daily = df_et.groupby(idx_et.normalize()).agg({
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        })
        today_norm = idx_et[-1].normalize()
        completed = daily[daily.index < today_norm]
        if len(completed) < 14:
            return None
        series = atr(completed, 14).dropna()
        if series.empty:
            return None
        return float(series.iloc[-1])

    def _lock_opening_range(self, df_5m: pd.DataFrame, cache: dict) -> None:
        """Berechnet or_high/or_low/or_open/or_close/or_color aus OR-Bars."""
        cfg = self.config
        orb_minutes = int(cfg.get("opening_range_minutes", 15))
        today = cache["date"]
        df = df_5m.copy()
        df.index = to_et(df.index)
        day_mask = np.asarray([d.date() == today for d in df.index], dtype=bool)
        day_df = df[day_mask]
        if day_df.empty:
            return

        hi, lo, rng = opening_range_levels(day_df, orb_minutes)
        if rng <= 0:
            return

        # OR-Bars für or_open / or_close isolieren
        orb_start = 9 * 60 + 30
        orb_end = orb_start + orb_minutes
        hhmm = np.array([d.hour * 60 + d.minute for d in day_df.index])
        or_mask = (hhmm >= orb_start) & (hhmm < orb_end)
        or_bars = day_df[or_mask]

        cache["or_high"] = hi
        cache["or_low"] = lo
        cache["or_range"] = rng

        if len(or_bars) >= 1:
            or_open = float(or_bars["Open"].iloc[0])
            or_close = float(or_bars["Close"].iloc[-1])
            cache["or_open"] = or_open
            cache["or_close"] = or_close
            cache["or_color"] = "green" if or_close > or_open else "red"

    def _detect_reversal_and_build_signal(
        self,
        df_5m: pd.DataFrame,
        direction: str,
        bar: Bar,
        cache: dict,
    ) -> Optional[Signal]:
        """Prüft aktuellen 5m-Bar auf Sweep + Reversal-Pattern. Gibt Signal oder None zurück.

        Long: Sweep  = bar.low < or_low;  Patterns: hammer, bullish_engulfing.
        Short: Sweep = bar.high > or_high; Patterns: inverted_hammer, bearish_engulfing.

        Entry/Stop aus Pattern-Geometrie (STOP-Order, kein Market-Close):
          hammer:            entry = high + buf,      stop = low  - buf
          bullish_engulfing: entry = prev.high + buf, stop = low  - buf
          inverted_hammer:   entry = low  - buf,      stop = high + buf
          bearish_engulfing: entry = prev.low  - buf, stop = high + buf
        """
        cfg = self.config
        or_high = cache.get("or_high")
        or_low = cache.get("or_low")
        or_range = cache.get("or_range", 0.0)
        daily_atr = cache.get("daily_atr")
        if or_high is None or or_low is None:
            return None

        # Sweep-Bedingung
        if direction == "long" and float(bar.low) >= float(or_low):
            return None
        if direction == "short" and float(bar.high) <= float(or_high):
            return None

        pattern = detect_reversal_pattern(
            df_5m,
            direction=direction,
            hammer_shadow_ratio=float(cfg.get("hammer_shadow_ratio", 2.0)),
            hammer_upper_shadow_ratio=float(cfg.get("hammer_upper_shadow_ratio", 0.3)),
            engulfing_min_body_ratio=float(cfg.get("engulfing_min_body_ratio", 0.6)),
        )
        if pattern is None:
            return None

        # Trend-Filter (optional)
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
                self._record_status(bar.symbol, "MIT_BLOCKED", reason)
                return None

        # Entry / Stop pattern-spezifisch (STOP-Order-Preis, kein Market-Close)
        buffer_ticks = float(cfg.get("buffer_ticks", 0.05))
        last_high = float(bar.high)
        last_low = float(bar.low)

        if direction == "long":
            if pattern == "hammer":
                entry = last_high + buffer_ticks
                stop = last_low - buffer_ticks
            else:  # bullish_engulfing: Entry am Hoch der roten Vorgängerkerze
                prev_high = float(df_5m.iloc[-2]["High"]) if len(df_5m) >= 2 else last_high
                entry = prev_high + buffer_ticks
                stop = last_low - buffer_ticks
            target = float(or_high)
            if stop >= entry or target <= entry:
                return None
        else:  # short
            if pattern == "inverted_hammer":
                entry = last_low - buffer_ticks
                stop = last_high + buffer_ticks
            else:  # bearish_engulfing: Entry am Tief der grünen Vorgängerkerze
                prev_low = float(df_5m.iloc[-2]["Low"]) if len(df_5m) >= 2 else last_low
                entry = prev_low - buffer_ticks
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
            self._record_status(bar.symbol, "RR_REJECT",
                                f"rr={rr_ratio:.2f} < {min_rr}")
            return None

        # Erweitertes Ziel (gegenüberliegende OR-Seite), wenn R:R ausreicht
        if cfg.get("use_extended_target", False):
            ext_min_rr = float(cfg.get("extended_target_min_rr", 2.5))
            if direction == "long":
                ext_target = float(or_high) + float(or_range)
                ext_reward = abs(ext_target - entry)
                if ext_reward / risk >= ext_min_rr:
                    target = ext_target
            else:
                ext_target = float(or_low) - float(or_range)
                ext_reward = abs(ext_target - entry)
                if ext_reward / risk >= ext_min_rr:
                    target = ext_target
            # R:R nach ggf. erweitertem Ziel neu berechnen
            reward = abs(target - entry)
            rr_ratio = reward / risk

        # Signal-Stärke aus R:R + Pattern-Qualität
        strength = float(np.clip(
            0.35 + 0.10 * (rr_ratio - min_rr)
            + (0.10 if pattern in ("bullish_engulfing", "bearish_engulfing") else 0.0),
            0.0, 1.0,
        ))
        min_strength = float(cfg.get("min_signal_strength", 0.35))
        if strength < min_strength:
            return None

        # Sizing: Basisgröße → VIX-Skalierung
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

        final_qty = int(max(0, round(base_qty * vix_factor)))

        # FeatureVector
        vol_ratio = 1.0
        if len(df_5m) >= 20 and "Volume" in df_5m.columns:
            recent_vol = float(df_5m["Volume"].tail(20).mean())
            if recent_vol > 0:
                vol_ratio = float(bar.volume) / recent_vol
        atr_pct = 0.0
        close = float(bar.close)
        if daily_atr and close > 0:
            atr_pct = float(daily_atr) / close

        features = FeatureVector(atr_pct=atr_pct, volume_ratio=vol_ratio)

        reason = (
            f"QuickFlip {direction}: {pattern} @ entry={entry:.2f} "
            f"(OR {float(or_low):.2f}..{float(or_high):.2f}, R:R {rr_ratio:.2f})"
        )

        metadata = {
            "entry_price": entry,
            "or_high": float(or_high),
            "or_low": float(or_low),
            "or_color": cache.get("or_color", ""),
            "reversal_pattern": pattern,
            "daily_atr": float(daily_atr) if daily_atr else 0.0,
            "rr_ratio": float(rr_ratio),
            "vix_factor": float(vix_factor),
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

    # ── Helper: Gap-Check ──────────────────────────────────────────

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
# Die OR-Eröffnungskerze IST die Manipulationskerze (kein separater Liquidity-Schritt).
# Kernfilter: OR-Range >= 25 % Daily ATR + Reversal-Pattern (Hammer / Engulfing).
#
# Architektur-Hinweis – State Machine:
# _day_caches[symbol]["state"] ist die einzige Quelle der Wahrheit für den Symbol-Tages-State.
# Jedes Symbol hat seinen eigenen Cache (dict[str, dict]). Backtest und LiveRunner mit
# N Symbolen teilen damit keinen State. Dieses Pattern ist Vorlage für sequenzielle Strategien.
