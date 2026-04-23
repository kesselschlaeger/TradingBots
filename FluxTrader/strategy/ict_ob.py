"""ICT Order Block MTF Confluence Strategy (ict_ob_mtf).

Multi-Timeframe Order Block strategy implementing ICT/SMC concepts:
  - 4H  → höchster Bias + Order-Block-Zonen
  - 1H  → Struktur-Bestätigung (BOS / CHOCH / Higher Low)
  - 15M → Fair Value Gap / Imbalance innerhalb des OB
  - 5M  → präziser Entry (Reaktionskerze oder Close > 50 % OB)

Broker-agnostisch: kein I/O, kein HTTP, kein SDK.
Identisch in Backtest und Live.

Unterstützt drei Asset-Klassen: equity | futures | crypto. Asset-spezifische
Session-Fenster, Trend-Referenz, Gap-Filter und Position-Sizing werden über
private Hooks (``_is_trading_session`` etc.) abgewickelt – die Kern-Logik
in ``_generate_signals`` bleibt strukturell identisch.
"""
from __future__ import annotations

from datetime import time
from typing import Optional

import numpy as np
import pandas as pd

from core.filters import (
    correlation_group,
    entry_cutoff_ok,
    gap_filter,
    is_market_hours,
    mit_independence_blocked,
    to_et,
    to_et_time,
    trend_filter_from_spy,
    vix_size_factor,
)
from core.indicators import (
    atr,
    detect_order_blocks,
    detect_structure_break,
    fair_value_gaps,
    resample_ohlcv,
)
from core.logging import get_logger
from core.models import Bar, FeatureVector, Signal
from core.risk import (
    order_block_stop,
    position_size,
    target_from_r,
)
from strategy.base import BaseStrategy
from strategy.registry import register

log = get_logger(__name__)


# ─────────────────────────── Default Config ─────────────────────────────────

# Fallback-Tabelle für Futures-Point-Values (USD pro Punkt). Wird nur
# genutzt wenn cfg["futures_point_value"] nicht gesetzt ist. Alle CME
# E-Mini / Micro-E-Mini sind unterstützt, können via Config überschrieben
# werden.
FUTURES_POINT_VALUES: dict = {
    "NQ": 20.0,    # E-mini Nasdaq-100 ($20 / Punkt)
    "ES": 50.0,    # E-mini S&P 500 ($50 / Punkt)
    "YM": 5.0,     # E-mini Dow ($5 / Punkt)
    "RTY": 50.0,   # E-mini Russell 2000 ($50 / Punkt)
    "MNQ": 2.0,    # Micro E-mini Nasdaq ($2 / Punkt)
    "MES": 5.0,    # Micro E-mini S&P ($5 / Punkt)
}


ICT_OB_DEFAULT_PARAMS: dict = {
    "min_bars": 250,                          # Mindestzahl 5m-Bars für Signalerzeugung
    "max_bars_buffer": 2000,                  # Rolling-Buffer in BaseStrategy

    # OB detection
    "atr_period": 14,                         # ATR-Fenster in Bars (MTF)
    "displacement_mult": 1.8,                 # Displacement = body > mult × ATR
    "swing_lookback_4h": 3,                   # Swing-Fenster für 4H-OB Detection
    "swing_lookback_1h": 5,                   # Swing-Fenster für 1H-Struktur

    # Entry
    "ob_entry_mode": "standard",              # aggressive | standard | conservative
    "min_confluence_score": 0.75,             # Mindest-Confluence (0..1) für Entry
    "min_signal_strength": 0.75,              # Signal-Strength-Threshold

    # Risk
    "stop_ob_mult": 0.75,                     # SL = mult × OB-range außerhalb der Zone
    "profit_target_r": 2.0,                   # R-Multiple für TP (1:2)
    "risk_per_trade": 0.005,                  # 0.5 % Risiko je Trade

    # Filters
    "use_trend_filter": True,                 # EMA-Referenz-Trend aktivieren
    "trend_ema_period": 20,                   # EMA-Periode für Trend-Filter
    "use_gap_filter": True,                   # Overnight-Gap-Block (nur equity relevant)
    "max_gap_pct": 0.03,                      # max. Overnight-Gap (3 %)
    "vix_threshold": 30.0,                    # VIX-Grenze für Positionsgrößen-Halbierung

    "allow_shorts": True,                     # Short-Seite aktivieren
    "market_open": time(9, 30),               # Equity Session-Start (ET)
    "market_close": time(16, 0),              # Equity Session-Ende (ET)
    "entry_cutoff_time": time(15, 0),         # Equity: kein neuer Entry nach 15:00 ET

    # ── Asset-Class Support (equity | futures | crypto) ───────────────
    "asset_class": "equity",                  # Routing-Schalter für Session/Sizing/Contract

    # Futures-spezifisch (CME Globex)
    "futures_exchange": "CME",                # IBKR-Exchange für Future-Contract
    "futures_point_value": 20.0,              # USD je Punkt (NQ=20, ES=50); überschreibt FUTURES_POINT_VALUES
    "futures_session_open": "18:00",          # ET, Abend-Open (Start Globex-Woche)
    "futures_session_close": "17:00",         # ET, Fr-Close (Ende Globex-Woche)
    "futures_entry_cutoff": "15:45",          # ET, letzter Entry vor US-Close; None = kein Cutoff

    # Crypto-spezifisch (24/7 Spot)
    "crypto_quote_currency": "USD",           # Notierungs-Währung für Crypto-Contract
    "crypto_session_open": None,              # None = 24/7 handelbar
    "crypto_entry_cutoff": None,              # None = kein Cutoff

    # Trend-Filter Referenz-Asset (überschreibt Auto-Routing je asset_class)
    "trend_reference_asset": None,            # None = auto: SPY (equity) / ES (futures) / None (crypto)

    # MIT Independence
    "use_mit_independence_guard": False,      # Korrelations-Gruppen-Lock aktivieren
    "mit_correlation_groups": {},             # Symbol-Gruppen-Mapping
}


# ─────────────────────────── Helpers ────────────────────────────────────────

def _coerce_time(val) -> Optional[time]:
    """Normalisiert 'HH:MM'-Strings / time-Objekte auf ``time`` | None."""
    if val is None:
        return None
    if isinstance(val, time):
        return val
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        parts = s.split(":")
        try:
            hh = int(parts[0])
            mm = int(parts[1]) if len(parts) > 1 else 0
            return time(hh, mm)
        except (ValueError, IndexError):
            return None
    return None


def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
    """Convert Bar list to OHLCV DataFrame with UTC index."""
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


def _is_bullish_reaction(bar: Bar, prev_bar: Optional[Bar],
                         ob_mid: float) -> bool:
    """5M bullish entry confirmation: engulfing, hammer, or close > OB 50 %."""
    if bar.close > ob_mid:
        return True
    if prev_bar is None:
        return False
    # Bullish engulfing
    if (bar.close > bar.open
            and prev_bar.close < prev_bar.open
            and bar.close > prev_bar.open
            and bar.open < prev_bar.close):
        return True
    # Hammer / pinbar
    body = abs(bar.close - bar.open)
    lower_wick = min(bar.open, bar.close) - bar.low
    if body > 0 and lower_wick > 2 * body and bar.close > bar.open:
        return True
    return False


def _is_bearish_reaction(bar: Bar, prev_bar: Optional[Bar],
                         ob_mid: float) -> bool:
    """5M bearish entry confirmation: engulfing, shooting star, or close < 50 %."""
    if bar.close < ob_mid:
        return True
    if prev_bar is None:
        return False
    # Bearish engulfing
    if (bar.close < bar.open
            and prev_bar.close > prev_bar.open
            and bar.close < prev_bar.open
            and bar.open > prev_bar.close):
        return True
    # Shooting star
    body = abs(bar.close - bar.open)
    upper_wick = bar.high - max(bar.open, bar.close)
    if body > 0 and upper_wick > 2 * body and bar.close < bar.open:
        return True
    return False


# ─────────────────────────── Strategy ───────────────────────────────────────

@register("ict_ob_mtf")
class IctOrderBlockStrategy(BaseStrategy):
    """ICT Order Block + Multi-Timeframe Confluence.

    Signal-Logik:
      1. 4H: valider OB (Sweep + Displacement + FVG), Preis in der Zone.
      2. 1H: Struktur bullish/bearish bestätigt (BOS/CHOCH/HL/LH).
      3. 15M: FVG innerhalb / am OB.
      4. 5M: Reaktionskerze oder Close > 50 % des OB.
      Confluence ≥ 0.75 (min. 3 von 4 aligned).
    """

    def __init__(self, config: dict, context=None):
        merged = dict(ICT_OB_DEFAULT_PARAMS)
        merged.update(config or {})
        super().__init__(merged, context=context)
        self._trend_cache: dict[object, dict] = {}
        self._gap_cache: dict[tuple[str, object], tuple[bool, float]] = {}

    @property
    def name(self) -> str:
        return "ict_ob_mtf"

    def _is_ready(self) -> bool:
        return len(self.bars) >= int(self.config.get("min_bars", 250))

    def reset(self) -> None:
        super().reset()
        self._trend_cache.clear()
        self._gap_cache.clear()

    # ── Core Signal Logic ──────────────────────────────────────────────

    def _generate_signals(self, bar: Bar) -> list[Signal]:  # noqa: C901
        cfg = self.config
        symbol = bar.symbol
        ts = bar.timestamp
        asset_class = str(cfg.get("asset_class", "equity")).lower()

        if not self._is_trading_session(ts):
            self._record_status(symbol, "OUTSIDE_HOURS",
                                "außerhalb Handelszeiten")
            return []

        if not self._entry_cutoff_ok(ts):
            self._record_status(symbol, "ENTRY_CUTOFF",
                                "Entry-Cutoff überschritten")
            return []

        # ── Build MTF DataFrames ──────────────────────────────────────
        bars_list = [b for b in self.bars if b.symbol == symbol]
        if len(bars_list) < int(cfg.get("min_bars", 250)):
            bars_list = self.context.bars(symbol)
            if len(bars_list) < 50:
                self._record_status(symbol, "NO_DATA",
                                    f"nur {len(bars_list)} Bars verfügbar")
                return []

        df_5m = _bars_to_df(bars_list)
        if len(df_5m) < 50:
            self._record_status(symbol, "NO_DATA",
                                f"nur {len(df_5m)} 5m-Bars")
            return []

        df_15m = resample_ohlcv(df_5m, "15M")
        df_1h = resample_ohlcv(df_5m, "1H")
        df_4h = resample_ohlcv(df_5m, "4H")

        if len(df_4h) < 8 or len(df_1h) < 15 or len(df_15m) < 20:
            self._record_status(
                symbol, "NO_DATA",
                f"MTF zu kurz (4H={len(df_4h)}, 1H={len(df_1h)}, 15M={len(df_15m)})",
            )
            return []

        current_price = bar.close

        # ── Day-key for caches ────────────────────────────────────────
        day_et = to_et(ts)
        day_key = day_et.date() if hasattr(day_et, "date") else day_et

        # ── Gap Filter (equity-only, futures/crypto: No-Op) ───────────
        if cfg.get("use_gap_filter", True):
            gap_ck = (symbol, day_key)
            cached = self._gap_cache.get(gap_ck)
            if cached is not None:
                gap_ok, gap_pct = cached
            else:
                gap_ok, gap_pct = self._gap_check_for_asset(df_5m)
                self._gap_cache[gap_ck] = (gap_ok, gap_pct)
            if not gap_ok:
                log.debug("ict_ob.gap_block", symbol=symbol, gap_pct=gap_pct)
                self._record_status(symbol, "GAP_BLOCK",
                                    f"gap {gap_pct:.2%}")
                return []

        # ── Trend Filter (asset-class aware) ──────────────────────────
        trend = {"bullish": True, "bearish": True}
        if cfg.get("use_trend_filter", True):
            ct = self._trend_cache.get(day_key)
            if ct is not None:
                trend = ct
            else:
                trend = self._resolve_trend(day_key)
                self._trend_cache[day_key] = trend

        # ── VIX overlay ───────────────────────────────────────────────
        vix_spot, _ = self.context.vix
        vix_factor = 1.0
        if vix_spot is not None:
            vix_factor = vix_size_factor(
                vix_spot, float(cfg.get("vix_threshold", 30.0)))

        # ── Step 1: Detect 4H Order Blocks ────────────────────────────
        obs_4h = detect_order_blocks(
            df_4h,
            atr_period=int(cfg.get("atr_period", 14)),
            displacement_mult=float(cfg.get("displacement_mult", 1.8)),
            swing_lookback=int(cfg.get("swing_lookback_4h", 3)),
        )
        if not obs_4h:
            self._record_status(symbol, "NO_OB", "keine 4H Order Blocks")
            return []

        valid_obs = self._filter_valid_obs(obs_4h, df_4h, current_price)
        if not valid_obs:
            self._record_status(
                symbol, "NO_VALID_OB",
                f"{len(obs_4h)} OB(s), keine in Preisnähe / alle invalidiert",
            )
            return []

        active_ob = valid_obs[-1]
        ob_type = active_ob["type"]

        # Direction must align with trend
        ref_asset = self._trend_reference_asset_key()
        if ob_type == "bullish" and not trend["bullish"]:
            log.debug("ict_ob.trend_block", symbol=symbol,
                      asset_class=asset_class, ref_asset=ref_asset)
            self._record_status(
                symbol, "TREND_BLOCK",
                f"4H bullish OB, aber {ref_asset or 'ref'}-Trend nicht bullish",
            )
            return []
        if ob_type == "bearish" and not trend["bearish"]:
            log.debug("ict_ob.trend_block", symbol=symbol,
                      asset_class=asset_class, ref_asset=ref_asset)
            self._record_status(
                symbol, "TREND_BLOCK",
                f"4H bearish OB, aber {ref_asset or 'ref'}-Trend nicht bearish",
            )
            return []
        if ob_type == "bearish" and not cfg.get("allow_shorts", True):
            self._record_status(symbol, "SHORTS_DISABLED",
                                "bearish OB, aber allow_shorts=False")
            return []

        # ── Step 2: 1H Structure Confirmation ─────────────────────────
        structure = detect_structure_break(
            df_1h, lookback=int(cfg.get("swing_lookback_1h", 5)),
        )
        structure_aligned = self._structure_aligned(structure, ob_type)

        # ── Step 3: 15M FVG near OB ──────────────────────────────────
        fvgs_15m = fair_value_gaps(df_15m)
        fvg_at_ob = self._fvg_overlaps_ob(fvgs_15m, active_ob, ob_type)

        # ── Step 4: 5M Entry Confirmation ─────────────────────────────
        prev_bar = bars_list[-2] if len(bars_list) >= 2 else None
        if ob_type == "bullish":
            entry_confirmed = _is_bullish_reaction(
                bar, prev_bar, active_ob["mid"])
        else:
            entry_confirmed = _is_bearish_reaction(
                bar, prev_bar, active_ob["mid"])

        # ── Confluence Score ──────────────────────────────────────────
        score = 0.25                        # 4H OB proximity (baseline)
        if structure_aligned:
            score += 0.25
        if fvg_at_ob:
            score += 0.25
        if entry_confirmed:
            score += 0.25

        min_score = float(cfg.get("min_confluence_score", 0.75))
        if score < min_score:
            self._record_status(
                symbol, "WEAK_CONFLUENCE",
                f"Confluence {score:.2f} < {min_score:.2f}",
            )
            return []

        # ── MIT Independence Guard ────────────────────────────────────
        if cfg.get("use_mit_independence_guard", False):
            groups = cfg.get("mit_correlation_groups", {})
            blocked, reason = mit_independence_blocked(
                symbol,
                self.context.open_symbols,
                self.context.reserved_groups,
                groups,
            )
            if blocked:
                log.debug("ict_ob.mit_blocked", symbol=symbol, reason=reason)
                self._record_status(symbol, "MIT_BLOCK", reason)
                return []

        # ── Stop / Target ─────────────────────────────────────────────
        ob_range = active_ob["high"] - active_ob["low"]
        if ob_range <= 0:
            self._record_status(symbol, "NO_VALID_OB",
                                "OB-Range ≤ 0")
            return []

        side = "long" if ob_type == "bullish" else "short"
        direction = 1 if side == "long" else -1

        stop_price = order_block_stop(
            side,
            active_ob["high"],
            active_ob["low"],
            float(cfg.get("stop_ob_mult", 0.75)),
        )
        entry_price = self._entry_price(
            bar, active_ob, ob_type,
            str(cfg.get("ob_entry_mode", "standard")),
        )
        target_price = target_from_r(
            side, entry_price, stop_price,
            float(cfg.get("profit_target_r", 2.0)),
        )

        # ── Position Sizing (asset-class aware) ───────────────────────
        equity = float(self.context.account.equity or 0.0)
        risk_pct = float(cfg.get("risk_per_trade", 0.005))
        contract_qty = self._effective_risk_qty(
            equity, risk_pct, float(entry_price), float(stop_price),
        )
        qty_factor = vix_factor
        strength = float(np.clip(score, 0.0, 1.0))

        # ── FeatureVector ─────────────────────────────────────────────
        atr_5m = atr(df_5m, int(cfg.get("atr_period", 14)))
        last_atr = (float(atr_5m.iloc[-1])
                    if not atr_5m.empty and not pd.isna(atr_5m.iloc[-1])
                    else 0.0)
        vol_ma = df_5m["Volume"].rolling(20).mean()
        last_vol_ma = (float(vol_ma.iloc[-1])
                       if len(vol_ma) >= 20 and not pd.isna(vol_ma.iloc[-1])
                       else 0.0)
        vol_ratio = (float(df_5m["Volume"].iloc[-1]) / last_vol_ma
                     if last_vol_ma > 0 else 1.0)

        features = FeatureVector(
            atr_pct=last_atr / current_price if current_price > 0 else 0.0,
            volume_ratio=vol_ratio,
        )

        # ── Reason string ─────────────────────────────────────────────
        reason = (
            f"ICT OB {ob_type.title()}: {current_price:.2f} at "
            f"OB [{active_ob['low']:.2f}–{active_ob['high']:.2f}] "
            f"confluence={score:.2f}"
        )
        if structure_aligned:
            reason += f" +1H_{structure['type']}"
        if fvg_at_ob:
            reason += " +15M_FVG"
        if entry_confirmed:
            reason += " +5M_entry"

        signal = Signal(
            strategy=self.name,
            symbol=symbol,
            features=features,
            direction=direction,
            strength=strength,
            stop_price=float(stop_price),
            target_price=float(target_price),
            timestamp=ts,
            metadata={
                "entry_price": float(entry_price),
                "ob_type": ob_type,
                "ob_high": active_ob["high"],
                "ob_low": active_ob["low"],
                "ob_mid": active_ob["mid"],
                "confluence_score": score,
                "structure": structure["type"],
                "fvg_at_ob": fvg_at_ob,
                "entry_confirmed": entry_confirmed,
                "qty_factor": qty_factor,
                "contract_qty": float(contract_qty),
                "asset_class": asset_class,
                "futures_exchange": cfg.get("futures_exchange", "CME"),
                "futures_point_value": float(cfg.get(
                    "futures_point_value",
                    FUTURES_POINT_VALUES.get(symbol.upper(), 0.0),
                )),
                "crypto_quote_currency": cfg.get("crypto_quote_currency", "USD"),
                "ibkr_crypto_symbol": cfg.get("ibkr_crypto_symbol"),
                "reason": reason,
                "risk_per_trade": float(cfg.get("risk_per_trade", 0.005)),
            },
        )

        # Für Futures bestimmt der Kontrakt-Count die Ordermenge direkt;
        # Share-basiertes position_size() greift nicht. Für equity/crypto
        # bleibt der bestehende R-basierte Flow in execute_signal intakt.
        if asset_class == "futures":
            signal.metadata["qty_hint"] = max(1, int(round(contract_qty)))

        if cfg.get("use_mit_independence_guard", False):
            signal.metadata["reserve_group"] = correlation_group(
                symbol, cfg.get("mit_correlation_groups", {}),
            )

        log.info("ict_ob.signal", symbol=symbol, direction=side,
                 strength=strength, confluence=score,
                 asset_class=asset_class, contract_qty=contract_qty)
        self._record_status(symbol, "SIGNAL", reason)
        return [signal]

    # ── Helper Methods ─────────────────────────────────────────────────

    @staticmethod
    def _filter_valid_obs(
        obs: list[dict],
        df: pd.DataFrame,
        current_price: float,
    ) -> list[dict]:
        """Keep only non-invalidated OBs where price is in/near the zone."""
        closes = df["Close"].values
        valid: list[dict] = []
        for ob in obs:
            idx = ob["idx"]
            ob_high, ob_low = ob["high"], ob["low"]
            ob_range = ob_high - ob_low
            if ob_range <= 0:
                continue

            # Invalidation: close through OB after formation
            invalidated = False
            for j in range(idx + 1, len(closes)):
                if ob["type"] == "bullish" and closes[j] < ob_low:
                    invalidated = True
                    break
                if ob["type"] == "bearish" and closes[j] > ob_high:
                    invalidated = True
                    break
            if invalidated:
                continue

            # Proximity: price within OB ± 50 % of range
            buf = ob_range * 0.5
            if ob_low - buf <= current_price <= ob_high + buf:
                valid.append(ob)
        return valid

    @staticmethod
    def _structure_aligned(structure: dict, ob_type: str) -> bool:
        stype = structure.get("type", "none")
        if ob_type == "bullish":
            return stype in ("bos_bullish", "choch_bullish", "higher_low")
        return stype in ("bos_bearish", "choch_bearish", "lower_high")

    @staticmethod
    def _fvg_overlaps_ob(
        fvgs: list[dict], ob: dict, ob_type: str,
    ) -> bool:
        """True if any FVG of matching direction overlaps the OB zone."""
        ob_high, ob_low = ob["high"], ob["low"]
        tolerance = (ob_high - ob_low) * 1.0
        for fvg in fvgs:
            if fvg["type"] != ob_type:
                continue
            if (fvg["high"] >= ob_low - tolerance
                    and fvg["low"] <= ob_high + tolerance):
                return True
        return False

    @staticmethod
    def _entry_price(
        bar: Bar, ob: dict, ob_type: str, mode: str,
    ) -> float:
        if mode == "aggressive":
            return ob["low"] if ob_type == "bullish" else ob["high"]
        if mode == "conservative":
            return bar.close
        # standard → 50 % level
        return ob["mid"]

    @staticmethod
    def _gap_check(
        df: pd.DataFrame, max_gap_pct: float,
    ) -> tuple[bool, float]:
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
        gap_pct = (abs(today_open - prev_close) / prev_close
                   if prev_close > 0 else 0.0)
        return gap_filter(today_open, prev_close, max_gap_pct), gap_pct

    # ── Asset-Class Hooks ──────────────────────────────────────────────
    # Die Kern-Signallogik bleibt equity/futures/crypto-agnostisch. Die
    # folgenden fünf Hooks kapseln alle asset-abhängigen Entscheidungen.

    def _asset_class(self) -> str:
        return str(self.config.get("asset_class", "equity")).lower()

    def _is_trading_session(self, ts) -> bool:
        """Asset-class-abhängige Session-Prüfung."""
        ac = self._asset_class()
        if ac == "crypto":
            return True
        if ac == "futures":
            et_dt = to_et(ts)
            wday = et_dt.weekday()              # 0=Mon … 6=Sun
            t = et_dt.time()
            open_t = _coerce_time(self.config.get("futures_session_open")) \
                or time(18, 0)
            close_t = _coerce_time(self.config.get("futures_session_close")) \
                or time(17, 0)
            # CME Globex vereinfacht: Mo 18:00 ET – Fr 17:00 ET. Sa/So
            # keine Trades. Wochenrand-Lücken an Mon/Fri werden exakt
            # abgefangen.
            if wday >= 5:                       # Sa, So
                return False
            if wday == 4 and t >= close_t:      # Freitag nach Close
                return False
            if wday == 0 and t < open_t:        # Montag vor Open
                return False
            return True
        # equity
        return is_market_hours(ts)

    def _entry_cutoff_ok(self, ts) -> bool:
        """Asset-class-abhängiger Entry-Cutoff."""
        ac = self._asset_class()
        if ac == "futures":
            cutoff = _coerce_time(self.config.get("futures_entry_cutoff"))
            if cutoff is None:
                return True
            return entry_cutoff_ok(ts, cutoff)
        if ac == "crypto":
            cutoff = _coerce_time(self.config.get("crypto_entry_cutoff"))
            if cutoff is None:
                return True
            return entry_cutoff_ok(ts, cutoff)
        return entry_cutoff_ok(ts, _coerce_time(
            self.config.get("entry_cutoff_time"),
        ))

    def _trend_reference_asset_key(self) -> Optional[str]:
        """Default-Routing: equity→SPY (implizit), futures→ES, crypto→None."""
        explicit = self.config.get("trend_reference_asset")
        if explicit:
            return str(explicit).upper()
        ac = self._asset_class()
        if ac == "futures":
            return "ES"
        if ac == "equity":
            return "SPY"
        return None

    def _resolve_trend(self, day_key) -> dict:
        """Liefert {'bullish': bool, 'bearish': bool} je nach Asset-Klasse."""
        ac = self._asset_class()
        ema_period = int(self.config.get("trend_ema_period", 20))

        if ac == "equity" and not self.config.get("trend_reference_asset"):
            return trend_filter_from_spy(
                self.context.spy_df_asof(self.context.now), ema_period,
            )

        ref = self._trend_reference_asset_key()
        if ref is None:
            return {"bullish": True, "bearish": True}

        ref_bars = self.context.bars(ref)
        if not ref_bars:
            return {"bullish": True, "bearish": True}
        df = _bars_to_df(ref_bars)
        if df.empty or len(df) < ema_period:
            return {"bullish": True, "bearish": True}
        return trend_filter_from_spy(df, ema_period)

    def _gap_check_for_asset(self, df_5m: pd.DataFrame) -> tuple[bool, float]:
        """Gap-Filter nur für equity; futures/crypto → neutral."""
        if self._asset_class() != "equity":
            return True, 0.0
        return self._gap_check(
            df_5m, float(self.config.get("max_gap_pct", 0.03)),
        )

    def _effective_risk_qty(
        self,
        equity: float,
        risk_pct: float,
        entry: float,
        stop: float,
    ) -> float:
        """Asset-spezifische Stückzahl/Kontrakt-Anzahl je Trade."""
        ac = self._asset_class()
        if equity <= 0 or entry <= 0:
            return 0.0
        if ac == "futures":
            risk_dollars = equity * risk_pct
            points_at_risk = abs(entry - stop)
            if points_at_risk <= 0:
                return 0.0
            symbol_guess = ""
            try:
                symbol_guess = self.bars[-1].symbol.upper() if self.bars else ""
            except Exception:  # noqa: BLE001
                symbol_guess = ""
            point_value = float(self.config.get(
                "futures_point_value",
                FUTURES_POINT_VALUES.get(symbol_guess, 0.0),
            ))
            if point_value <= 0:
                return 0.0
            qty = risk_dollars / (points_at_risk * point_value)
            return max(1.0, float(round(qty)))
        # equity + crypto: shares/units via bestehender R-Formel
        return float(position_size(equity, risk_pct, entry, stop))
