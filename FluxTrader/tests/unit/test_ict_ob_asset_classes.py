"""Tests für Asset-Class-Routing in strategy/ict_ob.py.

Deckt die asset-spezifischen Hooks (_is_trading_session, _entry_cutoff_ok,
_resolve_trend, _gap_check_for_asset, _effective_risk_qty) sowie die
Contract-Factory für IBKR.
"""
from __future__ import annotations

from datetime import datetime, time, timezone

import numpy as np
import pandas as pd
import pytest
import pytz

from core.models import Bar
from strategy.ict_ob import FUTURES_POINT_VALUES, IctOrderBlockStrategy
from tests.conftest import _et_dt, make_ohlcv

ET = pytz.timezone("America/New_York")


# ─────────────────────────── Helpers ────────────────────────────────────────

def _strat(context, **params) -> IctOrderBlockStrategy:
    return IctOrderBlockStrategy(params, context=context)


def _utc(year, month, day, hour, minute=0):
    """ET-Zeitpunkt nach UTC konvertieren."""
    return ET.localize(datetime(year, month, day, hour, minute)) \
        .astimezone(timezone.utc)


# ─────────────────────────── Trend Filter ──────────────────────────────────

class TestEquityTrend:
    def test_equity_uses_spy_trend(self, context):
        # SPY-Aufwärtstrend: Close > EMA(20) → bullish=True, bearish=False
        # spy_df_asof filtert strikt auf Tage < now → spy_df liegt einen Tag
        # vor now, damit alle Bars einfließen.
        df = make_ohlcv(
            50, base=520.0, seed=7, start=_et_dt(2025, 3, 10, 9, 30),
        )
        df["Close"] = df["Close"] + np.linspace(0, 5, len(df))
        context.set_spy_df(df)
        context.set_now(_et_dt(2025, 3, 12, 15, 0))

        strat = _strat(context, asset_class="equity", trend_ema_period=20)
        trend = strat._resolve_trend(day_key="2025-03-12")
        assert trend["bullish"] is True
        assert trend["bearish"] is False

    def test_equity_trend_reference_key_is_spy(self, context):
        strat = _strat(context, asset_class="equity")
        assert strat._trend_reference_asset_key() == "SPY"


# ─────────────────────────── Gap Filter ────────────────────────────────────

class TestFuturesGapFilter:
    def test_futures_ignores_gap_filter(self, context):
        # Minimaler DataFrame reicht – Methode darf ihn für Futures gar nicht anfassen
        df_empty = pd.DataFrame(
            columns=["Open", "High", "Low", "Close", "Volume"]
        )
        strat = _strat(context, asset_class="futures")
        ok, gap_pct = strat._gap_check_for_asset(df_empty)
        assert ok is True
        assert gap_pct == 0.0

    def test_crypto_ignores_gap_filter(self, context):
        df_empty = pd.DataFrame(
            columns=["Open", "High", "Low", "Close", "Volume"]
        )
        strat = _strat(context, asset_class="crypto")
        ok, gap_pct = strat._gap_check_for_asset(df_empty)
        assert ok is True
        assert gap_pct == 0.0


# ─────────────────────────── Sessions / Cutoffs ────────────────────────────

class TestCryptoSession:
    def test_crypto_no_session_block(self, context):
        strat = _strat(context, asset_class="crypto")
        # 03:00 UTC = 23:00 ET – außerhalb jeder Equity-Session
        ts = datetime(2025, 3, 15, 3, 0, tzinfo=timezone.utc)
        assert strat._is_trading_session(ts) is True
        # Sonntag 12:00 UTC – auch Crypto-Session
        ts_sun = datetime(2025, 3, 16, 12, 0, tzinfo=timezone.utc)
        assert strat._is_trading_session(ts_sun) is True

    def test_crypto_no_cutoff_when_null(self, context):
        strat = _strat(context, asset_class="crypto", crypto_entry_cutoff=None)
        ts = _utc(2025, 3, 12, 23, 59)
        assert strat._entry_cutoff_ok(ts) is True


class TestFuturesSession:
    def test_futures_saturday_closed(self, context):
        strat = _strat(context, asset_class="futures")
        ts_sat = _utc(2025, 3, 15, 12, 0)  # Samstag mittags
        assert strat._is_trading_session(ts_sat) is False

    def test_futures_sunday_closed(self, context):
        strat = _strat(context, asset_class="futures")
        ts_sun = _utc(2025, 3, 16, 20, 0)  # Sonntag 20:00 ET
        assert strat._is_trading_session(ts_sun) is False

    def test_futures_monday_before_open_closed(self, context):
        strat = _strat(
            context, asset_class="futures", futures_session_open="18:00",
        )
        ts = _utc(2025, 3, 17, 10, 0)  # Mo 10:00 ET vor Open
        assert strat._is_trading_session(ts) is False

    def test_futures_monday_after_open_open(self, context):
        strat = _strat(
            context, asset_class="futures", futures_session_open="18:00",
        )
        ts = _utc(2025, 3, 17, 19, 0)  # Mo 19:00 ET nach Open
        assert strat._is_trading_session(ts) is True

    def test_futures_tuesday_morning_open(self, context):
        strat = _strat(context, asset_class="futures")
        ts = _utc(2025, 3, 18, 3, 0)  # Di 03:00 ET mitten in Nacht-Session
        assert strat._is_trading_session(ts) is True

    def test_futures_friday_after_close_closed(self, context):
        strat = _strat(
            context, asset_class="futures", futures_session_close="17:00",
        )
        ts = _utc(2025, 3, 21, 18, 0)  # Fr 18:00 ET nach Close
        assert strat._is_trading_session(ts) is False

    def test_futures_entry_cutoff_respected(self, context):
        strat = _strat(
            context, asset_class="futures", futures_entry_cutoff="15:45",
        )
        # vor Cutoff: ok
        assert strat._entry_cutoff_ok(_utc(2025, 3, 18, 15, 0)) is True
        # nach Cutoff: geblockt
        assert strat._entry_cutoff_ok(_utc(2025, 3, 18, 15, 46)) is False

    def test_futures_entry_cutoff_none_is_always_ok(self, context):
        strat = _strat(
            context, asset_class="futures", futures_entry_cutoff=None,
        )
        assert strat._entry_cutoff_ok(_utc(2025, 3, 18, 23, 0)) is True


# ─────────────────────────── Position Sizing ───────────────────────────────

class TestFuturesSizing:
    def test_futures_position_sizing_nq(self, context):
        strat = _strat(
            context, asset_class="futures",
            futures_point_value=FUTURES_POINT_VALUES["NQ"],
        )
        # equity 200k, risk 0.5% = $1000, stop 50 Punkte → $1000 / ($20 * 50) = 1 Kontrakt
        qty = strat._effective_risk_qty(
            equity=200_000.0, risk_pct=0.005,
            entry=18_000.0, stop=17_950.0,
        )
        assert qty == 1.0

    def test_futures_position_sizing_minimum_one(self, context):
        strat = _strat(
            context, asset_class="futures", futures_point_value=20.0,
        )
        # Winziges Equity → rechnerisch < 1 Kontrakt → Clamp auf 1
        qty = strat._effective_risk_qty(
            equity=1_000.0, risk_pct=0.005,
            entry=18_000.0, stop=17_950.0,
        )
        assert qty >= 1.0

    def test_crypto_uses_share_sizing(self, context):
        strat = _strat(context, asset_class="crypto")
        qty = strat._effective_risk_qty(
            equity=100_000.0, risk_pct=0.005,
            entry=50_000.0, stop=49_000.0,
        )
        # position_size: risk 500 / 1000 stop → 0 shares (notional-cap)
        # Wichtig ist nur: same code path wie equity, nicht die Futures-Formel
        assert isinstance(qty, float)
        assert qty >= 0.0


# ─────────────────────────── Trend-Routing Futures ─────────────────────────

class TestFuturesTrend:
    def test_futures_trend_resolves_from_es(self, context):
        # ES-Referenz-Bars in den Context pushen (Aufwärtstrend)
        base_ts = _et_dt(2025, 3, 12, 9, 30)
        closes = np.linspace(5000.0, 5100.0, 50)
        for i, c in enumerate(closes):
            ts = base_ts + pd.Timedelta(minutes=5 * i)
            ts = pd.Timestamp(ts).to_pydatetime()
            context.push_bar(Bar(
                symbol="ES", timestamp=ts,
                open=c - 0.5, high=c + 1.0, low=c - 1.0,
                close=float(c), volume=1000,
            ))

        strat = _strat(
            context,
            asset_class="futures",
            trend_reference_asset="ES",
            trend_ema_period=20,
        )
        trend = strat._resolve_trend(day_key="2025-03-12")
        assert trend["bullish"] is True
        assert trend["bearish"] is False
        assert strat._trend_reference_asset_key() == "ES"

    def test_futures_trend_missing_bars_neutral(self, context):
        # Keine ES-Bars → neutral (bullish=bearish=True)
        strat = _strat(
            context, asset_class="futures",
            trend_reference_asset="ES", trend_ema_period=20,
        )
        trend = strat._resolve_trend(day_key="2025-03-12")
        assert trend == {"bullish": True, "bearish": True}


# ─────────────────────────── Contract Factory ──────────────────────────────

try:
    import ib_insync  # noqa: F401
    _IB_AVAILABLE = True
except ImportError:
    _IB_AVAILABLE = False


@pytest.mark.skipif(not _IB_AVAILABLE, reason="ib_insync fehlt")
class TestContractFactory:
    def test_contract_factory_equity(self):
        from ib_insync import Stock
        from execution.contract_factory import build_contract

        c = build_contract("AAPL", "equity", {})
        assert isinstance(c, Stock)
        assert c.symbol == "AAPL"
        assert c.exchange == "SMART"
        assert c.currency == "USD"

    def test_contract_factory_futures(self):
        from ib_insync import Future
        from execution.contract_factory import build_contract

        c = build_contract("NQ", "futures", {"futures_exchange": "CME"})
        assert isinstance(c, Future)
        assert c.symbol == "NQ"
        assert c.exchange == "CME"
        assert c.currency == "USD"
        # Front-Month: kein lastTradeDate gesetzt
        assert not getattr(c, "lastTradeDateOrContractMonth", "")

    def test_qualify_contract_futures_picks_nearest_non_expired(self):
        from types import SimpleNamespace

        from execution.contract_factory import build_contract, qualify_contract

        class FakeIB:
            def reqContractDetails(self, _contract):
                return [
                    SimpleNamespace(contract=SimpleNamespace(
                        symbol="NQ",
                        exchange="CME",
                        lastTradeDateOrContractMonth="20271217",
                        localSymbol="NQZ7",
                        tradingClass="NQ",
                        conId=2,
                    )),
                    SimpleNamespace(contract=SimpleNamespace(
                        symbol="NQ",
                        exchange="CME",
                        lastTradeDateOrContractMonth="20260618",
                        localSymbol="NQM6",
                        tradingClass="NQ",
                        conId=1,
                    )),
                ]

            def qualifyContracts(self, contract):
                return [contract]

        contract = build_contract("NQ", "futures", {"futures_exchange": "CME"})
        resolved = qualify_contract(FakeIB(), contract, "futures")

        assert resolved is not None
        assert resolved.lastTradeDateOrContractMonth == "20260618"
        assert resolved.localSymbol == "NQM6"

    def test_contract_factory_crypto(self):
        from execution.contract_factory import (
            CRYPTO_AVAILABLE,
            build_contract,
        )

        c = build_contract("BTC", "crypto", {
            "crypto_quote_currency": "USD",
        })
        assert c.symbol == "BTC"
        assert c.currency == "USD"
        if CRYPTO_AVAILABLE:
            from ib_insync import Crypto
            assert isinstance(c, Crypto)
            assert c.exchange == "PAXOS"
        else:
            from ib_insync import Stock
            assert isinstance(c, Stock)
            assert c.exchange == "PAXOS"


def test_ibkr_what_to_show_defaults_and_override():
    from data.providers.ibkr_provider import (
        _resolve_what_to_show,
        _resolve_what_to_show_candidates,
    )

    assert _resolve_what_to_show("equity", {}) == "TRADES"
    assert _resolve_what_to_show("futures", {}) == "MIDPOINT"
    assert _resolve_what_to_show("crypto", {}) == "AGGTRADES"
    assert _resolve_what_to_show("futures", {"ibkr_what_to_show": "BID_ASK"}) == "BID_ASK"
    assert _resolve_what_to_show_candidates("futures", {}) == ["MIDPOINT", "BID_ASK"]
    assert _resolve_what_to_show_candidates("futures", {"ibkr_what_to_show": "BID_ASK"}) == ["BID_ASK"]
