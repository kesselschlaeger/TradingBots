"""Tests für core/ml_filter.py – MLFilter + Null-Object-Pattern.

Kein Netzwerk, kein trainiertes Modell nötig (nur disabled-Mode getestet).
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import numpy as np
import pytest

from core.ml_filter import MLFilter, build_ml_filter
from core.models import BaseSignal, FeatureVector, PairSignal, Signal


# ─────────────────────────── MLFilter.disabled() ──────────────────────────────


class TestMLFilterDisabled:
    def test_passes_always_true(self):
        ml = MLFilter.disabled()
        sig = Signal(strategy="orb", symbol="AAPL", direction=1, strength=0.8)
        assert ml.passes(sig) is True

    def test_predict_returns_half(self):
        ml = MLFilter.disabled()
        sig = Signal(strategy="orb", symbol="AAPL")
        assert ml.predict(sig) == 0.5

    def test_passes_pair_signal(self):
        ml = MLFilter.disabled()
        ps = PairSignal(
            strategy="botti_pair", symbol="SPY",
            long_symbol="SPY", short_symbol="QQQ",
            z_score=2.0, action="ENTER",
        )
        assert ml.passes(ps) is True

    def test_predict_pair_signal(self):
        ml = MLFilter.disabled()
        ps = PairSignal(strategy="botti_pair", symbol="SPY")
        assert ml.predict(ps) == 0.5

    def test_works_with_custom_features(self):
        ml = MLFilter.disabled()
        fv = FeatureVector(sma_diff=0.1, adx=25.0, rsi=55.0, macd_hist=0.5)
        sig = Signal(strategy="botti", symbol="AAPL", features=fv,
                     direction=1, strength=0.7)
        assert ml.passes(sig) is True
        assert ml.predict(sig) == 0.5


# ─────────────────────────── build_ml_filter ──────────────────────────────────


class TestBuildMLFilter:
    def test_disabled_when_not_enabled(self):
        ml = build_ml_filter(enabled=False)
        sig = Signal(strategy="orb", symbol="AAPL")
        assert ml.passes(sig) is True

    def test_disabled_when_no_model_path(self):
        ml = build_ml_filter(enabled=True, model_path=None)
        sig = Signal(strategy="orb", symbol="AAPL")
        assert ml.passes(sig) is True

    def test_disabled_when_model_not_found(self):
        ml = build_ml_filter(enabled=True, model_path="/nonexistent/path")
        sig = Signal(strategy="orb", symbol="AAPL")
        assert ml.passes(sig) is True

    def test_loads_real_model(self, tmp_path):
        """Trainiere ein Minimal-Modell, speichere es, lade es via MLFilter."""
        sklearn = pytest.importorskip("sklearn")
        joblib = pytest.importorskip("joblib")
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler

        X = np.random.default_rng(42).normal(size=(100, 7))
        y = (X[:, 0] > 0).astype(int)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        model = LogisticRegression(max_iter=100)
        model.fit(X_scaled, y)

        joblib.dump(model, tmp_path / "model.pkl")
        joblib.dump(scaler, tmp_path / "scaler.pkl")

        ml = build_ml_filter(enabled=True, model_path=str(tmp_path),
                             threshold=0.5)
        sig = Signal(
            strategy="test", symbol="AAPL",
            features=FeatureVector(sma_diff=1.0, adx=30.0, atr_pct=0.02,
                                   rsi=60.0, macd_hist=0.5),
            direction=1, strength=0.8,
        )
        prob = ml.predict(sig)
        assert 0.0 <= prob <= 1.0
        assert isinstance(ml.passes(sig), bool)


# ─────────────────────────── MLFilter mit BaseSignal-Subklassen ───────────────


class TestMLFilterPolymorphism:
    def test_accepts_signal(self):
        ml = MLFilter.disabled()
        sig = Signal(strategy="orb", symbol="AAPL")
        assert ml.passes(sig) is True

    def test_accepts_pair_signal(self):
        ml = MLFilter.disabled()
        ps = PairSignal(strategy="pair", symbol="SPY",
                        long_symbol="SPY", short_symbol="QQQ",
                        z_score=1.5, action="ENTER")
        assert ml.passes(ps) is True

    def test_both_are_base_signal(self):
        sig = Signal(strategy="orb", symbol="AAPL")
        ps = PairSignal(strategy="pair", symbol="SPY")
        assert isinstance(sig, BaseSignal)
        assert isinstance(ps, BaseSignal)
