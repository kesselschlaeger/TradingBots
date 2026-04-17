"""Optionaler ML-Konfidenz-Filter für BaseSignal-Subklassen.

Sitzt zwischen Signal-Erzeugung und Execution – framework-weit.
Modell wird einmalig geladen, dann nur gelesen (thread-safe für asyncio).

Null-Object-Pattern: MLFilter.disabled() gibt ein Objekt zurück, das
immer True liefert → kein `if config.use_ml` im Runner nötig.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np

from core.logging import get_logger
from core.models import BaseSignal

log = get_logger(__name__)


class MLFilter:
    """Konfidenz-Filter basierend auf FeatureVector.

    predict() gibt [0,1] zurück, passes() prüft gegen Schwellenwert.
    """

    def __init__(self, model_path: Path, threshold: float = 0.6):
        import joblib
        self._model = joblib.load(model_path / "model.pkl")
        self._scaler = joblib.load(model_path / "scaler.pkl")
        self.threshold = threshold
        log.info("ml_filter.loaded", path=str(model_path),
                 threshold=threshold)

    def predict(self, signal: BaseSignal) -> float:
        """Gibt Konfidenz [0,1] zurück."""
        fv = signal.features
        X = np.array([[
            fv.sma_diff, fv.adx, fv.atr_pct,
            fv.rsi, fv.macd_hist, fv.z_score, fv.volume_ratio,
        ]])
        return float(self._model.predict_proba(self._scaler.transform(X))[0, 1])

    def passes(self, signal: BaseSignal) -> bool:
        return self.predict(signal) >= self.threshold

    @classmethod
    def disabled(cls) -> MLFilter:
        """Null-Objekt: passes() gibt immer True zurück."""
        instance = cls.__new__(cls)
        instance._model = None
        instance._scaler = None
        instance.threshold = 0.0
        return instance

    def __getattribute__(self, name: str):
        # Null-Object-Pattern: wenn kein Modell geladen, immer True/0.5
        if name == "predict":
            model = object.__getattribute__(self, "_model")
            if model is None:
                return lambda signal: 0.5
        if name == "passes":
            model = object.__getattribute__(self, "_model")
            if model is None:
                return lambda signal: True
        return object.__getattribute__(self, name)


def build_ml_filter(
    enabled: bool,
    model_path: Optional[str] = None,
    threshold: float = 0.6,
) -> MLFilter:
    """Factory: baut MLFilter oder Null-Objekt je nach Config."""
    if not enabled or not model_path:
        return MLFilter.disabled()
    p = Path(model_path)
    if not (p / "model.pkl").exists():
        log.warning("ml_filter.model_not_found", path=str(p))
        return MLFilter.disabled()
    return MLFilter(p, threshold=threshold)
