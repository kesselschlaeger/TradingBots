"""Tests für core/filters.timeframe_to_seconds."""
from __future__ import annotations

import pytest

from core.filters import timeframe_to_seconds


class TestTimeframeToSeconds:
    @pytest.mark.parametrize("tf,expected", [
        ("5Min", 300),
        ("5min", 300),
        ("15Min", 900),
        ("15m", 900),
        ("1Min", 60),
        ("30S", 30),
        ("30sec", 30),
        ("1H", 3600),
        ("2h", 7200),
        ("1Day", 86400),
        ("1D", 86400),
        ("1d", 86400),
        ("1w", 604800),
    ])
    def test_known_timeframes(self, tf, expected):
        assert timeframe_to_seconds(tf) == expected

    def test_unknown_unit_defaults_to_300(self):
        assert timeframe_to_seconds("5parsec") == 300

    def test_empty_defaults_to_300(self):
        assert timeframe_to_seconds("") == 300
        assert timeframe_to_seconds(None) == 300  # type: ignore[arg-type]

    def test_case_insensitive(self):
        assert timeframe_to_seconds("5MIN") == timeframe_to_seconds("5min")
