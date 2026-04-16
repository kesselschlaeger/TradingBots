"""Scheduler – APScheduler-Wrapper für tägliche Live-Bot-Events.

Events:
  - premarket_scan: ~30 min vor Open, scannt Gap-Kandidaten
  - market_open: Strategien starten, Bars streamen
  - eod_close: Positionen vor Close flattenen
  - post_market: Daily-Summary, State persistieren

Nutzt APScheduler 3.x (BackgroundScheduler / AsyncIOScheduler).
"""
from __future__ import annotations

from datetime import time
from typing import Any, Callable, Coroutine, Optional

from core.logging import get_logger

log = get_logger(__name__)

try:
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    APSCHEDULER_AVAILABLE = True
except ImportError:
    APSCHEDULER_AVAILABLE = False


class TradingScheduler:
    """Täglicher Zeitplan für Live-Bots.

    Alle Zeiten in US/Eastern. Der Runner übergibt die konkreten
    Callback-Coroutinen.
    """

    def __init__(self, tz: str = "America/New_York"):
        if not APSCHEDULER_AVAILABLE:
            raise RuntimeError("apscheduler fehlt – pip install apscheduler")
        self.tz = tz
        self._scheduler = AsyncIOScheduler(timezone=tz)

    def add_job(self, name: str, func: Callable[..., Coroutine],
                trigger_time: time, **kwargs: Any) -> None:
        trigger = CronTrigger(
            hour=trigger_time.hour,
            minute=trigger_time.minute,
            second=trigger_time.second,
            day_of_week="mon-fri",
            timezone=self.tz,
        )
        self._scheduler.add_job(func, trigger, id=name,
                                replace_existing=True, **kwargs)
        log.info("scheduler.job_added", name=name,
                 time=trigger_time.strftime("%H:%M:%S"))

    def schedule_trading_day(self,
                             premarket_scan: Optional[Callable] = None,
                             on_market_open: Optional[Callable] = None,
                             on_eod_close: Optional[Callable] = None,
                             on_post_market: Optional[Callable] = None,
                             premarket_time: time = time(9, 0),
                             market_open_time: time = time(9, 30),
                             eod_close_time: time = time(15, 27),
                             post_market_time: time = time(16, 5),
                             ) -> None:
        if premarket_scan:
            self.add_job("premarket_scan", premarket_scan, premarket_time)
        if on_market_open:
            self.add_job("market_open", on_market_open, market_open_time)
        if on_eod_close:
            self.add_job("eod_close", on_eod_close, eod_close_time)
        if on_post_market:
            self.add_job("post_market", on_post_market, post_market_time)

    def start(self) -> None:
        if not self._scheduler.running:
            self._scheduler.start()
            log.info("scheduler.started")

    def stop(self) -> None:
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            log.info("scheduler.stopped")

    @property
    def running(self) -> bool:
        return self._scheduler.running
