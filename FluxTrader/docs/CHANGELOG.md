# Changelog

## [Unreleased] — 2026-04-25

### Added

#### 5-State Health Machine (`live/health.py`, `live/health_eval.py`)
- New `overall_state(strategy, now)` method returns one of five states:
  `OK | IDLE_OUT_OF_WINDOW | DATA_STALE | PROCESS_DEAD | CIRCUIT_BREAK`
  evaluated in priority order (CIRCUIT_BREAK wins over all).
- `next_expected_bar_at(last_bar_ts)` — dynamic formula:
  `last_bar_ts + bar_timeframe_seconds + provider_poll_interval_s + stale_tolerance_s`
- `trade_window_phase(now)` — returns current phase:
  `premarket | regular | after_cutoff | after_eod | closed`
- `set_watchdog(strategy, ts)` — writer called by the runner watchdog loop.
- `snapshot()` now returns per-strategy: `last_watchdog_ts`, `next_expected_bar_at`,
  `seconds_to_next_bar`, `trade_window`, `in_trade_window`, `process_alive`,
  `data_flowing`, `overall_state`.
- New `live/health_eval.py`: stateless pure functions (`evaluate_liveness`,
  `next_expected_bar_at`, `trade_window_phase`) for dashboard router and CLI.

#### Runner Watchdog Loop (`live/runner.py`)
- `_watchdog_loop()` task — fires every `watchdog_interval_s` seconds,
  calls `health.set_watchdog()` and `state.upsert_bot_heartbeat(..., last_watchdog_ts=now)`.
- `_emit_health_alerts()` — called after each bar; delegates `data_stale` and
  `circuit_break` events to `notifier.alert_health()`.

#### Unified `alert_health()` Notifier Pipeline (`live/notifier.py`)
- `_HealthAlertTracker` — per-(bot, strategy, check) state machine:
  `NOOP → FIRING → FIRING_ONGOING → RESOLVED`.
- `alert_health(event, *, level, bot_name, strategy, check_name, is_firing)` —
  deduplication (`dedup_window_s`), reminder throttle (`reminder_interval_min`),
  routing via `alerts.routing.channel_health`.
- `send_health()` now returns `False` immediately when no health channel is configured
  (no silent fallback to main channel).

#### External `healthcheck` CLI (`main.py`)
- `cmd_healthcheck(cfg)` — dead-man's switch that queries the live DB and prints
  per-bot liveness (state, seconds-to-next-bar, watchdog age).
- Invoked via `python main.py healthcheck --config <file>`.

#### Dashboard Traffic-Light View (`dashboard/`)
- `GET /api/health/overview` — new endpoint returning per-bot liveness snapshot
  with `overall_state`, `active_alerts`, `process_alive`, `data_flowing`,
  `in_trade_window`, `next_expected_bar_at`, `seconds_to_next_bar`.
- Frontend: bot cards show color-coded traffic light (green/yellow/red/gray)
  based on `overall_state`; active alert pills displayed in header strip.

#### Config Extensions (`core/config.py`, all `configs/*.yaml`)
- `TradeWindowPhaseConfig` — per-phase alert enable flags
  (`premarket_alert`, `after_cutoff_alert`, `after_eod_alert`).
- `MonitoringConfig` extended: `watchdog_interval_s`, `bar_timeframe_seconds`,
  `provider_poll_interval_s`, `stale_tolerance_s`, `grace_period_s`,
  `reminder_interval_min`, `trade_window_phases`.
- `AlertsRoutingConfig` — `channel_default`, `channel_health`, `channel_readiness`.
- `AlertsConfig` extended: `dedup_window_s`, `routing: AlertsRoutingConfig`.
- All 17 YAML configs updated with `monitoring:` and `alerts:` blocks.

#### Trade Window Helpers (`core/filters.py`)
- `is_before_premarket()`, `is_after_entry_cutoff()`, `is_after_eod_close()`,
  `in_regular_trade_window()` — all accept dict or object config.

### Changed

- **`live/state.py`**: `bot_heartbeat` table gains `last_watchdog_ts TEXT` column
  (migration via `ALTER TABLE` if missing); `upsert_bot_heartbeat` uses
  `INSERT ... ON CONFLICT DO UPDATE SET` with COALESCE so partial updates are safe.
- **`live/anomaly.py`**: `check_heartbeat()` removed — connectivity liveness is now
  exclusively handled by the health state machine. Two corresponding unit tests
  marked `skip`.
- **`live/notifier.py`**: No channel fallback — health/readiness channels require
  explicit `bot_token` + `chat_id`; if not set, `send_health`/`send_readiness`
  return `False` immediately.
- **`main.py`**: `HealthState` init now receives `strategy_config` and
  `monitoring_config`; `LiveRunner` receives `monitoring_cfg`.

### Fixed

- `open_trade_atomic` INSERT no longer references the non-existent `reason` column
  (schema uses `exit_reason`).
- `upsert_bot_heartbeat` VALUES tuple corrected to 10 elements matching 10 columns.
- `close_trade_atomic` test corrected to use `exit_reason=` kwarg.
