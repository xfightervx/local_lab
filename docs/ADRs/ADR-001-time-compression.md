# Context
* We want fast feedback while keeping realistic business logic and freshness behavior.

# Decision
* Use time compression: 1 simulated day = 1 real minute (configurable via env).
* Use event_time for windows/freshness/business logic; ingest_time only for ops/monitoring.

# Consequence
* Watermarks/windows think in original event time.
* Airflow schedule maps 1 minute ↔ 1 simulated day.
* Backfills are just replays over event_time ranges.