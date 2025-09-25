# Unit tests
Kafka producer: ordering, payload schema, time-compression math.
Spark streaming: parsing, derivations, null/invalid handling, event_time correctness.
Spark batch: window filtering, groupby correctness, idempotency.
Airflow: DAG import, dependency graph, templated params.

# dbt tests
Schema tests (not_null, unique, ranges) + singular tests for business rules.
Freshness on event_time.

# Integration/E2E
Tiny fixture flows through: Kafka → Spark → Postgres → dbt; key checks pass.

# Coverage target (Python parts): ≥70%.