# Technology Decision Record (Phase 1)

Option B (Kappa). Every layer below states the decision, the alternatives we considered, why we chose it, and the known risks. All images and libraries are pinned; nothing uses `latest`.

## 1. Language

**Decision:** Python 3.12 for the producer, PySpark for the stream job, SQL for schemas, views and validation queries.

**Alternatives:** Scala or Java (native Spark and Kafka Streams), SQL-only (ksqlDB).

**Why:** PySpark is fully supported by Spark Structured Streaming, and our team already wrote the prototype in Python. SQL is the natural language for the serving views and for validating a reprocessed table against the old one.

**Risks:** Python UDFs are slower than native Spark functions, so the job uses built-in functions only. At 50 to 100 events/s the load is small.

**Pinned:** `python:3.12.7-slim`, `confluent-kafka==2.5.3`, PySpark 3.5.1 (inside the Spark image).

## 2. Message log

**Decision:** Apache Kafka 3.8.0 in KRaft mode, one broker, one topic `inventory-events` with 3 partitions, keyed by `product_id`, JSON with an explicit schema.

**Alternatives:** Redpanda (Kafka-compatible, single binary), Apache Pulsar (tiered storage, heavier), a topic per event type.

**Why:** Kafka is the standard replayable log, with per-topic time-based retention (`retention.ms` = 7 days) and re-reading from the earliest offset, which is exactly what the Kappa reprocessing procedure needs. KRaft removes the Zookeeper container. One topic keyed by product keeps each product's events ordered and simplifies the stream job. Spark's Kafka connector is mature.

**Risks:** Retention is deletion-based, so replay works only inside the 7-day window. Kafka deletes whole segments, so we set `segment.ms` to 1 hour for accurate expiry. JSON costs more disk than Avro; a Schema Registry was rejected as extra memory for no gain at this scale, and we enforce the contract with an explicit Spark schema plus a `schema_version` field.

**Pinned:** `apache/kafka:3.8.0`.

## 3. Stream processing

**Decision:** Spark Structured Streaming 3.5.1 on a standalone cluster (one master, one worker), PySpark API, micro-batch trigger of 5 seconds.

**Alternatives:**
- Apache Flink: true event-at-a-time processing, but heavier to operate and tune in one Compose project.
- Kafka Streams or ksqlDB: tight Kafka integration, but JVM-only development or limited custom logic.
- Plain Python consumer (our prototype): simple, but event-time windows, watermarks, state recovery and checkpoints would all have to be hand-built.
- Faust or Bytewax: lighter, but smaller communities and less proven checkpointing.

**Why:** It provides every Kappa requirement natively: event-time windows, watermarks (`withWatermark`), duplicate removal (`dropDuplicatesWithinWatermark`), and checkpointed offsets and state, so a restarted job resumes without loss or double counting. It lets us run a second job with its own checkpoint directory against the same topic for reprocessing.

**Output semantics:** At-least-once delivery from Kafka, plus deduplication on `event_id`, plus idempotent upserts into Postgres, gives effectively-once results.

**Risks:** Spark is memory hungry (about 5 GB for worker, driver and a second job). A 5-second micro-batch is not true streaming, which is fine for a dashboard that refreshes every 5 seconds. `dropDuplicatesWithinWatermark` needs Spark 3.5 or newer, which is why the version is pinned.

**Pinned:** `apache/spark:3.5.1-python3`, connector packages `spark-sql-kafka-0-10_2.12:3.5.1` and `org.postgresql:postgresql:42.7.3`.

## 4. Serving store

**Decision:** PostgreSQL 16.

**Alternatives:** TimescaleDB (time-series extension), ClickHouse (very fast analytics, new SQL dialect and ops burden), DuckDB (embedded, awkward for concurrent writer plus dashboard), Redis.

**Why:** Postgres supports `INSERT ... ON CONFLICT DO UPDATE`, which makes the Spark writes idempotent. `CREATE OR REPLACE VIEW` is an atomic switch, so reprocessing cut-over is a single statement. Grafana has a built-in Postgres data source. Our output volume is tiny (about 43,000 rows per day for 30 products at one-minute granularity), so a heavier analytical store would add cost for no benefit.

**Risks:** Postgres is not built for billions of rows, so raw events are never stored there; only windowed aggregates. If volume grew, we would partition tables by day or move to TimescaleDB.

**Pinned:** `postgres:16.4-alpine`.

## 5. BI and dashboards

**Decision:** Grafana 11.2.0 with a provisioned data source and dashboard.

**Alternatives:** Apache Superset (richer BI, but needs a custom image, a metadata database and more RAM), Metabase, Redash.

**Why:** Grafana refreshes panels every few seconds, has a native Postgres plugin, and is lightweight. The data source and dashboard JSON are provisioned from files in the repository, so the dashboard configuration is exported by definition and rebuilds identically on a fresh instance.

**Risks:** Less self-service exploration than Superset. Acceptable, because the dashboard has a fixed set of operational panels.

**Pinned:** `grafana/grafana:11.2.0`.

## 6. Deployment and orchestration

**Decision:** One Docker Compose project on one EC2 t3.xlarge (16 GB RAM, 30 GB gp3), health-check ordering with `depends_on: condition: service_healthy`, a one-shot `kafka-init` container that creates the topic, and a Compose profile (`reprocess`) for the v2 job.

**Alternatives:** Kubernetes or ECS (too heavy for one instance), systemd services (not reproducible).

**Why:** It matches the project constraints and makes a rebuild from a fresh instance a documented `docker compose up -d --build` plus a short setup.

**Risks:** The single instance is a single point of failure, accepted for a course project. The instance must be stopped when not in use to protect the $100 credit.

## 7. Security and configuration

**Decision:** Inbound rules limited to ports 22 and 3000 from our own IP as `/32`; all other UIs and databases bound to `127.0.0.1` and reached through an SSH tunnel; an Elastic IP; credentials in `.env` (listed in `.gitignore`, with a committed `.env.example`).

**Alternatives:** Opening UI ports to the internet (rejected), hardcoded credentials in the Compose file (rejected; the prototype does this and Phase 2 replaces it).

**Why:** It follows the course security rules and the standard practice of keeping secrets out of the repository. The Kappa design uses no S3, so no AWS credentials are needed in the containers.

**Risks:** If our home IP changes, the Security Group must be updated.

## 8. Layers deliberately not used

| Layer | Status |
|---|---|
| Batch layer and S3 archive | Not used (Kappa has a single log) |
| Glue Catalog and Athena | Not used |
| Schema Registry | Not used; explicit Spark schema plus `schema_version` instead |
