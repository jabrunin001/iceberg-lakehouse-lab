# Building an Iceberg lakehouse, and why table format matters

> **Draft outline — fill prose from the actual output of `demos/01`–`05` after running them.**

## 1. A data lake is a swamp without a table format

Most "data lakes" start as a directory of Parquet files in object storage. It works
until you need to: change a column, delete a few rows for a GDPR request, see what the
data looked like last Tuesday, or let two engines read it safely at once. Plain Parquet
on a directory gives you none of that.

## 2. What a table format adds

A table format (Iceberg) layers a catalog + metadata + snapshots over those files:
atomic commits, schema/partition evolution, time-travel, and ACID row-level operations —
while the data stays open Parquet anyone can read. _(See `demos/05_iceberg_vs_parquet.py`.)_

## 3. The capabilities, with real output

- **Time-travel & rollback** — recover from a bad load without a backup restore.
  _(Paste the before/after counts from `demos/01_time_travel.py`.)_
- **Schema evolution** — add a column with no data rewrite; old rows read as NULL.
  _(Paste the `DESCRIBE` before/after from `demos/02_schema_evolution.py`.)_
- **Hidden partitioning & partition evolution** — partition by `days(event_ts)` without a
  partition column, then change the layout for future data only.
  _(Paste partition spec from `demos/03_hidden_partitioning.py`.)_
- **Maintenance** — compact small files and expire snapshots.
  _(Paste file/snapshot counts from `demos/04_compaction_maintenance.py`.)_

## 4. Why modern platforms (incl. Netflix) run this way

This lab deliberately separates **storage** (MinIO/S3), **catalog** (Iceberg REST), and
**compute** (Spark for writes, Trino for reads). That separation is the core lakehouse
idea.

## 5. Try it yourself

```bash
make up && make seed && make ingest && make build && make test && make demo
```

Full architecture: [`docs/architecture.md`](../architecture.md).
