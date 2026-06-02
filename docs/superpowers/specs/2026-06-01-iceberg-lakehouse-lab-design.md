# iceberg-lakehouse-lab — Design Spec

**Date:** 2026-06-01
**Status:** Approved (design), pending implementation plan
**Author:** James Bruning

## Purpose

A local, Dockerized lakehouse that demonstrates fluency with Apache Iceberg — the
single most Netflix-specific gap in James's data-engineering portfolio. It ingests
real MovieLens data plus a synthetic playback-events stream into Iceberg, transforms
it through medallion layers with dbt, validates it with dbt tests + Great Expectations,
and showcases the Iceberg capabilities Netflix relies on. Paired with a blog post:
*"Building an Iceberg lakehouse, and why table format matters."*

**Primary goal:** a credible, polished, fully-working showcase shippable in ~2–3
weekends — optimized for signaling "I know Iceberg / lakehouse" to a Netflix hiring
manager, not for exhaustive internals research.

**Target roles this supports:**
- Netflix **Security Analytics Engineer (L4, JR39980)** — the Great Expectations
  data-quality suite is the concrete signal here.
- Any Netflix **Data & Insights** Data Engineer role — Iceberg + dbt + Spark fidelity.

## Non-Goals (YAGNI)

- Not a deep internals research project (no custom Iceberg connectors, no JVM source work).
- No cloud deployment — local Docker only.
- No streaming/Flink runtime — "events" arrive as daily batch appends (enough to demo
  snapshots/time-travel/incremental).
- No Airflow in the MVP — orchestration is a Makefile; Airflow is documented as a
  "next step," not built.

## Decisions (locked during brainstorming)

| Decision | Choice | Why |
|----------|--------|-----|
| Engine/stack | **PySpark + Iceberg + dbt-spark + Trino, in Docker** | Literally Netflix's stack; James already knows PySpark; stays in Python (sidesteps the Java gap). |
| Storage + catalog | **MinIO (S3) + Iceberg REST catalog** | Realistic lakehouse footprint; separates storage / catalog / compute. |
| Dataset | **MovieLens (real) + synthetic playback-events generator** | Real + recognizable + on-theme, plus controllable incremental loads for Iceberg demos. |
| Orchestration | **Makefile** | Leanest path to a working demo. |
| Data quality | **dbt tests (+ dbt-expectations) across silver/gold, + focused Great Expectations suite on the critical gold table** | dbt tests = breadth; GE suite = the DQ-framework signal JR39980 wants. |

## Architecture

Object-store-backed lakehouse, mirroring how Netflix runs (S3 + catalog + Spark + Trino):

```
┌─────────────┐   ┌──────────────────┐   ┌─────────┐
│  PySpark    │──▶│ Iceberg REST     │◀──│  Trino  │   (query layer)
│ (ingest +   │   │ catalog          │   └─────────┘
│  dbt-spark) │   └──────────────────┘        │
└─────────────┘            │                  ▼
        │                  ▼            sample SQL queries
        └────────▶  MinIO (S3-compatible warehouse)
```

**Components (Docker Compose services):**
- **MinIO** — S3-compatible object storage (the data lake / warehouse bucket).
- **Iceberg REST catalog** — table catalog (modern, Netflix-style).
- **Spark** (PySpark + Iceberg runtime jars) — ingestion and dbt-spark transforms.
- **Trino** (Iceberg connector → same REST catalog + MinIO) — interactive SQL query engine.

Each component has one clear job and communicates through well-defined interfaces
(S3 API, Iceberg REST API, Spark/Trino SQL), so each can be understood and swapped
independently.

## Data Flow (medallion layers)

- **Bronze** — raw ingest, append-only Iceberg tables:
  - MovieLens: `movies`, `ratings`, `tags`, `links`
  - Synthetic: `playback_events` (appended in daily batches)
- **Silver** (dbt-spark) — cleaned/conformed/typed:
  - `dim_movie`, `dim_user`, `fact_rating`, `fact_playback_event`
- **Gold** (dbt-spark) — business marts:
  - `movie_engagement` (views, completion rate, avg rating) ← **critical table, GE-validated**
  - `daily_active_users`
  - `top_titles`

## Synthetic Playback-Events Generator

Python script producing daily batches of playback events:
`{user_id, movie_id, event_type ∈ {play, pause, seek, complete}, event_ts, device, position_seconds}`.
Each run appends a new day's partition to the bronze `playback_events` Iceberg table.
This is what makes the time-travel, partitioning, and maintenance demos concrete and
reproducible.

## Iceberg Feature Showcase (`demos/`)

Each script demonstrates one Iceberg capability and maps to a blog section:

1. `01_time_travel.py` — query as-of snapshot; roll back a deliberately-bad load.
2. `02_schema_evolution.py` — add a column to events; old data still reads, no rewrite.
3. `03_hidden_partitioning.py` — `days(event_ts)` hidden partitioning + partition evolution.
4. `04_compaction_maintenance.py` — snapshot expiry + `rewrite_data_files` compaction.
5. `05_iceberg_vs_parquet.py` — same data as plain Parquet/Hive vs Iceberg → the blog's
   central "why table format matters" argument (atomic commits, safe schema change,
   time-travel, no expensive directory listings).

## Data Quality

- **dbt tests** across silver + gold: `not_null`, `unique`, `relationships`,
  `accepted_values`, plus `dbt-expectations` for richer column/row assertions.
- **Great Expectations** suite on gold `movie_engagement`: value ranges (completion rate
  ∈ [0,1], avg rating ∈ [0.5,5]), row-count bounds, and freshness.

## Repository Structure

```
iceberg-lakehouse-lab/
├── README.md                 # showcase centerpiece (written for a Netflix HM)
├── docker-compose.yml
├── Makefile                  # up / seed / ingest / build / test / demo / query / down
├── .github/workflows/ci.yml  # ruff + dbt parse + smoke test on sample data
├── ingestion/
│   ├── download_movielens.py
│   ├── generate_playback_events.py
│   └── ingest_bronze.py      # PySpark → Iceberg bronze
├── dbt/                      # dbt-spark project
│   ├── models/silver/...
│   ├── models/gold/...
│   └── tests/
├── quality/
│   └── great_expectations/   # GE suite on gold movie_engagement
├── demos/
│   ├── 01_time_travel.py
│   ├── 02_schema_evolution.py
│   ├── 03_hidden_partitioning.py
│   ├── 04_compaction_maintenance.py
│   └── 05_iceberg_vs_parquet.py
├── trino/                    # catalog config + sample queries
├── docs/
│   ├── architecture.md       # + diagram
│   └── blog/why-table-format-matters.md
└── data/                     # gitignored
```

## Testing Strategy

- **Data:** dbt tests on every silver/gold model.
- **Data-quality framework:** Great Expectations suite on the critical gold table.
- **Code:** pytest smoke tests for the event generator and bronze ingestion.
- **CI:** GitHub Actions runs ruff lint, `dbt parse`/compile, and a sample-data smoke
  test (ingest → build → test) so the repo carries a green badge.

## Make Targets (orchestration interface)

| Target | Action |
|--------|--------|
| `make up` / `make down` | start / stop the Docker stack |
| `make seed` | download MovieLens + generate initial playback events |
| `make ingest` | PySpark load → bronze Iceberg tables |
| `make build` | `dbt run` silver + gold |
| `make test` | `dbt test` + Great Expectations |
| `make demo` | run the 5 Iceberg feature demos |
| `make query` | open Trino with sample queries |

## Build Plan (2–3 weekends)

1. **Weekend 1 — Foundation:** Docker stack (Spark+Iceberg+MinIO+REST+Trino), MovieLens
   download, event generator v1, bronze ingest, prove a table queries in Trino.
2. **Weekend 2 — Transform + quality:** dbt silver+gold models, dbt tests, GE suite,
   Makefile glue.
3. **Weekend 3 — Showcase:** the 5 Iceberg demos, README + architecture diagram, blog
   post, CI, polish.

## Deliverables

- Working `make up && make seed && make ingest && make build && make test && make demo`.
- README written to impress a Netflix hiring manager.
- Architecture diagram.
- Blog post: "Building an Iceberg lakehouse, and why table format matters."
- Green CI badge.

## Location

Separate repo at `~/Projects/iceberg-lakehouse-lab` (this directory). Not part of the
CareerOps job-search system. Destined for James's public GitHub.
