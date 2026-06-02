# Architecture

## Component diagram

```mermaid
flowchart LR
    subgraph Ingest
      ML[MovieLens CSV] --> B
      GEN[Synthetic events] --> B
    end
    B[PySpark ingest] -->|writes| ICE[(Iceberg tables\nbronze/silver/gold)]
    DBT[dbt-spark] -->|transforms| ICE
    ICE -. catalog .- REST[Iceberg REST catalog]
    ICE -. storage .- MINIO[(MinIO S3)]
    TRINO[Trino] -->|reads| ICE
    GE[Great Expectations] -->|validates gold| TRINO
```

## Components

| Component | Role |
|-----------|------|
| MinIO | S3-compatible object store (the data lake) |
| Iceberg REST catalog | Tracks table metadata / snapshots |
| Spark (PySpark) | Ingestion + dbt-spark transforms |
| Trino | Interactive SQL query engine |
| dbt-spark | Bronze→silver→gold transformations + tests |
| Great Expectations | Data-quality validation on the critical gold table |

## Data layers

- **Bronze:** raw, append-only (`movies`, `ratings`, `tags`, `links`, `playback_events`)
- **Silver:** conformed (`dim_movie`, `dim_user`, `fact_rating`, `fact_playback_event`)
- **Gold:** marts (`movie_engagement`, `daily_active_users`, `top_titles`)
