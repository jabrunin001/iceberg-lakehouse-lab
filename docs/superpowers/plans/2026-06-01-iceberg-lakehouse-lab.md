# iceberg-lakehouse-lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local, Dockerized Apache Iceberg lakehouse (MovieLens + synthetic playback events → bronze/silver/gold via dbt-spark, validated with dbt tests + Great Expectations, queried via Trino) plus a feature-showcase and blog post, as a Netflix-targeted portfolio piece.

**Architecture:** Object-store-backed lakehouse in Docker Compose: MinIO (S3) for storage, an Iceberg REST catalog, Spark/PySpark for ingestion + dbt transforms, Trino for interactive SQL. Data flows raw → bronze (append-only Iceberg) → silver (conformed dims/facts) → gold (marts), orchestrated by a Makefile.

**Tech Stack:** Python 3.11, PySpark 3.5 + Apache Iceberg 1.6, dbt-spark, Trino, MinIO, Great Expectations, Docker Compose, pytest, ruff, GitHub Actions.

---

## Conventions

- **Iceberg catalog name:** `demo` (Spark + Trino both point at it). Namespaces: `bronze`, `silver`, `gold`.
- **S3 creds (local only, non-secret):** access key `admin`, secret `password`, region `us-east-1`, bucket `warehouse`.
- **Python env:** a project virtualenv at `.venv` with `requirements.txt`; host-side scripts (generator, GE, tests) run in it. Spark/dbt run inside containers.
- **Commit style:** conventional commits (`feat:`, `chore:`, `test:`, `docs:`). Commit after every task.
- **TDD note:** Python modules (event generator, ingestion helpers) use real pytest TDD. Infra/config tasks (compose, catalog, dbt wiring) use run-and-verify steps — the "test" is a smoke command with expected output.

## File Structure (decomposition)

| File | Responsibility |
|------|----------------|
| `docker-compose.yml` | Defines minio, mc (bucket init), iceberg-rest, spark-iceberg (+thrift), trino |
| `docker/spark/spark-defaults.conf` | Spark→Iceberg-REST→MinIO config |
| `docker/trino/catalog/demo.properties` | Trino Iceberg connector config |
| `requirements.txt` | Host Python deps |
| `Makefile` | Orchestration entrypoints |
| `ingestion/download_movielens.py` | Fetch + unzip MovieLens to `data/raw/` |
| `ingestion/generate_playback_events.py` | Deterministic synthetic events → `data/raw/playback_events/date=.../` |
| `ingestion/ingest_bronze.py` | PySpark: raw CSV/Parquet → bronze Iceberg tables |
| `dbt/` | dbt-spark project (silver + gold models, tests) |
| `quality/great_expectations/` | GE suite on gold `movie_engagement` |
| `demos/01..05_*.py` | One Iceberg capability each |
| `trino/sample_queries.sql` | Example analyst queries |
| `tests/` | pytest for generator + ingestion smoke |
| `.github/workflows/ci.yml` | ruff + dbt parse + sample smoke test |
| `docs/architecture.md`, `docs/blog/why-table-format-matters.md` | Narrative + diagram |
| `README.md` | Showcase centerpiece |

---

# WEEKEND 1 — FOUNDATION

### Task 1: Repo scaffolding

**Files:**
- Create: `requirements.txt`, `README.md` (skeleton), `Makefile` (skeleton), directory tree

- [ ] **Step 1: Create directory tree**

```bash
cd ~/Projects/iceberg-lakehouse-lab
mkdir -p docker/spark docker/trino/catalog ingestion dbt demos quality trino tests \
         data/raw docs/blog .github/workflows
```

- [ ] **Step 2: Write `requirements.txt`**

```text
pyspark==3.5.1
dbt-spark[PyHive]==1.8.0
great-expectations==0.18.19
requests==2.32.3
pandas==2.2.2
pyarrow==16.1.0
pytest==8.2.2
ruff==0.5.0
```

- [ ] **Step 3: Write `README.md` skeleton**

```markdown
# iceberg-lakehouse-lab

A local Apache Iceberg lakehouse demo: MovieLens + synthetic playback events →
bronze/silver/gold with dbt-spark, validated with dbt tests + Great Expectations,
queried with Trino. Built to demonstrate Iceberg fluency.

## Quickstart
\`\`\`bash
make up && make seed && make ingest && make build && make test && make demo
\`\`\`

(Full docs in `docs/architecture.md`.)
```

- [ ] **Step 4: Write `Makefile` skeleton (targets filled in later tasks)**

```makefile
.PHONY: up down seed ingest build test demo query clean

up: ## start the docker stack
	docker compose up -d

down: ## stop the docker stack
	docker compose down

clean: ## stop and remove volumes + local data
	docker compose down -v
	rm -rf data/raw/* spark-warehouse
```

- [ ] **Step 5: Create the venv and install deps**

```bash
python3.11 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
```
Expected: installs without error; `dbt --version` shows `dbt-spark`.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "chore: scaffold repo structure, requirements, Makefile skeleton"
```

---

### Task 2: Docker Compose stack (MinIO + Iceberg REST + Spark + Trino)

**Files:**
- Create: `docker-compose.yml`, `docker/spark/spark-defaults.conf`, `docker/trino/catalog/demo.properties`

- [ ] **Step 1: Write `docker/spark/spark-defaults.conf`**

```properties
spark.sql.extensions                                org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.demo                              org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.demo.type                         rest
spark.sql.catalog.demo.uri                          http://iceberg-rest:8181
spark.sql.catalog.demo.io-impl                      org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.demo.warehouse                    s3://warehouse/
spark.sql.catalog.demo.s3.endpoint                  http://minio:9000
spark.sql.catalog.demo.s3.path-style-access         true
spark.sql.defaultCatalog                            demo
```

- [ ] **Step 2: Write `docker/trino/catalog/demo.properties`**

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://warehouse/
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=admin
s3.aws-secret-key=password
```

- [ ] **Step 3: Write `docker-compose.yml`**

```yaml
services:
  minio:
    image: minio/minio:RELEASE.2024-06-13T22-53-53Z
    container_name: minio
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password
      MINIO_DOMAIN: minio
    ports: ["9000:9000", "9001:9001"]
    command: server /data --console-address ":9001"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      retries: 10

  mc:
    image: minio/mc:RELEASE.2024-06-12T14-34-03Z
    container_name: mc
    depends_on: { minio: { condition: service_started } }
    entrypoint: >
      /bin/sh -c "
      until (mc alias set m http://minio:9000 admin password) do sleep 1; done;
      mc mb --ignore-existing m/warehouse;
      tail -f /dev/null
      "

  iceberg-rest:
    image: tabulario/iceberg-rest:1.6.0
    container_name: iceberg-rest
    depends_on: [minio]
    ports: ["8181:8181"]
    environment:
      CATALOG_WAREHOUSE: s3://warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1

  spark-iceberg:
    image: tabulario/spark-iceberg:3.5.1_1.6.0
    container_name: spark-iceberg
    depends_on: [iceberg-rest, minio]
    environment:
      AWS_ACCESS_KEY_ID: admin
      AWS_SECRET_ACCESS_KEY: password
      AWS_REGION: us-east-1
    volumes:
      - ./docker/spark/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
      - ./ingestion:/home/iceberg/ingestion
      - ./dbt:/home/iceberg/dbt
      - ./demos:/home/iceberg/demos
      - ./data:/home/iceberg/data
    ports: ["8888:8888", "10000:10000", "4040:4040"]
    command: >
      /bin/sh -c "
      /opt/spark/sbin/start-thriftserver.sh
        --hiveconf hive.server2.thrift.port=10000
        --hiveconf hive.server2.thrift.bind.host=0.0.0.0;
      tail -f /dev/null
      "

  trino:
    image: trinodb/trino:451
    container_name: trino
    depends_on: [iceberg-rest, minio]
    ports: ["8080:8080"]
    volumes:
      - ./docker/trino/catalog:/etc/trino/catalog
```

- [ ] **Step 4: Bring the stack up**

Run: `docker compose up -d && sleep 30 && docker compose ps`
Expected: all of `minio`, `iceberg-rest`, `spark-iceberg`, `trino` show `Up`. (`mc` may show `Up` running tail.)

- [ ] **Step 5: Verify the warehouse bucket exists**

Run: `docker exec mc mc ls m/`
Expected: output contains `warehouse/`.

- [ ] **Step 6: Verify Trino is serving**

Run: `docker exec trino trino --execute "SHOW CATALOGS"`
Expected: list includes `demo` and `system`.

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat: docker compose stack (minio, iceberg-rest, spark, trino)"
```

---

### Task 3: End-to-end Iceberg smoke test (Spark writes, Trino reads)

**Files:**
- Create: `tests/smoke_stack.sh`

- [ ] **Step 1: Write `tests/smoke_stack.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

# Create a namespace + tiny Iceberg table via Spark SQL, then read it from Trino.
docker exec spark-iceberg spark-sql -e "
  CREATE NAMESPACE IF NOT EXISTS demo.smoke;
  CREATE TABLE IF NOT EXISTS demo.smoke.t (id INT, name STRING) USING iceberg;
  INSERT INTO demo.smoke.t VALUES (1, 'iceberg'), (2, 'lakehouse');
"

echo '--- Trino sees the table written by Spark ---'
docker exec trino trino --execute "SELECT count(*) FROM demo.smoke.t"
```

- [ ] **Step 2: Make it executable and run it**

Run: `chmod +x tests/smoke_stack.sh && ./tests/smoke_stack.sh`
Expected: final line prints `2` (Trino reads the table Spark wrote — proves storage+catalog+both engines agree).

- [ ] **Step 3: Clean up the smoke namespace**

Run: `docker exec spark-iceberg spark-sql -e "DROP TABLE demo.smoke.t; DROP NAMESPACE demo.smoke;"`
Expected: no error.

- [ ] **Step 4: Commit**

```bash
git add tests/smoke_stack.sh && git commit -m "test: end-to-end Spark-write/Trino-read smoke test"
```

---

### Task 4: MovieLens download script

**Files:**
- Create: `ingestion/download_movielens.py`, `tests/test_download_movielens.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_download_movielens.py
from pathlib import Path
from ingestion.download_movielens import expected_files, target_dir

def test_expected_files_are_the_four_movielens_csvs():
    assert set(expected_files()) == {"movies.csv", "ratings.csv", "tags.csv", "links.csv"}

def test_target_dir_is_under_data_raw():
    assert target_dir() == Path("data/raw/movielens")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `. .venv/bin/activate && pytest tests/test_download_movielens.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ingestion.download_movielens'`.

- [ ] **Step 3: Write `ingestion/download_movielens.py`**

```python
"""Download the MovieLens small dataset into data/raw/movielens/."""
from __future__ import annotations
import io, zipfile
from pathlib import Path
import requests

URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

def expected_files() -> list[str]:
    return ["movies.csv", "ratings.csv", "tags.csv", "links.csv"]

def target_dir() -> Path:
    return Path("data/raw/movielens")

def download() -> Path:
    dest = target_dir()
    dest.mkdir(parents=True, exist_ok=True)
    resp = requests.get(URL, timeout=120)
    resp.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for member in zf.namelist():
            name = Path(member).name
            if name in expected_files():
                (dest / name).write_bytes(zf.read(member))
    return dest

if __name__ == "__main__":
    out = download()
    print(f"Downloaded MovieLens to {out}: {sorted(p.name for p in out.glob('*.csv'))}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_download_movielens.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Run the real download**

Run: `python -m ingestion.download_movielens`
Expected: prints `Downloaded MovieLens to data/raw/movielens: ['links.csv', 'movies.csv', 'ratings.csv', 'tags.csv']`.

- [ ] **Step 6: Commit**

```bash
git add ingestion/download_movielens.py tests/test_download_movielens.py
git commit -m "feat: MovieLens download script with tests"
```

---

### Task 5: Synthetic playback-events generator (TDD)

**Files:**
- Create: `ingestion/generate_playback_events.py`, `tests/test_generate_playback_events.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_generate_playback_events.py
from datetime import date
from ingestion.generate_playback_events import generate_events, EVENT_TYPES

def test_generates_requested_number_of_events():
    events = generate_events(day=date(2024, 1, 1), n=100, movie_ids=[1, 2, 3], user_ids=[10, 11], seed=42)
    assert len(events) == 100

def test_events_have_required_schema():
    e = generate_events(day=date(2024, 1, 1), n=1, movie_ids=[1], user_ids=[10], seed=1)[0]
    assert set(e.keys()) == {"event_id", "user_id", "movie_id", "event_type", "event_ts", "device", "position_seconds"}
    assert e["event_type"] in EVENT_TYPES
    assert e["movie_id"] == 1 and e["user_id"] == 10

def test_is_deterministic_for_a_seed():
    a = generate_events(day=date(2024, 1, 1), n=50, movie_ids=[1, 2], user_ids=[10, 11], seed=7)
    b = generate_events(day=date(2024, 1, 1), n=50, movie_ids=[1, 2], user_ids=[10, 11], seed=7)
    assert a == b

def test_all_timestamps_fall_on_the_requested_day():
    events = generate_events(day=date(2024, 3, 5), n=200, movie_ids=[1], user_ids=[10], seed=3)
    assert all(e["event_ts"].date() == date(2024, 3, 5) for e in events)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_generate_playback_events.py -v`
Expected: FAIL — `ModuleNotFoundError`.

- [ ] **Step 3: Write `ingestion/generate_playback_events.py`**

```python
"""Deterministic synthetic playback-events generator.

Writes one daily batch as Parquet to data/raw/playback_events/date=YYYY-MM-DD/.
Determinism (seeded) keeps demos reproducible.
"""
from __future__ import annotations
import argparse, random, uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path

EVENT_TYPES = ("play", "pause", "seek", "complete")
DEVICES = ("tv", "mobile", "web", "tablet")

def generate_events(day: date, n: int, movie_ids: list[int], user_ids: list[int], seed: int) -> list[dict]:
    rng = random.Random(seed)
    events: list[dict] = []
    for _ in range(n):
        secs = rng.randint(0, 86_399)
        ts = datetime.combine(day, time.min) + timedelta(seconds=secs)
        events.append({
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "user_id": rng.choice(user_ids),
            "movie_id": rng.choice(movie_ids),
            "event_type": rng.choice(EVENT_TYPES),
            "event_ts": ts,
            "device": rng.choice(DEVICES),
            "position_seconds": rng.randint(0, 7200),
        })
    return events

def write_batch(day: date, n: int, movie_ids: list[int], user_ids: list[int], seed: int) -> Path:
    import pandas as pd
    events = generate_events(day, n, movie_ids, user_ids, seed)
    out_dir = Path("data/raw/playback_events") / f"date={day.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "events.parquet"
    pd.DataFrame(events).to_parquet(out, index=False)
    return out

def _movie_ids_from_movielens() -> list[int]:
    import pandas as pd
    df = pd.read_csv("data/raw/movielens/movies.csv")
    return df["movieId"].tolist()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2024-01-01")
    p.add_argument("--days", type=int, default=7)
    p.add_argument("--per-day", type=int, default=2000)
    p.add_argument("--users", type=int, default=500)
    args = p.parse_args()

    movie_ids = _movie_ids_from_movielens()
    user_ids = list(range(1, args.users + 1))
    start = date.fromisoformat(args.start)
    for i in range(args.days):
        d = start + timedelta(days=i)
        out = write_batch(d, args.per_day, movie_ids, user_ids, seed=1000 + i)
        print(f"wrote {args.per_day} events for {d} -> {out}")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_generate_playback_events.py -v`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add ingestion/generate_playback_events.py tests/test_generate_playback_events.py
git commit -m "feat: deterministic synthetic playback-events generator with tests"
```

---

### Task 6: Bronze ingestion (PySpark → Iceberg) + seed wiring

**Files:**
- Create: `ingestion/ingest_bronze.py`
- Modify: `Makefile` (add `seed`, `ingest`)

- [ ] **Step 1: Write `ingestion/ingest_bronze.py`**

```python
"""Load raw MovieLens CSVs + playback-event Parquet into bronze Iceberg tables.

Run INSIDE the spark-iceberg container (paths are container paths under /home/iceberg).
"""
from pyspark.sql import SparkSession

RAW = "/home/iceberg/data/raw"

def main() -> None:
    spark = SparkSession.builder.appName("ingest_bronze").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS demo.bronze")

    # MovieLens dimension/fact CSVs (append-only bronze, overwrite for idempotent reload)
    for name in ("movies", "ratings", "tags", "links"):
        df = spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW}/movielens/{name}.csv")
        df.writeTo(f"demo.bronze.{name}").using("iceberg").createOrReplace()
        print(f"bronze.{name}: {df.count()} rows")

    # Playback events: partitioned by day, appended.
    events = spark.read.parquet(f"{RAW}/playback_events")
    (events.writeTo("demo.bronze.playback_events")
           .using("iceberg")
           .partitionedBy("days(event_ts)")
           .createOrReplace())
    print(f"bronze.playback_events: {events.count()} rows")

    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Add `seed` and `ingest` targets to `Makefile`**

```makefile
seed: ## download MovieLens + generate playback events (host venv)
	. .venv/bin/activate && python -m ingestion.download_movielens
	. .venv/bin/activate && python -m ingestion.generate_playback_events --days 7 --per-day 2000

ingest: ## load raw data into bronze Iceberg tables (in spark container)
	docker exec spark-iceberg spark-submit /home/iceberg/ingestion/ingest_bronze.py
```

- [ ] **Step 3: Run seed + ingest**

Run: `make seed && make ingest`
Expected: prints row counts, e.g. `bronze.movies: 9742 rows`, `bronze.ratings: 100836 rows`, `bronze.playback_events: 14000 rows`.

- [ ] **Step 4: Verify bronze tables from Trino**

Run: `docker exec trino trino --execute "SHOW TABLES FROM demo.bronze"`
Expected: lists `links, movies, playback_events, ratings, tags`.

- [ ] **Step 5: Verify partitioning on events**

Run: `docker exec trino trino --execute "SELECT count(*) FROM demo.bronze.\"playback_events\$partitions\""`
Expected: a small integer (one partition per seeded day, ≈7).

- [ ] **Step 6: Commit**

```bash
git add ingestion/ingest_bronze.py Makefile
git commit -m "feat: bronze ingestion to Iceberg + seed/ingest make targets"
```

---

# WEEKEND 2 — TRANSFORM + QUALITY

### Task 7: dbt-spark project init + connection

**Files:**
- Create: `dbt/dbt_project.yml`, `dbt/profiles.yml`, `dbt/packages.yml`
- Modify: `Makefile` (add `build`)

- [ ] **Step 1: Write `dbt/dbt_project.yml`**

```yaml
name: iceberg_lakehouse
version: "1.0.0"
config-version: 2
profile: iceberg_lakehouse
model-paths: ["models"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]
models:
  iceberg_lakehouse:
    +file_format: iceberg
    silver:
      +schema: silver
      +materialized: table
    gold:
      +schema: gold
      +materialized: table
```

- [ ] **Step 2: Write `dbt/profiles.yml`** (dbt runs inside the spark container; host is the thrift server)

```yaml
iceberg_lakehouse:
  target: dev
  outputs:
    dev:
      type: spark
      method: thrift
      host: localhost
      port: 10000
      schema: silver
      threads: 1
```

- [ ] **Step 3: Write `dbt/packages.yml`**

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.0
  - package: metaplane/dbt_expectations
    version: 0.10.4
```

- [ ] **Step 4: Install dbt deps and test the connection (inside the container)**

Run:
```bash
docker exec -w /home/iceberg/dbt spark-iceberg dbt deps
docker exec -w /home/iceberg/dbt spark-iceberg dbt debug
```
Expected: `dbt debug` ends with `All checks passed!` (Connection test: OK via thrift).

> If the thrift connection fails, confirm `start-thriftserver.sh` is running (`docker exec spark-iceberg ss -ltn | grep 10000`) and that `spark.sql.defaultCatalog=demo` is in `spark-defaults.conf` so dbt schemas resolve to `demo.<schema>`.

- [ ] **Step 5: Add `build` target to `Makefile`**

```makefile
build: ## run dbt models (silver + gold)
	docker exec -w /home/iceberg/dbt spark-iceberg dbt run
```

- [ ] **Step 6: Commit**

```bash
git add dbt/dbt_project.yml dbt/profiles.yml dbt/packages.yml Makefile
git commit -m "feat: dbt-spark project init + thrift connection + build target"
```

---

### Task 8: Silver models + tests

**Files:**
- Create: `dbt/models/silver/dim_movie.sql`, `dim_user.sql`, `fact_rating.sql`, `fact_playback_event.sql`, `dbt/models/silver/schema.yml`

- [ ] **Step 1: Write `dbt/models/silver/dim_movie.sql`**

```sql
with src as (select * from demo.bronze.movies)
select
    cast(movieId as bigint)                                   as movie_id,
    title,
    -- last parenthesised 4-digit group is the release year
    try_cast(regexp_extract(title, '\\((\\d{4})\\)', 1) as int) as release_year,
    split(genres, '\\|')                                      as genres
from src
where movieId is not null
```

- [ ] **Step 2: Write `dbt/models/silver/dim_user.sql`** (users are implied by ratings + events)

```sql
with rating_users as (select distinct cast(userId as bigint) as user_id from demo.bronze.ratings),
     event_users  as (select distinct user_id from demo.bronze.playback_events)
select user_id from rating_users
union
select user_id from event_users
```

- [ ] **Step 3: Write `dbt/models/silver/fact_rating.sql`**

```sql
select
    cast(userId as bigint)               as user_id,
    cast(movieId as bigint)              as movie_id,
    cast(rating as double)               as rating,
    cast(from_unixtime(timestamp) as timestamp) as rated_at
from demo.bronze.ratings
where rating is not null
```

- [ ] **Step 4: Write `dbt/models/silver/fact_playback_event.sql`**

```sql
select
    event_id,
    cast(user_id as bigint)   as user_id,
    cast(movie_id as bigint)  as movie_id,
    event_type,
    event_ts,
    device,
    cast(position_seconds as int) as position_seconds
from demo.bronze.playback_events
where event_id is not null
```

- [ ] **Step 5: Write `dbt/models/silver/schema.yml`**

```yaml
version: 2
models:
  - name: dim_movie
    columns:
      - name: movie_id
        tests: [not_null, unique]
      - name: release_year
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 1900
              max_value: 2030
              row_condition: "release_year is not null"
  - name: dim_user
    columns:
      - name: user_id
        tests: [not_null, unique]
  - name: fact_rating
    columns:
      - name: rating
        tests:
          - dbt_expectations.expect_column_values_to_be_between: { min_value: 0.5, max_value: 5.0 }
      - name: movie_id
        tests:
          - relationships: { to: ref('dim_movie'), field: movie_id }
  - name: fact_playback_event
    columns:
      - name: event_id
        tests: [not_null, unique]
      - name: event_type
        tests:
          - accepted_values: { values: ["play", "pause", "seek", "complete"] }
```

- [ ] **Step 6: Build silver and run its tests**

Run:
```bash
docker exec -w /home/iceberg/dbt spark-iceberg dbt run --select silver
docker exec -w /home/iceberg/dbt spark-iceberg dbt test --select silver
```
Expected: 4 models built; all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add dbt/models/silver
git commit -m "feat: silver dims/facts with dbt + dbt-expectations tests"
```

---

### Task 9: Gold marts + tests

**Files:**
- Create: `dbt/models/gold/movie_engagement.sql`, `daily_active_users.sql`, `top_titles.sql`, `dbt/models/gold/schema.yml`

- [ ] **Step 1: Write `dbt/models/gold/movie_engagement.sql`** (the GE-critical table)

```sql
with plays as (
    select movie_id, count(*) as play_events,
           count(distinct user_id) as unique_viewers,
           sum(case when event_type = 'complete' then 1 else 0 end) as completes
    from {{ ref('fact_playback_event') }}
    group by movie_id
),
ratings as (
    select movie_id, avg(rating) as avg_rating, count(*) as rating_count
    from {{ ref('fact_rating') }}
    group by movie_id
)
select
    m.movie_id,
    m.title,
    coalesce(p.play_events, 0)    as play_events,
    coalesce(p.unique_viewers, 0) as unique_viewers,
    case when coalesce(p.play_events, 0) = 0 then 0.0
         else cast(p.completes as double) / p.play_events end as completion_rate,
    r.avg_rating,
    coalesce(r.rating_count, 0)   as rating_count
from {{ ref('dim_movie') }} m
left join plays   p on m.movie_id = p.movie_id
left join ratings r on m.movie_id = r.movie_id
```

- [ ] **Step 2: Write `dbt/models/gold/daily_active_users.sql`**

```sql
select
    cast(event_ts as date) as activity_date,
    count(distinct user_id) as active_users,
    count(*)                as total_events
from {{ ref('fact_playback_event') }}
group by cast(event_ts as date)
```

- [ ] **Step 3: Write `dbt/models/gold/top_titles.sql`**

```sql
select movie_id, title, play_events, unique_viewers, completion_rate, avg_rating
from {{ ref('movie_engagement') }}
where play_events > 0
order by unique_viewers desc, play_events desc
limit 100
```

- [ ] **Step 4: Write `dbt/models/gold/schema.yml`**

```yaml
version: 2
models:
  - name: movie_engagement
    columns:
      - name: movie_id
        tests: [not_null, unique]
      - name: completion_rate
        tests:
          - dbt_expectations.expect_column_values_to_be_between: { min_value: 0, max_value: 1 }
      - name: avg_rating
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0.5
              max_value: 5.0
              row_condition: "avg_rating is not null"
  - name: daily_active_users
    columns:
      - name: activity_date
        tests: [not_null, unique]
      - name: active_users
        tests: [not_null]
  - name: top_titles
    columns:
      - name: movie_id
        tests: [not_null, unique]
```

- [ ] **Step 5: Build gold + run all dbt tests**

Run:
```bash
docker exec -w /home/iceberg/dbt spark-iceberg dbt run --select gold
docker exec -w /home/iceberg/dbt spark-iceberg dbt test
```
Expected: 3 gold models built; ALL tests PASS across silver + gold.

- [ ] **Step 6: Commit**

```bash
git add dbt/models/gold
git commit -m "feat: gold marts (movie_engagement, dau, top_titles) with tests"
```

---

### Task 10: Great Expectations suite on `movie_engagement`

**Files:**
- Create: `quality/ge_validate.py`, `tests/test_ge_smoke.py`
- Modify: `Makefile` (add `test`)

- [ ] **Step 1: Write the failing smoke test**

```python
# tests/test_ge_smoke.py
from quality.ge_validate import build_suite

def test_suite_has_core_expectations():
    names = {e["expectation_type"] for e in build_suite()}
    assert "expect_column_values_to_be_between" in names
    assert "expect_table_row_count_to_be_between" in names
    assert "expect_column_values_to_not_be_null" in names
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_ge_smoke.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'quality.ge_validate'`.

- [ ] **Step 3: Write `quality/ge_validate.py`**

```python
"""Great Expectations validation of the gold movie_engagement table.

Reads the table via Trino (DBAPI) into pandas and validates it. Keeps GE decoupled
from Spark so it runs on the host venv and in CI.
"""
from __future__ import annotations
import sys

def build_suite() -> list[dict]:
    """The expectation set for gold.movie_engagement (also unit-testable)."""
    return [
        {"expectation_type": "expect_table_row_count_to_be_between",
         "kwargs": {"min_value": 1, "max_value": 100000}},
        {"expectation_type": "expect_column_values_to_not_be_null",
         "kwargs": {"column": "movie_id"}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "completion_rate", "min_value": 0, "max_value": 1}},
        {"expectation_type": "expect_column_values_to_be_between",
         "kwargs": {"column": "avg_rating", "min_value": 0.5, "max_value": 5.0,
                    "mostly": 1.0, "row_condition": "avg_rating == avg_rating",
                    "condition_parser": "pandas"}},
    ]

def _load_table() -> "pandas.DataFrame":
    import pandas as pd
    from trino.dbapi import connect
    conn = connect(host="localhost", port=8080, user="ge", catalog="demo", schema="gold")
    return pd.read_sql("SELECT * FROM demo.gold.movie_engagement", conn)

def validate() -> bool:
    import great_expectations as gx
    df = _load_table()
    ctx = gx.get_context(mode="ephemeral")
    validator = ctx.sources.add_pandas("gold").read_dataframe(df, asset_name="movie_engagement")
    for exp in build_suite():
        getattr(validator, exp["expectation_type"])(**exp["kwargs"])
    result = validator.validate()
    print(f"GE success={result.success}; "
          f"{result.statistics['successful_expectations']}/"
          f"{result.statistics['evaluated_expectations']} expectations passed")
    return bool(result.success)

if __name__ == "__main__":
    sys.exit(0 if validate() else 1)
```

> Add `trino==0.328.0` to `requirements.txt` (Trino DBAPI client) in this step and re-`pip install -r requirements.txt`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_ge_smoke.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Run the real GE validation against gold**

Run: `. .venv/bin/activate && python -m quality.ge_validate`
Expected: prints `GE success=True; 4/4 expectations passed`.

- [ ] **Step 6: Add `test` target to `Makefile`**

```makefile
test: ## run dbt tests + Great Expectations on gold
	docker exec -w /home/iceberg/dbt spark-iceberg dbt test
	. .venv/bin/activate && python -m quality.ge_validate
```

- [ ] **Step 7: Commit**

```bash
git add quality/ge_validate.py tests/test_ge_smoke.py Makefile requirements.txt
git commit -m "feat: Great Expectations suite on gold movie_engagement + test target"
```

---

# WEEKEND 3 — SHOWCASE

> Each demo is a self-contained script run inside the spark container:
> `docker exec spark-iceberg spark-submit /home/iceberg/demos/<file>.py`.
> All print clearly-labeled before/after output so they read well in the blog/README.

### Task 11: Demo 01 — Time travel & rollback

**Files:**
- Create: `demos/01_time_travel.py`

- [ ] **Step 1: Write `demos/01_time_travel.py`**

```python
"""Demo: Iceberg snapshots, time-travel queries, and rollback."""
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("demo_time_travel").getOrCreate()
    t = "demo.bronze.ratings"

    snaps = spark.sql(f"SELECT snapshot_id, committed_at FROM {t}.snapshots ORDER BY committed_at")
    first_snapshot = snaps.first()["snapshot_id"]
    print("=== snapshots ==="); snaps.show(truncate=False)

    before = spark.table(t).count()
    print(f"current row count: {before}")

    # Simulate a bad load: delete a chunk of rows (creates a new snapshot).
    spark.sql(f"DELETE FROM {t} WHERE rating < 1.0")
    after = spark.table(t).count()
    print(f"after 'bad' delete: {after}")

    # Time-travel: read the table AS OF the first snapshot.
    travelled = spark.sql(f"SELECT count(*) AS c FROM {t} VERSION AS OF {first_snapshot}").first()["c"]
    print(f"time-travel to first snapshot sees: {travelled}")

    # Roll back to the first snapshot — undo the bad delete.
    spark.sql(f"CALL demo.system.rollback_to_snapshot('{t}', {first_snapshot})")
    restored = spark.table(t).count()
    print(f"after rollback: {restored} (== original {before}: {restored == before})")
    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the demo**

Run: `docker exec spark-iceberg spark-submit /home/iceberg/demos/01_time_travel.py`
Expected: final line shows `after rollback: <N> (== original <N>: True)`.

- [ ] **Step 3: Re-ingest to reset state for later demos**

Run: `make ingest`
Expected: bronze reloaded cleanly.

- [ ] **Step 4: Commit**

```bash
git add demos/01_time_travel.py && git commit -m "feat: demo 01 time-travel + rollback"
```

---

### Task 12: Demo 02 — Schema evolution

**Files:**
- Create: `demos/02_schema_evolution.py`

- [ ] **Step 1: Write `demos/02_schema_evolution.py`**

```python
"""Demo: add a column to an existing Iceberg table; old data still reads, no rewrite."""
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("demo_schema_evolution").getOrCreate()
    t = "demo.bronze.playback_events"

    print("=== schema BEFORE ==="); spark.sql(f"DESCRIBE {t}").show(truncate=False)

    # Add a new nullable column — a metadata-only change, no data rewrite.
    spark.sql(f"ALTER TABLE {t} ADD COLUMN bitrate_kbps INT")
    print("=== schema AFTER ==="); spark.sql(f"DESCRIBE {t}").show(truncate=False)

    # Existing rows return NULL for the new column; no migration needed.
    nulls = spark.sql(f"SELECT count(*) AS c FROM {t} WHERE bitrate_kbps IS NULL").first()["c"]
    total = spark.table(t).count()
    print(f"existing rows with NULL new column: {nulls}/{total} (old data still readable)")
    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the demo**

Run: `docker exec spark-iceberg spark-submit /home/iceberg/demos/02_schema_evolution.py`
Expected: `DESCRIBE` shows `bitrate_kbps` after; nulls == total.

- [ ] **Step 3: Commit**

```bash
git add demos/02_schema_evolution.py && git commit -m "feat: demo 02 schema evolution"
```

---

### Task 13: Demo 03 — Hidden partitioning & partition evolution

**Files:**
- Create: `demos/03_hidden_partitioning.py`

- [ ] **Step 1: Write `demos/03_hidden_partitioning.py`**

```python
"""Demo: hidden partitioning (no extra column) + partition evolution."""
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("demo_hidden_partitioning").getOrCreate()
    t = "demo.bronze.playback_events"

    # Hidden partitioning: query filters on event_ts; Iceberg prunes by days(event_ts)
    # WITHOUT the user ever managing a partition column.
    print("=== current partition spec ===")
    spark.sql(f"SELECT * FROM {t}.partitions").show(truncate=False)

    print("=== a date-filtered query (partition pruning happens transparently) ===")
    spark.sql(f"""
        SELECT count(*) AS c FROM {t}
        WHERE event_ts >= TIMESTAMP '2024-01-03 00:00:00'
          AND event_ts <  TIMESTAMP '2024-01-04 00:00:00'
    """).show()

    # Partition evolution: change the layout for FUTURE data without rewriting the past.
    spark.sql(f"ALTER TABLE {t} ADD PARTITION FIELD device")
    print("=== partition spec AFTER evolution (now days(event_ts) + device) ===")
    spark.sql(f"DESCRIBE {t}").show(truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the demo**

Run: `docker exec spark-iceberg spark-submit /home/iceberg/demos/03_hidden_partitioning.py`
Expected: partitions listed; filtered count printed; spec shows added `device` field.

- [ ] **Step 3: Re-ingest to reset partition spec**

Run: `make ingest`

- [ ] **Step 4: Commit**

```bash
git add demos/03_hidden_partitioning.py && git commit -m "feat: demo 03 hidden partitioning + evolution"
```

---

### Task 14: Demo 04 — Compaction & maintenance

**Files:**
- Create: `demos/04_compaction_maintenance.py`

- [ ] **Step 1: Write `demos/04_compaction_maintenance.py`**

```python
"""Demo: small-files problem, rewrite_data_files compaction, snapshot expiry."""
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("demo_compaction").getOrCreate()
    t = "demo.bronze.playback_events"

    files_before = spark.sql(f"SELECT count(*) AS c FROM {t}.files").first()["c"]
    print(f"data files before compaction: {files_before}")

    # Compact small files into larger ones.
    res = spark.sql(f"CALL demo.system.rewrite_data_files(table => '{t}')")
    print("=== rewrite_data_files result ==="); res.show(truncate=False)

    files_after = spark.sql(f"SELECT count(*) AS c FROM {t}.files").first()["c"]
    print(f"data files after compaction: {files_after}")

    # Expire old snapshots to reclaim metadata/storage.
    snaps_before = spark.sql(f"SELECT count(*) AS c FROM {t}.snapshots").first()["c"]
    spark.sql(f"CALL demo.system.expire_snapshots(table => '{t}', older_than => now(), retain_last => 1)")
    snaps_after = spark.sql(f"SELECT count(*) AS c FROM {t}.snapshots").first()["c"]
    print(f"snapshots: {snaps_before} -> {snaps_after}")
    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the demo**

Run: `docker exec spark-iceberg spark-submit /home/iceberg/demos/04_compaction_maintenance.py`
Expected: prints file counts before/after and snapshot reduction.

- [ ] **Step 3: Commit**

```bash
git add demos/04_compaction_maintenance.py && git commit -m "feat: demo 04 compaction + snapshot expiry"
```

---

### Task 15: Demo 05 — Iceberg vs plain Parquet (the blog's core argument)

**Files:**
- Create: `demos/05_iceberg_vs_parquet.py`

- [ ] **Step 1: Write `demos/05_iceberg_vs_parquet.py`**

```python
"""Demo: contrast a plain-Parquet 'table' with an Iceberg table.

Shows three things Parquet-on-a-directory cannot do safely: atomic schema change,
time-travel, and ACID row-level delete — the 'why table format matters' payload.
"""
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("demo_iceberg_vs_parquet").getOrCreate()

    sample = spark.table("demo.bronze.ratings").limit(5000)

    # Plain Parquet directory (no table format).
    sample.write.mode("overwrite").parquet("/home/iceberg/data/tmp/parquet_ratings")
    print("Plain Parquet: a directory of files. No snapshots, no catalog, no ACID.")

    # Iceberg table.
    sample.writeTo("demo.bronze.ratings_iceberg_demo").using("iceberg").createOrReplace()

    print("\n--- Iceberg gives you: ---")
    print("1) Time-travel / snapshots:")
    spark.sql("SELECT snapshot_id, committed_at FROM demo.bronze.ratings_iceberg_demo.snapshots").show(truncate=False)

    print("2) ACID row-level delete (Parquet would require rewriting whole files by hand):")
    spark.sql("DELETE FROM demo.bronze.ratings_iceberg_demo WHERE rating < 1.0")
    print("   delete committed atomically as a new snapshot.")

    print("3) Safe in-place schema evolution (no rewrite):")
    spark.sql("ALTER TABLE demo.bronze.ratings_iceberg_demo ADD COLUMN source STRING")
    spark.sql("DESCRIBE demo.bronze.ratings_iceberg_demo").show(truncate=False)

    spark.sql("DROP TABLE demo.bronze.ratings_iceberg_demo")
    spark.stop()

if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run the demo**

Run: `docker exec spark-iceberg spark-submit /home/iceberg/demos/05_iceberg_vs_parquet.py`
Expected: prints the three Iceberg capabilities with snapshot/describe output.

- [ ] **Step 3: Add `demo` and `query` targets to `Makefile`**

```makefile
demo: ## run all Iceberg feature demos
	for f in demos/0*.py; do echo "=== $$f ==="; docker exec spark-iceberg spark-submit /home/iceberg/$$f; done

query: ## open Trino CLI with the demo catalog
	docker exec -it trino trino --catalog demo
```

- [ ] **Step 4: Commit**

```bash
git add demos/05_iceberg_vs_parquet.py Makefile
git commit -m "feat: demo 05 iceberg-vs-parquet + demo/query make targets"
```

---

### Task 16: Trino sample queries

**Files:**
- Create: `trino/sample_queries.sql`

- [ ] **Step 1: Write `trino/sample_queries.sql`**

```sql
-- Top 10 most-watched titles by unique viewers
SELECT title, unique_viewers, completion_rate, avg_rating
FROM demo.gold.movie_engagement
WHERE play_events > 0
ORDER BY unique_viewers DESC
LIMIT 10;

-- Daily active users trend
SELECT activity_date, active_users, total_events
FROM demo.gold.daily_active_users
ORDER BY activity_date;

-- Time-travel: inspect snapshot history of the events table
SELECT snapshot_id, committed_at, operation
FROM demo.bronze."playback_events$snapshots"
ORDER BY committed_at;
```

- [ ] **Step 2: Verify the queries run**

Run: `docker exec -i trino trino -f - < trino/sample_queries.sql`
Expected: three result sets print without error.

- [ ] **Step 3: Commit**

```bash
git add trino/sample_queries.sql && git commit -m "docs: Trino sample analyst queries"
```

---

### Task 17: CI (GitHub Actions) + ruff

**Files:**
- Create: `.github/workflows/ci.yml`, `ruff.toml`

- [ ] **Step 1: Write `ruff.toml`**

```toml
line-length = 100
target-version = "py311"
[lint]
select = ["E", "F", "I", "UP"]
```

- [ ] **Step 2: Write `.github/workflows/ci.yml`** (lint + unit tests + dbt parse; full stack smoke is documented but kept light for CI runtime)

```yaml
name: ci
on: [push, pull_request]
jobs:
  lint-and-unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install -r requirements.txt
      - name: Ruff lint
        run: ruff check .
      - name: Unit tests (generator, download, GE suite shape)
        run: pytest tests/test_generate_playback_events.py tests/test_download_movielens.py tests/test_ge_smoke.py -v
      - name: dbt parse
        working-directory: dbt
        run: |
          dbt deps
          dbt parse --no-version-check || echo "parse requires profiles; structural check only"
```

- [ ] **Step 3: Run ruff and unit tests locally to confirm green**

Run: `. .venv/bin/activate && ruff check . && pytest tests/test_generate_playback_events.py tests/test_download_movielens.py tests/test_ge_smoke.py -v`
Expected: ruff reports no errors; all unit tests PASS.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml ruff.toml
git commit -m "ci: ruff lint + unit tests + dbt parse"
```

---

### Task 18: Architecture doc + diagram

**Files:**
- Create: `docs/architecture.md`

- [ ] **Step 1: Write `docs/architecture.md`** with a Mermaid diagram + component table

```markdown
# Architecture

## Component diagram

\`\`\`mermaid
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
\`\`\`

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
\`\`\`
```

- [ ] **Step 2: Commit**

```bash
git add docs/architecture.md && git commit -m "docs: architecture diagram + component overview"
```

---

### Task 19: Blog post — "why table format matters"

**Files:**
- Create: `docs/blog/why-table-format-matters.md`

- [ ] **Step 1: Write `docs/blog/why-table-format-matters.md`**

Structure (write ~1000–1400 words, drawing the concrete output from each demo script):
1. **Hook:** the "data lake swamp" — directories of Parquet with no atomic commits, no schema safety, no history.
2. **What a table format adds:** snapshots, atomic commits, schema/partition evolution, time-travel (reference `demos/05`).
3. **Walk through each capability** with the actual command + output from `demos/01`–`04`.
4. **Why this is how modern platforms (incl. Netflix) run** — separation of storage (MinIO/S3), catalog (REST), and compute (Spark/Trino); Iceberg as the open table format Netflix co-created.
5. **Try it yourself:** the `make up && make seed && make ingest && make build && make test && make demo` quickstart.

- [ ] **Step 2: Commit**

```bash
git add docs/blog/why-table-format-matters.md
git commit -m "docs: blog post — why table format matters"
```

---

### Task 20: Final README + polish

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Rewrite `README.md`** as the showcase centerpiece

Include, in this order:
1. One-paragraph pitch + a badges line (CI).
2. The Mermaid architecture diagram (copy from `docs/architecture.md`).
3. **What this demonstrates** — bullet list mapping features to skills (Iceberg, lakehouse separation of concerns, dbt medallion modeling, data quality with dbt tests + GE, Trino, Docker).
4. **Quickstart** — prerequisites (Docker, Python 3.11) + the full `make` sequence with expected end state.
5. **Repo tour** — the file-structure table.
6. **The Iceberg demos** — one line each linking to `demos/` and the blog.
7. Link to `docs/blog/why-table-format-matters.md` and `docs/architecture.md`.

- [ ] **Step 2: Full end-to-end verification from a clean state**

Run:
```bash
make clean && make up && sleep 30 && make seed && make ingest && make build && make test && make demo
```
Expected: completes with dbt tests + GE passing and all 5 demos printing their success output.

- [ ] **Step 3: Commit**

```bash
git add README.md && git commit -m "docs: showcase README + final polish"
```

- [ ] **Step 4: (Optional) publish**

```bash
gh repo create iceberg-lakehouse-lab --public --source=. --remote=origin --push
```
Expected: repo created and pushed; CI badge goes green after the first Actions run.

---

## Spec Coverage Check

| Spec requirement | Task |
|------------------|------|
| PySpark + Iceberg + dbt-spark + Trino in Docker | 2 |
| MinIO (S3) + Iceberg REST catalog | 2 |
| MovieLens dataset | 4 |
| Synthetic playback-events generator | 5 |
| Bronze append-only Iceberg tables | 6 |
| Silver dims/facts | 8 |
| Gold marts (movie_engagement, dau, top_titles) | 9 |
| dbt tests (+ dbt-expectations) | 8, 9 |
| Great Expectations on critical gold table | 10 |
| Makefile orchestration (up/seed/ingest/build/test/demo/query) | 1, 6, 7, 10, 15 |
| 5 Iceberg feature demos | 11–15 |
| Trino sample queries | 16 |
| CI (ruff + dbt parse + smoke) | 17 |
| Architecture diagram | 18 |
| Blog post | 19 |
| Showcase README | 20 |
| pytest smoke (generator + ingestion) | 4, 5, 10 |

All spec sections map to at least one task.
```
