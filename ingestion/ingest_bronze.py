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
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(f"{RAW}/movielens/{name}.csv")
        )
        df.writeTo(f"demo.bronze.{name}").using("iceberg").createOrReplace()
        print(f"bronze.{name}: {df.count()} rows")

    # Playback events: partitioned by day, appended.
    events = spark.read.parquet(f"{RAW}/playback_events")
    (
        events.writeTo("demo.bronze.playback_events")
        .using("iceberg")
        .partitionedBy("days(event_ts)")
        .createOrReplace()
    )
    print(f"bronze.playback_events: {events.count()} rows")

    spark.stop()


if __name__ == "__main__":
    main()
