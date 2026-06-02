"""Demo: add a column to an existing Iceberg table; old data still reads, no rewrite."""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("demo_schema_evolution").getOrCreate()
    t = "demo.bronze.playback_events"

    print("=== schema BEFORE ===")
    spark.sql(f"DESCRIBE {t}").show(truncate=False)

    # Add a new nullable column — a metadata-only change, no data rewrite.
    spark.sql(f"ALTER TABLE {t} ADD COLUMN bitrate_kbps INT")
    print("=== schema AFTER ===")
    spark.sql(f"DESCRIBE {t}").show(truncate=False)

    # Existing rows return NULL for the new column; no migration needed.
    nulls = spark.sql(
        f"SELECT count(*) AS c FROM {t} WHERE bitrate_kbps IS NULL"
    ).first()["c"]
    total = spark.table(t).count()
    print(f"existing rows with NULL new column: {nulls}/{total} (old data still readable)")
    spark.stop()


if __name__ == "__main__":
    main()
