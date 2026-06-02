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
    spark.sql(
        f"""
        SELECT count(*) AS c FROM {t}
        WHERE event_ts >= TIMESTAMP '2024-01-03 00:00:00'
          AND event_ts <  TIMESTAMP '2024-01-04 00:00:00'
    """
    ).show()

    # Partition evolution: change the layout for FUTURE data without rewriting the past.
    spark.sql(f"ALTER TABLE {t} ADD PARTITION FIELD device")
    print("=== partition spec AFTER evolution (now days(event_ts) + device) ===")
    spark.sql(f"DESCRIBE {t}").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
