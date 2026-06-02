"""Demo: Iceberg snapshots, time-travel queries, and rollback."""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("demo_time_travel").getOrCreate()
    t = "demo.bronze.ratings"

    snaps = spark.sql(
        f"SELECT snapshot_id, committed_at FROM {t}.snapshots ORDER BY committed_at"
    )
    first_snapshot = snaps.first()["snapshot_id"]
    print("=== snapshots ===")
    snaps.show(truncate=False)

    before = spark.table(t).count()
    print(f"current row count: {before}")

    # Simulate a bad load: delete a chunk of rows (creates a new snapshot).
    spark.sql(f"DELETE FROM {t} WHERE rating < 1.0")
    after = spark.table(t).count()
    print(f"after 'bad' delete: {after}")

    # Time-travel: read the table AS OF the first snapshot.
    travelled = spark.sql(
        f"SELECT count(*) AS c FROM {t} VERSION AS OF {first_snapshot}"
    ).first()["c"]
    print(f"time-travel to first snapshot sees: {travelled}")

    # Roll back to the first snapshot — undo the bad delete.
    spark.sql(f"CALL demo.system.rollback_to_snapshot('{t}', {first_snapshot})")
    restored = spark.table(t).count()
    print(f"after rollback: {restored} (== original {before}: {restored == before})")
    spark.stop()


if __name__ == "__main__":
    main()
