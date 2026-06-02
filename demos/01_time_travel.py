"""Demo: Iceberg snapshots, time-travel queries, and rollback."""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("demo_time_travel").getOrCreate()
    t = "demo.bronze.ratings"

    print("=== snapshots ===")
    spark.sql(
        f"SELECT snapshot_id, committed_at FROM {t}.snapshots ORDER BY committed_at"
    ).show(truncate=False)

    before = spark.table(t).count()
    # Capture the CURRENT snapshot (the state right before our 'bad' change). Rolling
    # back to this is always valid because it is, by definition, an ancestor of whatever
    # we commit next.
    good_snapshot = spark.sql(
        f"SELECT snapshot_id FROM {t}.snapshots ORDER BY committed_at DESC"
    ).first()["snapshot_id"]
    print(f"current row count: {before} (good snapshot: {good_snapshot})")

    # Simulate a bad load: delete a chunk of rows (creates a new snapshot).
    spark.sql(f"DELETE FROM {t} WHERE rating < 1.0")
    after = spark.table(t).count()
    print(f"after 'bad' delete: {after}")

    # Time-travel: read the table AS OF the good (pre-delete) snapshot.
    travelled = spark.sql(
        f"SELECT count(*) AS c FROM {t} VERSION AS OF {good_snapshot}"
    ).first()["c"]
    print(f"time-travel to good snapshot sees: {travelled}")

    # Roll back to the good snapshot — undo the bad delete.
    spark.sql(f"CALL demo.system.rollback_to_snapshot('{t}', {good_snapshot})")
    restored = spark.table(t).count()
    print(f"after rollback: {restored} (== original {before}: {restored == before})")
    spark.stop()


if __name__ == "__main__":
    main()
