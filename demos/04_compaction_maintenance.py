"""Demo: small-files problem, rewrite_data_files compaction, snapshot expiry."""
from pyspark.sql import SparkSession


def main():
    spark = SparkSession.builder.appName("demo_compaction").getOrCreate()
    t = "demo.bronze.playback_events"

    files_before = spark.sql(f"SELECT count(*) AS c FROM {t}.files").first()["c"]
    print(f"data files before compaction: {files_before}")

    # Compact small files into larger ones.
    res = spark.sql(f"CALL demo.system.rewrite_data_files(table => '{t}')")
    print("=== rewrite_data_files result ===")
    res.show(truncate=False)

    files_after = spark.sql(f"SELECT count(*) AS c FROM {t}.files").first()["c"]
    print(f"data files after compaction: {files_after}")

    # Expire old snapshots to reclaim metadata/storage. now() can't be used directly as
    # a CALL argument, so resolve a cutoff timestamp first and pass it as a literal.
    cutoff = spark.sql(
        "SELECT date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS t"
    ).first()["t"]
    snaps_before = spark.sql(f"SELECT count(*) AS c FROM {t}.snapshots").first()["c"]
    spark.sql(
        f"CALL demo.system.expire_snapshots("
        f"table => '{t}', older_than => TIMESTAMP '{cutoff}', retain_last => 1)"
    )
    snaps_after = spark.sql(f"SELECT count(*) AS c FROM {t}.snapshots").first()["c"]
    print(f"snapshots: {snaps_before} -> {snaps_after}")
    spark.stop()


if __name__ == "__main__":
    main()
