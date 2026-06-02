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
    spark.sql(
        "SELECT snapshot_id, committed_at FROM demo.bronze.ratings_iceberg_demo.snapshots"
    ).show(truncate=False)

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
