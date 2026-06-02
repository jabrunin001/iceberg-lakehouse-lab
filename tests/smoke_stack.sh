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
