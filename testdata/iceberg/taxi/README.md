# NYC Yellow Taxi Iceberg fixture source data

This directory is a complete, frozen Iceberg v2 table containing a 175,000-row
slice of the NYC Taxi & Limousine Commission Yellow Taxi Trip Record Data.
It includes data files, a manifest, a manifest list, and table metadata.
Each Parquet field carries the corresponding Iceberg field ID.

The table uses the stable URI prefix `s3://iceberg-test/warehouse/taxi/` in
its metadata. Tests map that prefix to this checked-in directory, so the
metadata stays valid regardless of where the repository is cloned.

## Selection

- Source: `yellow_tripdata_2024-01.parquet`, published by the NYC TLC.
- Pickup dates: 2024-01-08 through 2024-01-14 (inclusive).
- 25,000 valid trips per pickup date, ordered by pickup timestamp and location
  IDs, for a total of 175,000 rows.
- Excludes records with non-positive distance or total amount.
- Data is stored in seven Zstandard-compressed Parquet files, partitioned by
  `pickup_date`.

The original records contain pickup/drop-off timestamps and locations,
passenger count, distance, payment type, and fare components. This fixture
keeps those fields while using snake_case column names.

## Provenance

The NYC TLC publishes the monthly trip records as Parquet files and documents
the included fields at:

<https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>

## Source extraction

The data slice was produced with DuckDB 1.4.2 before writing the committed
Iceberg metadata tree with Iceberg Rust 0.10.1:

```sql
COPY (
  WITH sampled AS (
    SELECT
      VendorID AS vendor_id,
      tpep_pickup_datetime AS pickup_at,
      tpep_dropoff_datetime AS dropoff_at,
      passenger_count,
      trip_distance,
      PULocationID AS pickup_location_id,
      DOLocationID AS dropoff_location_id,
      payment_type,
      fare_amount,
      tip_amount,
      tolls_amount,
      total_amount,
      CAST(tpep_pickup_datetime AS DATE) AS pickup_date,
      row_number() OVER (
        PARTITION BY CAST(tpep_pickup_datetime AS DATE)
        ORDER BY tpep_pickup_datetime, PULocationID, DOLocationID
      ) AS row_number
    FROM read_parquet(
      'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    )
    WHERE tpep_pickup_datetime >= TIMESTAMP '2024-01-08 00:00:00'
      AND tpep_pickup_datetime < TIMESTAMP '2024-01-15 00:00:00'
      AND trip_distance > 0
      AND total_amount > 0
  )
  SELECT * EXCLUDE (row_number)
  FROM sampled
  WHERE row_number <= 25000
) TO 'testdata/iceberg/taxi/data' (
  FORMAT parquet,
  PARTITION_BY (pickup_date),
  COMPRESSION zstd,
  ROW_GROUP_SIZE 25000
);
```
