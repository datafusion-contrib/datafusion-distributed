-- datafusion-cli -f testdata/join/generate_parquet_from_csv.sql

-- Generate parquet dim files from csv files.
COPY (SELECT * FROM "testdata/join/csv/dim/data_A0.csv")
TO "testdata/join/parquet/dim/data_A0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/data_B0.csv")
TO "testdata/join/parquet/dim/data_B0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/data_C0.csv")
TO "testdata/join/parquet/dim/data_C0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/data_D0.csv")
TO "testdata/join/parquet/dim/data_D0.parquet"
STORED AS PARQUET;

-- Generate parquet fact files from csv files.
COPY (SELECT * FROM "testdata/join/csv/fact/data_A0.csv")
TO "testdata/join/parquet/fact/data_A0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/data_B0.csv")
TO "testdata/join/parquet/fact/data_B0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/data_B1.csv")
TO "testdata/join/parquet/fact/data_B1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/data_B2.csv")
TO "testdata/join/parquet/fact/data_B2.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/data_C0.csv")
TO "testdata/join/parquet/fact/data_C0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/data_C1.csv")
TO "testdata/join/parquet/fact/data_C1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/data_D0.csv")
TO "testdata/join/parquet/fact/data_D0.parquet"
STORED AS PARQUET;

