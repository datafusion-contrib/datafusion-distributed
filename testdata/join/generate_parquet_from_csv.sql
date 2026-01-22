-- datafusion-cli -f testdata/join/generate_parquet_from_csv.sql

-- Generate parquet dim files from csv files.
COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=A/data0.csv")
TO "testdata/join/parquet/dim/d_dkey=A/data0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=B/data0.csv")
TO "testdata/join/parquet/dim/d_dkey=B/data0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=C/data0.csv")
TO "testdata/join/parquet/dim/d_dkey=C/data0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=D/data0.csv")
TO "testdata/join/parquet/dim/d_dkey=D/data0.parquet"
STORED AS PARQUET;

-- Generate parquet fact files from csv files.
COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=A/data0.csv")
TO "testdata/join/parquet/fact/f_dkey=A/data0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=B/data0.csv")
TO "testdata/join/parquet/fact/f_dkey=B/data0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=C/data0.csv")
TO "testdata/join/parquet/fact/f_dkey=C/data0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=D/data0.csv")
TO "testdata/join/parquet/fact/f_dkey=D/data0.parquet"
STORED AS PARQUET;

