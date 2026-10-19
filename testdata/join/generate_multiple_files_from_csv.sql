-- datafusion-cli -f testdata/join/generate_multiple_files_from_csv.sql
-- Keep this fixture separate from the single-file tables used by other tests.
-- Each key has two dim files and three fact files to exercise cross-file joins.
-- Filenames include the key so tests can select four or five partitions.
-- E reuses A's CSV rows; the partition key is supplied by the directory.

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=A/A0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=A/A1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=B/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=B/B0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=B/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=B/B1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=C/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=C/C0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=C/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=C/C1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=D/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=D/D0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=D/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=D/D1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=E/E0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/dim/d_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/dim/d_dkey=E/E1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=A/A0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=A/A1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=A/A2.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=B/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=B/B0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=B/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=B/B1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=B/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=B/B2.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=C/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=C/C0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=C/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=C/C1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=C/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=C/C2.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=D/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=D/D0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=D/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=D/D1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=D/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=D/D2.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=E/E0.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=E/E1.parquet"
STORED AS PARQUET;

COPY (SELECT * FROM "testdata/join/csv/fact/f_dkey=A/data0.csv")
TO "testdata/join/parquet_multiple_files/fact/f_dkey=E/E2.parquet"
STORED AS PARQUET;
