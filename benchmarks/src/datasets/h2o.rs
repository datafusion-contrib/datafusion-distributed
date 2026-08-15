// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::common;
use arrow::array::{Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Official h2oai/db-benchmark groupby-datagen seed.
const SEED: u64 = 108;

/// Default group cardinality (`K`) matching `G1_*_1e2_0_0`.
const DEFAULT_GROUPS: u64 = 100;

/// Rows at scale factor 1, matching the official "small" (1e7) dataset.
const ROWS_PER_SCALE_FACTOR: f64 = 10_000_000.0;

const BATCH_SIZE: usize = 8192;

const TABLE_NAME: &str = "x";

pub fn get_queries() -> Vec<String> {
    common::get_queries("testdata/h2o/queries")
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/h2o/queries", id)
}

/// Number of rows for a scale factor. SF1 is 10 million rows.
pub fn rows_for_scale_factor(sf: f64) -> Result<u64, DataFusionError> {
    if !sf.is_finite() || sf <= 0.0 {
        return Err(DataFusionError::Internal(format!(
            "scale factor must be > 0, got {sf}"
        )));
    }
    let rows = (sf * ROWS_PER_SCALE_FACTOR).round();
    if rows < 1.0 || !rows.is_finite() {
        return Err(DataFusionError::Internal(format!(
            "scale factor {sf} produces no rows"
        )));
    }
    Ok(rows as u64)
}

/// Generates the h2o groupby table `x` as parquet files under `data_dir/x/`.
///
/// Column distributions follow
/// [groupby-datagen.R](https://github.com/h2oai/db-benchmark/blob/master/_data/groupby-datagen.R):
/// `id1`/`id2`/`id4`/`id5` have `K` distinct values, `id3`/`id6` have `N/K`,
/// `v1` is in `[1, 5]`, `v2` is in `[1, 15]`, and `v3` is `round(U(0, 100), 6)`.
pub fn generate_h2o_data(
    data_dir: &Path,
    n_rows: u64,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    generate_h2o_data_with_groups(data_dir, n_rows, DEFAULT_GROUPS, partitions)
}

pub fn generate_h2o_data_with_groups(
    data_dir: &Path,
    n_rows: u64,
    n_groups: u64,
    partitions: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    if n_rows == 0 {
        return Err("n_rows must be > 0".into());
    }
    if n_groups == 0 {
        return Err("n_groups must be > 0".into());
    }
    if partitions == 0 {
        return Err("partitions must be > 0".into());
    }

    let n_groups = n_groups.min(n_rows);
    let n_small_groups = (n_rows / n_groups).max(1);

    let table_dir = data_dir.join(TABLE_NAME);
    fs::create_dir_all(&table_dir)?;

    let schema = schema();
    let id1_pool: Vec<String> = (1..=n_groups).map(|i| format!("id{i:03}")).collect();
    let id3_pool: Vec<String> = (1..=n_small_groups).map(|i| format!("id{i:010}")).collect();

    let mut rng = StdRng::seed_from_u64(SEED);
    let mut file_idx = 1usize;
    for part_rows in partition_row_counts(n_rows, partitions) {
        if part_rows == 0 {
            continue;
        }
        write_partition(
            &mut rng,
            &schema,
            &id1_pool,
            &id3_pool,
            n_groups,
            n_small_groups,
            part_rows,
            &table_dir.join(format!("{file_idx}.parquet")),
        )?;
        file_idx += 1;
    }
    Ok(())
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id1", DataType::Utf8, false),
        Field::new("id2", DataType::Utf8, false),
        Field::new("id3", DataType::Utf8, false),
        Field::new("id4", DataType::Int64, false),
        Field::new("id5", DataType::Int64, false),
        Field::new("id6", DataType::Int64, false),
        Field::new("v1", DataType::Int64, false),
        Field::new("v2", DataType::Int64, false),
        Field::new("v3", DataType::Float64, false),
    ]))
}

fn partition_row_counts(n_rows: u64, partitions: usize) -> Vec<u64> {
    let p = partitions as u64;
    let base = n_rows / p;
    let rem = n_rows % p;
    (0..p).map(|i| base + u64::from(i < rem)).collect()
}

#[allow(clippy::too_many_arguments)]
fn write_partition(
    rng: &mut StdRng,
    schema: &Arc<Schema>,
    id1_pool: &[String],
    id3_pool: &[String],
    n_groups: u64,
    n_small_groups: u64,
    part_rows: u64,
    path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let file = fs::File::create(path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let mut remaining = part_rows;
    while remaining > 0 {
        let batch_rows = remaining.min(BATCH_SIZE as u64) as usize;
        writer.write(&build_batch(
            rng,
            schema,
            id1_pool,
            id3_pool,
            n_groups,
            n_small_groups,
            batch_rows,
        )?)?;
        remaining -= batch_rows as u64;
    }

    writer.close()?;
    Ok(())
}

fn build_batch(
    rng: &mut StdRng,
    schema: &Arc<Schema>,
    id1_pool: &[String],
    id3_pool: &[String],
    n_groups: u64,
    n_small_groups: u64,
    rows: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let mut id1 = StringBuilder::with_capacity(rows, rows * 8);
    let mut id2 = StringBuilder::with_capacity(rows, rows * 8);
    let mut id3 = StringBuilder::with_capacity(rows, rows * 16);
    let mut id4 = Int64Builder::with_capacity(rows);
    let mut id5 = Int64Builder::with_capacity(rows);
    let mut id6 = Int64Builder::with_capacity(rows);
    let mut v1 = Int64Builder::with_capacity(rows);
    let mut v2 = Int64Builder::with_capacity(rows);
    let mut v3 = Float64Builder::with_capacity(rows);

    for _ in 0..rows {
        id1.append_value(&id1_pool[rng.random_range(0..id1_pool.len())]);
        id2.append_value(&id1_pool[rng.random_range(0..id1_pool.len())]);
        id3.append_value(&id3_pool[rng.random_range(0..id3_pool.len())]);
        id4.append_value(rng.random_range(1..=n_groups) as i64);
        id5.append_value(rng.random_range(1..=n_groups) as i64);
        id6.append_value(rng.random_range(1..=n_small_groups) as i64);
        v1.append_value(rng.random_range(1..=5) as i64);
        v2.append_value(rng.random_range(1..=15) as i64);
        v3.append_value((rng.random::<f64>() * 100.0 * 1_000_000.0).round() / 1_000_000.0);
    }

    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id1.finish()),
            Arc::new(id2.finish()),
            Arc::new(id3.finish()),
            Arc::new(id4.finish()),
            Arc::new(id5.finish()),
            Arc::new(id6.finish()),
            Arc::new(v1.finish()),
            Arc::new(v2.finish()),
            Arc::new(v3.finish()),
        ],
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dfd-h2o-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn query_files_are_numbered() {
        assert_eq!(
            get_queries(),
            ["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10"]
        );
        assert!(get_query("q1").unwrap().contains("GROUP BY id1"));
    }

    #[test]
    fn scale_factor_maps_to_official_sizes() {
        assert_eq!(rows_for_scale_factor(1.0).unwrap(), 10_000_000);
        assert_eq!(rows_for_scale_factor(10.0).unwrap(), 100_000_000);
        assert_eq!(rows_for_scale_factor(0.01).unwrap(), 100_000);
        assert!(rows_for_scale_factor(0.0).is_err());
        assert!(rows_for_scale_factor(-1.0).is_err());
    }

    #[test]
    fn generate_writes_partitioned_groupby_table() {
        let dir = temp_dir();
        generate_h2o_data_with_groups(&dir, 2_000, 100, 2).unwrap();

        let files: Vec<_> = fs::read_dir(dir.join("x"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("parquet"))
            .collect();
        assert_eq!(files.len(), 2);

        let mut rows = 0usize;
        let mut id1 = std::collections::BTreeSet::new();
        let mut id3 = std::collections::BTreeSet::new();
        for path in &files {
            let file = fs::File::open(path).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)
                .unwrap()
                .build()
                .unwrap();
            for batch in reader {
                let batch = batch.unwrap();
                rows += batch.num_rows();
                let id1_col = batch
                    .column_by_name("id1")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let id3_col = batch
                    .column_by_name("id3")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                let v1_col = batch
                    .column_by_name("v1")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let v3_col = batch
                    .column_by_name("v3")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                for i in 0..batch.num_rows() {
                    id1.insert(id1_col.value(i).to_string());
                    id3.insert(id3_col.value(i).to_string());
                    assert!((1..=5).contains(&v1_col.value(i)));
                    assert!((0.0..=100.0).contains(&v3_col.value(i)));
                }
            }
        }

        assert_eq!(rows, 2_000);
        assert!(id1.len() <= 100);
        assert!(id3.len() <= 20);
        fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn generated_table_answers_groupby_q1() {
        let dir = temp_dir();
        generate_h2o_data_with_groups(&dir, 2_000, 100, 2).unwrap();

        let ctx = SessionContext::new();
        ctx.register_parquet(
            "x",
            dir.join("x").to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let batches = ctx
            .sql("SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let out_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(out_rows > 0);
        assert!(out_rows <= 100);
        fs::remove_dir_all(dir).ok();
    }
}
