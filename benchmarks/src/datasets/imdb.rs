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

//! Join Order Benchmark (JOB) over the IMDB dataset.
//!
//! Data is the 21-table snapshot from
//! [event.cwi.nl/da/job/imdb.tgz](https://event.cwi.nl/da/job/imdb.tgz).
//! Queries follow [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark).

use super::common;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::prelude::{CsvReadOptions, SessionContext};
use std::fs;
use std::path::Path;

/// The 21 JOB / IMDB tables, matching DataFusion's `benchmarks/src/imdb`.
pub const IMDB_TABLES: &[&str] = &[
    "aka_name",
    "aka_title",
    "cast_info",
    "char_name",
    "comp_cast_type",
    "company_name",
    "company_type",
    "complete_cast",
    "info_type",
    "keyword",
    "kind_type",
    "link_type",
    "movie_companies",
    "movie_info_idx",
    "movie_keyword",
    "movie_link",
    "name",
    "role_type",
    "title",
    "movie_info",
    "person_info",
];

pub fn get_queries() -> Vec<String> {
    let mut queries = common::get_queries("testdata/imdb/queries");
    queries.sort_by(|a, b| query_sort_key(a).cmp(&query_sort_key(b)));
    queries
}

pub fn get_query(id: &str) -> Result<String, DataFusionError> {
    common::get_query("testdata/imdb/queries", id)
}

/// Sort `q1a` before `q1b` before `q10a`.
fn query_sort_key(id: &str) -> (u32, &str) {
    let rest = id.strip_prefix('q').unwrap_or(id);
    let digits = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    let number = rest[..digits].parse().unwrap_or(0);
    (number, &rest[digits..])
}

/// Official JOB table schemas (see DataFusion `get_imdb_table_schema`).
pub fn get_imdb_table_schema(table: &str) -> Schema {
    match table {
        "aka_name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("person_id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("name_pcode_cf", DataType::Utf8, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("surname_pcode", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "aka_title" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("kind_id", DataType::Int32, false),
            Field::new("production_year", DataType::Int32, true),
            Field::new("phonetic_code", DataType::Utf8, true),
            Field::new("episode_of_id", DataType::Int32, true),
            Field::new("season_nr", DataType::Int32, true),
            Field::new("episode_nr", DataType::Int32, true),
            Field::new("note", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "cast_info" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("person_id", DataType::Int32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("person_role_id", DataType::Int32, true),
            Field::new("note", DataType::Utf8, true),
            Field::new("nr_order", DataType::Int32, true),
            Field::new("role_id", DataType::Int32, false),
        ]),
        "char_name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("surname_pcode", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "comp_cast_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, false),
        ]),
        "company_name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("country_code", DataType::Utf8, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("name_pcode_sf", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "company_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, true),
        ]),
        "complete_cast" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, true),
            Field::new("subject_id", DataType::Int32, false),
            Field::new("status_id", DataType::Int32, false),
        ]),
        "info_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("info", DataType::Utf8, false),
        ]),
        "keyword" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("keyword", DataType::Utf8, false),
            Field::new("phonetic_code", DataType::Utf8, true),
        ]),
        "kind_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("kind", DataType::Utf8, true),
        ]),
        "link_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("link", DataType::Utf8, false),
        ]),
        "movie_companies" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("company_id", DataType::Int32, false),
            Field::new("company_type_id", DataType::Int32, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        "movie_info_idx" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("info_type_id", DataType::Int32, false),
            Field::new("info", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        "movie_keyword" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("keyword_id", DataType::Int32, false),
        ]),
        "movie_link" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("linked_movie_id", DataType::Int32, false),
            Field::new("link_type_id", DataType::Int32, false),
        ]),
        "name" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("gender", DataType::Utf8, true),
            Field::new("name_pcode_cf", DataType::Utf8, true),
            Field::new("name_pcode_nf", DataType::Utf8, true),
            Field::new("surname_pcode", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "role_type" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("role", DataType::Utf8, false),
        ]),
        "title" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("imdb_index", DataType::Utf8, true),
            Field::new("kind_id", DataType::Int32, false),
            Field::new("production_year", DataType::Int32, true),
            Field::new("imdb_id", DataType::Int32, true),
            Field::new("phonetic_code", DataType::Utf8, true),
            Field::new("episode_of_id", DataType::Int32, true),
            Field::new("season_nr", DataType::Int32, true),
            Field::new("episode_nr", DataType::Int32, true),
            Field::new("series_years", DataType::Utf8, true),
            Field::new("md5sum", DataType::Utf8, true),
        ]),
        "movie_info" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("movie_id", DataType::Int32, false),
            Field::new("info_type_id", DataType::Int32, false),
            Field::new("info", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        "person_info" => Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("person_id", DataType::Int32, false),
            Field::new("info_type_id", DataType::Int32, false),
            Field::new("info", DataType::Utf8, false),
            Field::new("note", DataType::Utf8, true),
        ]),
        other => unimplemented!("Schema for table {other} is not implemented"),
    }
}

/// Convert JOB CSV files into `output_dir/<table>/0.parquet`.
///
/// `tables` defaults to [`IMDB_TABLES`] when empty. Each table must have a
/// matching `<table>.csv` in `input_dir`.
pub async fn convert_imdb_csv_to_parquet(
    input_dir: &Path,
    output_dir: &Path,
    tables: &[&str],
) -> Result<(), Box<dyn std::error::Error>> {
    let tables = if tables.is_empty() {
        IMDB_TABLES
    } else {
        tables
    };
    fs::create_dir_all(output_dir)?;
    let ctx = SessionContext::new();

    for table in tables {
        let input_path = input_dir.join(format!("{table}.csv"));
        if !input_path.exists() {
            return Err(format!("missing IMDB csv: {}", input_path.display()).into());
        }
        let schema = get_imdb_table_schema(table);
        let df = ctx
            .read_csv(
                input_path
                    .to_str()
                    .ok_or("IMDB csv path is not valid UTF-8")?,
                CsvReadOptions::new()
                    .schema(&schema)
                    .has_header(false)
                    .delimiter(b',')
                    .escape(b'\\')
                    .file_extension(".csv"),
            )
            .await?;
        let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        let df = df.select_columns(&cols)?;

        let table_dir = output_dir.join(table);
        fs::create_dir_all(&table_dir)?;
        let output_path = table_dir.join("0.parquet");
        println!(
            "Converting '{}' to '{}'",
            input_path.display(),
            output_path.display()
        );
        df.write_parquet(
            output_path
                .to_str()
                .ok_or("IMDB parquet path is not valid UTF-8")?,
            DataFrameWriteOptions::new().with_single_file_output(true),
            None,
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::ParquetReadOptions;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_dir() -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("dfd-imdb-{nanos}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn query_files_are_job_numbered() {
        let ids = get_queries();
        assert_eq!(ids.len(), 113);
        assert_eq!(ids.first().map(String::as_str), Some("q1a"));
        assert_eq!(ids.last().map(String::as_str), Some("q33c"));
        let q1a = ids.iter().position(|id| id == "q1a").unwrap();
        let q1b = ids.iter().position(|id| id == "q1b").unwrap();
        let q10a = ids.iter().position(|id| id == "q10a").unwrap();
        assert!(q1a < q1b);
        assert!(q1b < q10a);
        assert!(get_query("q1a").unwrap().contains("company_type"));
    }

    #[test]
    fn schema_covers_all_tables() {
        for table in IMDB_TABLES {
            assert!(!get_imdb_table_schema(table).fields().is_empty(), "{table}");
        }
    }

    #[test]
    fn query_sort_key_orders_letter_variants() {
        assert!(query_sort_key("q1a") < query_sort_key("q1b"));
        assert!(query_sort_key("q1d") < query_sort_key("q2a"));
        assert!(query_sort_key("q9d") < query_sort_key("q10a"));
    }

    #[tokio::test]
    async fn convert_writes_table_directories() {
        let dir = temp_dir();
        fs::write(dir.join("kind_type.csv"), "1,movie\n2,tv series\n").unwrap();
        fs::write(
            dir.join("title.csv"),
            "1,The Matrix,,1,1999,,,,,,,\n2,Inception,,1,2010,,,,,,,\n",
        )
        .unwrap();

        convert_imdb_csv_to_parquet(&dir, &dir, &["kind_type", "title"])
            .await
            .unwrap();

        assert!(dir.join("kind_type").join("0.parquet").exists());
        assert!(dir.join("title").join("0.parquet").exists());

        let ctx = SessionContext::new();
        ctx.register_parquet(
            "kind_type",
            dir.join("kind_type").to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
        ctx.register_parquet(
            "title",
            dir.join("title").to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let batches = ctx
            .sql("SELECT t.title, kt.kind FROM title t JOIN kind_type kt ON t.kind_id = kt.id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);
        fs::remove_dir_all(dir).ok();
    }
}
