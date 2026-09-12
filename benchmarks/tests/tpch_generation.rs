#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::path::Path;

    use datafusion_distributed_benchmarks::datasets::tpch::generate_tpch_data;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use tempfile::TempDir;

    #[test]
    fn partitions_scalable_tables_without_duplicating_fixed_tables() {
        let temp = TempDir::new().unwrap();
        generate_tpch_data(temp.path(), 0.001, 4).unwrap();
        assert_layout(temp.path(), 4);

        // Simulate leftovers from the old generator, then regenerate with fewer partitions.
        for table in ["nation", "region"] {
            let dir = temp.path().join(table);
            fs::copy(dir.join("1.parquet"), dir.join("2.parquet")).unwrap();
        }
        generate_tpch_data(temp.path(), 0.001, 2).unwrap();
        assert_layout(temp.path(), 2);
    }

    fn assert_layout(root: &Path, partitions: usize) {
        for (table, expected_files, expected_rows) in [
            ("nation", 1, 25),
            ("region", 1, 5),
            ("customer", partitions, 150),
        ] {
            let files = fs::read_dir(root.join(table))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect::<Vec<_>>();
            let rows: i64 = files
                .iter()
                .map(|path| {
                    SerializedFileReader::new(File::open(path).unwrap())
                        .unwrap()
                        .metadata()
                        .file_metadata()
                        .num_rows()
                })
                .sum();
            assert_eq!(
                (files.len(), rows),
                (expected_files, expected_rows),
                "{table}"
            );
        }
    }
}
