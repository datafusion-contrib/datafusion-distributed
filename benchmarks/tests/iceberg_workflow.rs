#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Command, Output};

    use datafusion_distributed_benchmarks::datasets::tpch::generate_tpch_data;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    #[test]
    fn prepares_and_runs_both_formats_without_overwriting_results() {
        let temp = TempDir::new().unwrap();
        let dataset = temp.path().join("tpch/sf1");
        let iceberg = temp.path().join("tpch/sf1-iceberg");
        generate_tpch_data(&dataset, 0.001, 1).unwrap();
        write_legacy_previous_run(&dataset);
        let prepare = ["prepare-iceberg", "--input", path(&dataset)];
        success(&prepare);
        assert!(!dataset.join("dataset.json").exists());

        let options = ["--query", "q6", "--iterations", "1", "--threads", "2"];
        let run = [&["run", "--dataset", path(&dataset)][..], &options].concat();
        let iceberg_run = [
            &["run", "--dataset", path(&iceberg), "--format", "iceberg"][..],
            &options,
        ]
        .concat();
        assert!(success(&run).contains("branch 'legacy' [prev]"));
        let parquet = saved_run(&dataset);
        success(&iceberg_run);
        saved_run(&iceberg);
        assert!(success(&iceberg_run).contains("Comparing"));
        assert_eq!(saved_run(&dataset), parquet);
        let comparison = success(&["compare", path(&dataset), path(&iceberg)]);
        assert!(comparison.contains(&format!(
            "Comparing {} results from branch",
            dataset.display()
        )));
        assert!(comparison.contains(&format!("with {} results from branch", iceberg.display())));
        assert!(comparison.contains("q6: prev="));

        assert!(!command(&prepare).status.success());
        fs::remove_file(iceberg.join("_SUCCESS")).unwrap();
        let incomplete = command(&iceberg_run);
        assert!(!incomplete.status.success());
        assert!(String::from_utf8_lossy(&incomplete.stderr).contains("missing or incomplete"));
    }

    #[test]
    fn rejects_unknown_backends_and_the_retired_run_flag() {
        for (flags, message) in [
            (vec!["--format", "csv"], "isn't a valid value"),
            (vec!["--iceberg"], "wasn't expected"),
            (vec!["--iceberg-column-stats"], "wasn't expected"),
        ] {
            let output = command(&[&["run", "--dataset", "unused"][..], &flags].concat());
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(message),
                "{output:?}"
            );
        }
    }

    #[test]
    fn compares_explicit_states_and_reads_legacy_results() {
        let temp = TempDir::new().unwrap();
        let dataset = temp.path().join("sf1");
        write_comparison_results(&dataset);
        let legacy = success(&["compare", "base", "candidate", "--dataset", path(&dataset)]);
        assert!(legacy.contains("prev= 100 ms, new= 200 ms"));
        for (base, new, expected) in [
            ("sf1@base", "sf1@candidate", "prev= 100 ms, new= 200 ms"),
            (
                "sf1-iceberg@base",
                "sf1-iceberg@candidate",
                "prev=  10 ms, new=  20 ms",
            ),
            ("sf1@base", "sf1-iceberg@base", "prev= 100 ms, new=  10 ms"),
            (
                "sf1@base",
                "sf1-iceberg@candidate",
                "prev= 100 ms, new=  20 ms",
            ),
        ] {
            let output = success(&[
                "compare",
                path(&temp.path().join(base)),
                path(&temp.path().join(new)),
            ]);
            assert!(output.contains(expected), "{output}");
        }
    }

    #[test]
    fn rejects_ambiguous_or_missing_comparisons() {
        let temp = TempDir::new().unwrap();
        let compare = ["compare", "--dataset", path(temp.path())];
        // Preserve the existing empty-result behavior for ordinary branch comparisons.
        success(&[compare.as_slice(), &["base", "candidate"]].concat());
        for (args, message) in [
            (vec![], "Exactly two states"),
            (vec!["tpch/sf1"], "Exactly two states"),
            (vec!["one", "two", "three"], "Exactly two states"),
            (
                vec!["@base", "tpch/sf1@candidate"],
                "Dataset and branch must not be empty",
            ),
            (
                vec!["tpch/sf1@", "tpch/sf1@candidate"],
                "Dataset and branch must not be empty",
            ),
            (vec!["--iceberg"], "wasn't expected"),
            (vec!["--compare-iceberg"], "wasn't expected"),
            (
                vec![path(temp.path()), "missing-dataset"],
                "Missing saved benchmark results",
            ),
        ] {
            let output = command(&[&["compare"], args.as_slice()].concat());
            assert!(!output.status.success());
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(message),
                "{output:?}"
            );
        }
    }

    fn write_legacy_previous_run(dataset: &Path) {
        let dir = dataset.join(".results/legacy");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("q6.json"),
            json!({
                "id": "q6", "dataset": path(dataset),
                "iterations": [{"elapsed": 100, "row_count": 1, "n_tasks": 0, "error": null}]
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            dataset.join("previous.json"),
            json!({
                "workers": 0, "threads": 2, "start_time": 0,
                "dataset": path(dataset), "branch": "legacy", "results": []
            })
            .to_string(),
        )
        .unwrap();
    }

    fn write_comparison_results(dataset: &Path) {
        for (suffix, branch, elapsed) in [
            ("", "base", 100),
            ("-iceberg", "base", 10),
            ("", "candidate", 200),
            ("-iceberg", "candidate", 20),
        ] {
            let dataset = PathBuf::from(format!("{}{suffix}", dataset.display()));
            let dir = dataset.join(".results").join(branch);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("q6.json"), json!({
                "id": "q6", "dataset": path(&dataset),
                "iterations": [{"elapsed": elapsed, "row_count": 1, "n_tasks": 1, "error": null}]
            }).to_string()).unwrap();
        }
    }

    fn saved_run(dir: &Path) -> (Vec<u8>, Vec<u8>) {
        let run = fs::read(dir.join("previous.json")).unwrap();
        let parsed: Value = serde_json::from_slice(&run).unwrap();
        assert_eq!(parsed.as_object().unwrap().len(), 6);
        assert_eq!(parsed["dataset"].as_str(), dir.to_str());
        let branch = parsed["branch"].as_str().unwrap();
        let current = Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .output()
            .unwrap();
        assert_eq!(
            branch,
            String::from_utf8_lossy(&current.stdout)
                .trim()
                .rsplit('/')
                .next()
                .unwrap()
        );
        let result = fs::read(dir.join(".results").join(branch).join("q6.json")).unwrap();
        let parsed: Value = serde_json::from_slice(&result).unwrap();
        assert!(parsed["iterations"][0]["error"].is_null());
        (run, result)
    }

    fn path(path: &Path) -> &str {
        path.to_str().unwrap()
    }

    fn command(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_dfbench"))
            .args(args)
            .output()
            .unwrap()
    }

    fn success(args: &[&str]) -> String {
        let output = command(args);
        assert!(
            output.status.success(),
            "{args:?}\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    }
}
