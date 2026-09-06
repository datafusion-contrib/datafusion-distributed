#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::process::{Command, Output};

    use datafusion_distributed_benchmarks::datasets::tpch::generate_tpch_data;
    use serde_json::{Value, json};
    use tempfile::TempDir;

    #[test]
    fn prepares_and_runs_both_formats_without_overwriting_results() {
        let temp = TempDir::new().unwrap();
        let dataset = temp.path().join("tpch/sf1");
        generate_tpch_data(&dataset, 0.001, 1).unwrap();
        write_legacy_previous_run(&dataset);
        let prepare = ["prepare-iceberg", "--input", path(&dataset)];
        success(&prepare);
        assert!(!dataset.join("dataset.json").exists());

        let run = [
            "run",
            "--dataset",
            path(&dataset),
            "--query",
            "q6",
            "--iterations",
            "1",
            "--threads",
            "2",
        ];
        assert!(success(&run).contains("branch 'legacy' [prev]"));
        let parquet = saved_run(&dataset, "parquet");
        success(&[run.as_slice(), &["--iceberg"]].concat());
        saved_run(&dataset, "iceberg");
        assert!(success(&[run.as_slice(), &["--iceberg"]].concat()).contains("Comparing"));
        assert_eq!(saved_run(&dataset, "parquet"), parquet);
        let comparison = success(&["compare", "--dataset", path(&dataset), "--compare-iceberg"]);
        assert!(comparison.contains("(parquet) [prev]"));
        assert!(comparison.contains("(iceberg) [new]"));
        assert!(comparison.contains("q6: prev="));

        assert!(!command(&prepare).status.success());
        fs::remove_file(dataset.join(".iceberg/_SUCCESS")).unwrap();
        let incomplete = command(&[run.as_slice(), &["--iceberg"]].concat());
        assert!(!incomplete.status.success());
        assert!(String::from_utf8_lossy(&incomplete.stderr).contains("missing or incomplete"));
    }

    #[test]
    fn compares_selected_formats_and_reads_legacy_results() {
        let temp = TempDir::new().unwrap();
        write_comparison_results(temp.path());
        let compare = [
            "compare",
            "base",
            "candidate",
            "--dataset",
            path(temp.path()),
        ];
        for (flags, expected) in [
            (vec![], "prev= 100 ms, new= 200 ms"),
            (vec!["--iceberg"], "prev=  10 ms, new=  20 ms"),
        ] {
            let output = success(&[compare.as_slice(), flags.as_slice()].concat());
            assert!(output.contains(expected), "{output}");
        }
        let output = success(&[
            "compare",
            "base",
            "--dataset",
            path(temp.path()),
            "--compare-iceberg",
        ]);
        assert!(output.contains("prev= 100 ms, new=  10 ms"));
    }

    #[test]
    fn rejects_ambiguous_or_missing_comparisons() {
        let temp = TempDir::new().unwrap();
        let compare = ["compare", "--dataset", path(temp.path())];
        // Preserve the existing empty-result behavior for ordinary branch comparisons.
        success(&[compare.as_slice(), &["base", "candidate"]].concat());
        for (args, message) in [
            (
                vec!["base", "candidate", "--compare-iceberg"],
                "at most one branch",
            ),
            (
                vec!["base", "--iceberg", "--compare-iceberg"],
                "cannot be used with",
            ),
            (vec![], "Exactly two branches"),
            (vec!["base", "--iceberg"], "Exactly two branches"),
            (
                vec!["base", "--compare-iceberg"],
                "Missing saved benchmark results",
            ),
        ] {
            let output = command(&[compare.as_slice(), args.as_slice()].concat());
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
        for (format, branch, elapsed) in [
            (".", "base", 100),
            (".iceberg", "base", 10),
            (".", "candidate", 200),
            (".iceberg", "candidate", 20),
        ] {
            let dir = dataset.join(format).join(".results").join(branch);
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("q6.json"), json!({
                "id": "q6", "dataset": path(dataset),
                "iterations": [{"elapsed": elapsed, "row_count": 1, "n_tasks": 1, "error": null}]
            }).to_string()).unwrap();
        }
    }

    fn saved_run(dataset: &Path, format: &str) -> (Vec<u8>, Vec<u8>) {
        let dir = if format == "iceberg" {
            dataset.join(".iceberg")
        } else {
            dataset.to_path_buf()
        };
        let run = fs::read(dir.join("previous.json")).unwrap();
        let parsed: Value = serde_json::from_slice(&run).unwrap();
        assert_eq!(parsed.as_object().unwrap().len(), 6);
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
