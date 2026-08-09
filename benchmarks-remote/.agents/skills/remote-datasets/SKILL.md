---
name: remote-datasets
description: Manage datasets for the DataFusion Distributed remote benchmark foundation. Use when an agent needs to discover local datasets, selectively synchronize Parquet data to the benchmark S3 bucket, diagnose dataset discovery or upload behavior, or explicitly remove a remote dataset.
---

# Remote Datasets

Operate from `benchmarks-remote` and use its npm commands. Do not reproduce dataset-to-directory mappings or call S3 with a guessed bucket name.

## Authenticate

1. Use the caller-selected `AWS_PROFILE` and `AWS_REGION`; never hardcode an account, profile, or `aws-vault` wrapper.
2. Run `aws sts get-caller-identity` before a mutating operation.
3. If SSO has expired and `AWS_PROFILE` is set, run `aws sso login --profile "$AWS_PROFILE"` once. Stop and request authentication if it does not succeed.

## Discover datasets

Run:

```bash
npm run sync-bucket -- --list
```

Treat dataset names as literal paths relative to `testdata/`, for example `tpch/sf10` or `clickbench/0-100`. The command discovers datasets in the current checkout, the primary checkout for a linked worktree, or `BENCHMARK_TESTDATA_ROOT`. Do not invent aliases such as `tpch_sf10`.

## Synchronize datasets

Prefer explicit selection because datasets can be large:

```bash
npm run sync-bucket -- --dataset tpch/sf10
npm run sync-bucket -- --dataset tpch/sf10 clickbench/0-100
```

Run `npm run sync-bucket` without `--dataset` only when the user explicitly wants every discovered dataset uploaded. Sync requires at least one Parquet file and mirrors only Parquet files with `aws s3 sync --delete`. It is safe to rerun after interruption.

Do not create readiness markers. Benchmark execution intentionally assumes the requested S3 data exists.

## Remove datasets

Dataset removal is destructive. Resolve the exact dataset name and obtain explicit authorization before running:

```bash
npm run dataset-destroy -- --dataset tpch/sf10 --yes
```

This removes only the selected S3 prefix. It does not remove local `testdata/`, engine deployments, or the foundation.

## Validate changes

When changing dataset tooling, run:

```bash
npm run build
npm test
```

Exercise `--list` before any live sync. Use the command's printed source and destination paths as the upload evidence.
