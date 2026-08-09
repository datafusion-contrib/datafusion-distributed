---
name: remote-benchmark-run
description: Run and assess remote DataFusion Distributed benchmarks against an already deployed DataFusion, Trino, Spark, or Ballista Kubernetes engine. Use when an agent needs to execute TPC-H, TPC-DS, ClickBench, selected queries, repeated iterations, debugging plans, result comparison, interruption recovery, or benchmark failure diagnosis.
---

# Remote Benchmark Run

Operate from `benchmarks-remote`. Benchmark execution is local and connects to the remote Kubernetes service through a temporary port-forward.

## Preconditions

1. Use the caller-selected `AWS_PROFILE` and `AWS_REGION`; never hardcode an account, profile, or `aws-vault` wrapper.
2. Require an existing foundation, deployed engine, and synchronized dataset. Do not deploy infrastructure, install an engine, or sync data as part of a benchmark command.
3. Use literal dataset paths such as `tpch/sf10` or `clickbench/0-100`; never translate them to underscore aliases.

## Run

Select one supported engine:

```bash
npm run datafusion-bench -- --dataset tpch/sf10
npm run trino-bench -- --dataset tpch/sf10
npm run spark-bench -- --dataset tpch/sf10
npm run ballista-bench -- --dataset tpch/sf10
```

Common options are:

```text
--iterations <number>
--queries q1,q6,q21
--warmup true|false
--debug true|false
```

Pass engine-specific DataFusion options only to `datafusion-bench`; inspect `src/datafusion-bench.ts --help` behavior before changing defaults.

The wrapper acquires the global benchmark lock, opens `localhost:9000`, runs the local TypeScript client, and cleans up the tunnel and lock on exit. It does not upload results. Abrupt interruption may leave the previous completed local results, which is acceptable.

If another active run owns the lock, wait for it. Never delete or overwrite an active lock. A stale lock becomes recoverable after its heartbeat timeout.

## Assess results

Results are stored below the dataset's local `.results-remote/` directory and automatically compare with the previous completed run. Use `npm run compare` for an explicit local comparison.

For performance claims, prefer the full suite total. Treat a single run or isolated query as noisy; repeat a suspect query with enough iterations and compare the same workload and engine shape across revisions.

## Diagnose failures

- If the engine service is absent, use `$remote-engine-deployment`; do not add lazy deployment to the runner.
- If data is missing, use `$remote-datasets`; the runner intentionally does not scan S3 before execution.
- If the port-forward fails, inspect the engine namespace, service, and pods with the generated kubeconfig.
- If a query fails, preserve the query error and inspect the relevant engine logs. Do not add a separate health-check system.

## Validate runner changes

Run:

```bash
npm run build
npm test
bash -n k8s/run-benchmark.sh
```

Then run at least one live query with one iteration and no warmup against each affected engine. Confirm the command exits successfully and leaves no listener on local port 9000.
