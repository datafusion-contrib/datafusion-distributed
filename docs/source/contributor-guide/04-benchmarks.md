# Benchmarks

Use this page to run existing benchmarks, qualify performance improvements, and
write new benchmarks.

## Micro benchmarks

When contributing performance improvements to very specific pieces of code,
prefer using the existing micro-benches in `benchmarks/benches`.

If there's no benchmark that exercises the code path you are aiming to improve,
consider creating a separate PR adding the necessary benchmark first, following
the same pattern as the existing ones.

The structure of new benchmarks should be unsurprising and consistent with
existing ones.

## Local Benchmarks

It's recommended to run these benchmarks locally when contributing to ensure
there are no performance regressions.

### Generating Test Data

First, a TPCH dataset must be generated:

```bash
cd benchmarks
SCALE_FACTOR=10 ./gen-tpch.sh
```

This might take a while.

### Running Benchmarks

After generating the data, it's recommended to use the `run.sh` script to run
the benchmarks. A good setup is to run 8 workers throttled at 2 physical threads
per worker. This provides a relatively accurate benchmarking environment for a
distributed system locally.

```bash
WORKERS=8 ./benchmarks/run.sh --threads 2 --dataset tpch/sf10
```

Subsequent runs will compare results against the previous one, so a useful trick
to measure the impact of a PR is to first run the benchmarks on `main`, and then
on the PR branch.

More information about these benchmarks can be found in
the [benchmarks README](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/README.md).

## Remote Benchmarks

These benchmarks are hosted on
https://github.com/gabotechs/datafusion-distributed-dev-tools, and they can be
trigger on PRs by commenting `benchmark run tpch/sf100` or any other relevant
dataset (only whitelisted people can run those).

## Qualifying performance improvements

A performance claim needs reproducible, representative evidence. A faster
microbenchmark alone is not sufficient evidence for a cross-cutting production
change.

Most benchmarks in this project are based on real data, and therefore, the
variability of results is high. When reporting benchmarks results over TPC-H,
TPC-DS or ClickBench data, keep into account that certain queries can easily
show variabilities of +-50% just because of network noise when reading files
from S3.

The single most important value is the total wall time of the full suite, not
individual query results. For example a +100% improvement in one specific query
is meaningless if the full test suite did just a +1% improvement.

Do not overindex in individual query results reported by a single benchmark run,
when in doubt, re-run that specific query with a high number of iterations
(e.g., -i 20), in `main` VS the new branch, and then evaluate if indeed there
was an issue there.
