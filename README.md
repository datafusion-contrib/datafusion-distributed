# DataFusion Distributed

[![Crates.io][crates-badge]][crates-url]
[![Docs][docs-badge]][docs-url]
[![Apache licensed][license-badge]][license-url]
[![Discord chat][discord-badge]][discord-url]

Scale [Apache DataFusion](https://github.com/apache/datafusion) across a
cluster — without leaving the DataFusion you already know.

> [!NOTE]
> This project is not part of Apache DataFusion.

DataFusion Distributed is a toolkit that extends Apache DataFusion with
distributed query execution. It aims for a developer experience as close as
possible to vanilla DataFusion while staying unopinionated about your networking
stack.

It's **not** an out-of-the-box distributed engine — it's a library for building
one, with sane defaults for the common case of file-based data sources. A
distributed plan **is** a normal DataFusion physical plan, with a few extra
nodes that stream Arrow data between machines, so you can take an existing
single-node DataFusion system and add distributed execution with minimal
changes.

## Getting started

Going distributed takes exactly three things:

1. Enable the distributed planner on your session.
2. Tell it where your workers are.
3. Run the worker gRPC servers.

The [Quick start](https://datafusion-contrib.github.io/datafusion-distributed/user-guide/01-quick-start.html)
walks through all three in a few minutes. For everything beyond that — resolving
workers dynamically, distributing your own custom `ExecutionPlan`s, collecting
runtime metrics, and more — see the
[full documentation](https://datafusion-contrib.github.io/datafusion-distributed).

## Benchmarks

DataFusion Distributed consistently outperforms other distributed query engines
across TPC-H and TPC-DS. The chart below shows how much slower each engine is
relative to DataFusion Distributed
(lower is better):

![How much slower than DataFusion Distributed?](./docs/source/_static/images/summary_relative.png)

<details>
<summary>Per-dataset totals</summary>

| Dataset     | df-dist |                                                                                Ballista | Spark | Trino | Queries compared |
|-------------|--------:|----------------------------------------------------------------------------------------:|------:|------:|-----------------:|
| TPC-H SF1   |  **7s** |                                                                                     11s |   30s |   18s |               22 |
| TPC-H SF10  | **10s** |                                                                                     57s |   51s |   33s |               22 |
| TPC-H SF100 | **42s** | N/A ([#1836](https://github.com/datafusion-contrib/datafusion-distributed/issues/1836)) |  261s |   93s |               19 |
| TPC-DS SF1  | **29s** |                                                                                     72s |  101s |   85s |               67 |

![TPC-H SF1](./docs/source/_static/images/tpch_sf1.png)
![TPC-H SF10](./docs/source/_static/images/tpch_sf10.png)
![TPC-H SF100](./docs/source/_static/images/tpch_sf100.png)
![TPC-DS SF1](./docs/source/_static/images/tpcds_sf1.png)

</details>

**Conditions.** All engines ran on the same cluster: 12 AWS EC2 `c5n.2xlarge`
instances (8 vCPUs and 21 GiB of memory each, with up to 25 Gbps networking)
reading Parquet files stored in Amazon S3. Each engine's total is the sum of
per-query median (p50) latencies over the queries that all compared engines
completed successfully; lower is better.

The benchmarking code is public and open for anyone to easily reproduce. It uses
AWS CDK for automating the creation of the benchmarking cluster so that anyone
can reproduce the same results in their own AWS account. The code can be found
in the [benchmarks/cdk](./benchmarks/cdk) directory.

## Core tenets of the project

- Be as close as possible to vanilla DataFusion, providing a seamless
  integration with existing DataFusion systems and a familiar API for building
  applications.
- Unopinionated about networking. This crate does not take any opinion about the
  networking stack, and users are expected to leverage their own infrastructure
  for hosting DataFusion nodes.
- No coordinator-worker split. To keep infrastructure simple, any node can act
  as a coordinator or a worker.
- A library, not an engine. The goal is to provide the tools for people to build
  distributed engines, not being one.

## Documentation

- User guide: https://datafusion-contrib.github.io/datafusion-distributed
- Contributor
  guide: https://datafusion-contrib.github.io/datafusion-distributed/contributor-guide/01-index.html

[crates-badge]: https://img.shields.io/crates/v/datafusion-distributed.svg
[crates-url]: https://crates.io/crates/datafusion-distributed
[docs-badge]: https://img.shields.io/badge/docs-online-blue
[docs-url]: https://datafusion-contrib.github.io/datafusion-distributed
[license-badge]: https://img.shields.io/badge/license-Apache%202.0-blue.svg
[license-url]: https://github.com/datafusion-contrib/datafusion-distributed/blob/main/LICENSE.txt
[discord-badge]: https://img.shields.io/badge/Chat-Discord-purple
[discord-url]: https://discord.com/invite/Qw5gKqHxUM
