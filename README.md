# DataFusion Distributed

Library for building distributed query execution engines based on [Apache DataFusion](https://github.com/apache/datafusion).

> [!NOTE]  
> This project is not part of Apache DataFusion

## Quickstart

Starting with the following dependencies:

<details>
<summary><code>Cargo.toml</code></summary>

```toml
[package]
name = "datafusion-distributed-quick-start"
version = "0.1.0"
edition = "2024"
default-run = "main"

[dependencies]
datafusion = "54"
datafusion-distributed = "2"
object_store = { version = "0.13.2", features = ["http"] }
tokio = { version = "1", features = ["full"] }
tonic = "0"
url = "2"
alloc-no-stdlib = "=2.0.4"

[patch.crates-io]
# https://github.com/dropbox/rust-brotli/issues/256, issue unrelated to this project, this will stop being
# necessary as sson as rust-brotli fixes it.
alloc-no-stdlib = { git = "https://github.com/dropbox/rust-alloc-no-stdlib", rev = "6032b6a9b20e03737135c55a0270ccffcc1438ef" }
alloc-stdlib = { git = "https://github.com/dropbox/rust-alloc-no-stdlib", rev = "6032b6a9b20e03737135c55a0270ccffcc1438ef" }

[[bin]]
name = "main"
path = "src/main.rs"

[[bin]]
name = "worker"
path = "src/worker.rs"

```

</details>

---

`src/worker.rs`: Spawn a Distributed DataFusion worker in a localhost port.

<details>
<summary><code>use</code> statements</summary>

```rust
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion_distributed::Worker;
use object_store::http::HttpBuilder;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tonic::transport::Server;
use url::Url;
```

</details>

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Spawn a Distributed DataFusion worker in a localhost port.
    let base = Url::parse("https://datasets.clickhouse.com")?;
    let store = HttpBuilder::new().with_url(base.clone()).build()?;
    let runtime = RuntimeEnvBuilder::new().build_arc()?;
    runtime.register_object_store(&base, Arc::new(store));

    let worker = Worker::default().with_runtime_env(runtime);

    let port = std::env::var("PORT")?.parse()?;
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    println!("Distributed DataFusion worker listening on {addr}...");
    Ok(Server::builder()
        .add_service(worker.into_worker_server())
        .serve(addr)
        .await?)
}
```

--- 

`src/main.rs`: Prepare the `SessionContext` with all the pieces necessary to communicate with the workers above.

<details>
<summary><code>use</code> statements</summary>

```rust
use datafusion::error::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_distributed::{
    DistributedExt, SessionStateBuilderExt, WorkerResolver, display_plan_ascii,
};
use object_store::http::HttpBuilder;
use std::sync::Arc;
use url::Url;
```

</details>

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 2. Create a WorkerResolver implementation that knows how to resolve
    //    Distributed DataFusion workers running remotely.
    let workers = std::env::var("WORKERS").unwrap_or_default();
    let mut urls: Vec<Url> = vec![];
    for port in workers.split(",").filter(|v| !v.is_empty()) {
        urls.push(Url::parse(&format!("http://127.0.0.1:{port}"))?);
    }

    struct LocalhostWorkerResolver(Vec<Url>);
    impl WorkerResolver for LocalhostWorkerResolver {
        fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
            Ok(self.0.clone())
        }
    }

    // 3. Build the SessionContext as usual. Distributed queries will use
    //    this SessionContext as a "coordinator", as it'll be in charge of
    //    distributed planning + fanning out tasks to workers.
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::new().with_information_schema(true))
        .with_distributed_worker_resolver(LocalhostWorkerResolver(urls))
        .with_distributed_planner()
        // A very low value forces queries to be heavily distributed.
        .with_distributed_file_scan_config_bytes_per_partition(1)?
        .build();

    let ctx = SessionContext::from(state);

    let base = Url::parse("https://datasets.clickhouse.com")?;
    let store = HttpBuilder::new().with_url(base.clone()).build()?;
    ctx.register_object_store(&base, Arc::new(store));
    ctx.register_parquet(
        "hits",
        "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet",
        ParquetReadOptions::default(),
    )
        .await?;

    // 4. Issue the SQL query, and get a nice visualization for distributed plans 
    //    with `display_plan_ascii`.
    let sql = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    let df = ctx.sql(&sql).await?;
    let plan = df.create_physical_plan().await?;
    println!("{}", display_plan_ascii(plan.as_ref(), false));
    df.show().await?;

    Ok(())
}
```

---

Start a couple of workers, each on its own port:

```sh
PORT=8080 cargo run --bin worker &
PORT=8081 cargo run --bin worker &
PORT=8082 cargo run --bin worker &
```

Then point the main script at them and run a query:

```sh
WORKERS=8080,8081,8082 cargo run -- "SELECT COUNT(*), AVG(\"ResolutionWidth\") FROM hits"
```

You'll see the distributed plan printed, followed by the query results.

## Benchmarks

DataFusion Distributed consistently outperforms other distributed query engines across TPC-H and
TPC-DS. The chart below shows how much slower each engine is relative to DataFusion Distributed
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

**Conditions.** All engines ran on the same cluster: 12 AWS EC2 `c5n.2xlarge` instances (8 vCPUs and
21 GiB of memory each, with up to 25 Gbps networking) reading Parquet files stored in Amazon S3. Each
engine's total is the sum of per-query median (p50) latencies over the queries that all compared engines
completed successfully; lower is better.

The benchmarking code is public an open for anyone to easily reproduce. It uses AWS CDK for automating the creation
of the benchmarking cluster so that anyone can reproduce the same results in their own AWS account. The code can
be found in the [benchmarks/cdk](./benchmarks/cdk) directory.

## What can you do with this crate?

This crate is a toolkit that extends [Apache DataFusion](https://github.com/apache/datafusion) with distributed capabilities, providing a developer 
experience as close as possible to vanilla DataFusion while being unopinionated about the networking stack.

It's not an out of the box distributed engine, it's instead a library for building distributed query engines with some
sane defaults for when the data sources are just files.

Users of this library can expect to take their existing single-node DataFusion-based systems and add distributed
capabilities with minimal changes.

## Core tenets of the project

- Be as close as possible to vanilla DataFusion, providing a seamless integration with existing DataFusion systems and
  a familiar API for building applications.
- Unopinionated about networking. This crate does not take any opinion about the networking stack, and users are
  expected to leverage their own infrastructure for hosting DataFusion nodes.
- No coordinator-worker split. To keep infrastructure simple, any node can act as a coordinator or a worker.

# Docs

The user guide can be found here:

https://datafusion-contrib.github.io/datafusion-distributed

If you'd like to contribute, see the contributor guide:

https://datafusion-contrib.github.io/datafusion-distributed/contributor-guide/index.html
