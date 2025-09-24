# DataFusion Distributed

Library that brings distributed execution capabilities to [DataFusion](https://github.com/apache/datafusion).

> [!WARNING]
> This project is currently under construction and is not yet ready for production use.

## What can you do with this crate?

This crate is a toolkit that extends [DataFusion](https://github.com/apache/datafusion) with distributed capabilities,
providing a developer experience as close as possible to vanilla DataFusion while being unopinionated about the
networking stack used for hosting the different workers involved in a query.

Users of this library can expect to take their existing single-node DataFusion-based systems and add distributed
capabilities with minimal changes.

## Core tenets of the project

- Be as close as possible to vanilla DataFusion, providing a seamless integration with existing DataFusion systems and
  a familiar API for building applications.
- Unopinionated about networking. This crate does not take any opinion about the networking stack, and users are
  expected to leverage their own infrastructure for hosting DataFusion nodes.
- No coordinator-worker architecture. To keep infrastructure simple, any node can act as a coordinator or a worker.

## Architecture

Before diving into the architecture, it's important to clarify some terms and what they mean:

- `worker`: a physical machine listening to serialized execution plans over an Arrow Flight interface.
- `network boundary`: a node in the plan that streams data from a network interface rather than directly from its
  children. Implemented as DataFusion `ExecutionPlan`s: `NeworkShuffle` and `NetworkCoalesce`.
- `stage`: a portion of the plan separated by a network boundary from other parts of the plan.
  Implemented as a DataFusion `ExecutionPlan`: `StageExec`.
- `task`: a unit of work in a stage that executes the inner plan in parallel to other tasks.
- `subplan`: a slice of the overall plan

A distributed DataFusion query is executed in a very similar fashion as a normal DataFusion query with one key
difference:

The physical plan is divided into stages and each stage is assigned tasks that run the inner plan in parallel in
different workers. All of this is done at the physical plan level, implemented as a `PhysicalOptimizerRule` that:

1. Inspects the non-distributed plan, placing network boundaries (`NetworkShuffle` and `NetworkCoalesce` nodes) in
   the appropriate places.
2. Based on the placed network boundaries, divides the plan into stages and assigns tasks to them.

### Types of network boundaries

There are different flavors of network boundaries that can stream data over the wire across stages:

- `NetworkShuffle`: the equivalent to a `RepartitionExec` that spreads out different partitions to different tasks
  across stages. Refer to the [network_shuffle.rs](./src/execution_plans/network_shuffle.rs) docs for a more detailed
  explanation.
- `NetworkCoalesce`: the equivalent to a `CoalescePartitionsExec` but with tasks. It collapses P partitions across
  N tasks into a single task with N*P partitions. Refer to
  the [network_coalesce.rs](./src/execution_plans/network_coalesce.rs)
  docs for a more detailed explanation

### Example plan distribution

For example, imagine we have a plan that looks like this:

```
┌───────────────────────┐
│CoalescePartitionsExec │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│    ProjectionExec     │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│     AggregateExec     │
│        (final)        │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│    RepartitionExec    │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│     AggregateExec     │
│       (partial)       │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│    DataSourceExec     │
└───────────────────────┘
```

By looking at the existing nodes, we can decide to place some network boundaries like this:

```
┌───────────────────────┐
│CoalescePartitionsExec │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│  NetworkCoalesceExec  │ <- injected during distributed planning. This will collapse all tasks into one,
└───────────────────────┘    wihtout performing any changes in the overall partitioning scheme.
            ▲
            │
┌───────────────────────┐
│    ProjectionExec     │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│     AggregateExec     │
│        (final)        │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│  NetworkShuffleExec   │ <- injected during distributed planning. This will shuffle the data across the network,
└───────────────────────┘    fanning out the different partitions to different workers.
            ▲
            │
┌───────────────────────┐
│    RepartitionExec    │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│     AggregateExec     │
│       (partial)       │
└───────────────────────┘
            ▲
            │
┌───────────────────────┐
│ ParititonIsolatorExec │ <- injected during distributed planning. As this lower part of the plan will run in multiple
└───────────────────────┘    tasks in parallel, this node makes sure no two same partitions from the underlying 
            ▲                DataSourceExec are read in two different tasks.
            │
┌───────────────────────┐
│    DataSourceExec     │
└───────────────────────┘
```

Once the network boundaries are properly placed, the distributed planner will break down the plan into stages,
and will assign tasks to them:

```
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ Stage 3
                     ┌───────────────────────┐
│                    │CoalescePartitionsExec │                    │
                     └───────────────────────┘
│                                ▲                                │
                                 │
│                    ┌───────────────────────┐                    │
                     │  NetworkCoalesceExec  │
│                    └───────────────────────┘                    │
 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▲ ─ ▲ ─ ▲ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
            ┌────────────────┘   │   └────────────────┐
┌ ─ ─ ─ ─ ─ │ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │ ─ ─ Stage 2
  ┌───────────────────┐┌───────────────────┐┌───────────────────┐
│ │  ProjectionExec   ││  ProjectionExec   ││  ProjectionExec   │ │
  └───────────────────┘└───────────────────┘└───────────────────┘
│           ▲                    ▲                    ▲           │
            │                    │                    │
│ ┌───────────────────┐┌───────────────────┐┌───────────────────┐ │
  │   AggregateExec   ││   AggregateExec   ││   AggregateExec   │
│ │      (final)      ││      (final)      ││      (final)      │ │
  └───────────────────┘└───────────────────┘└───────────────────┘
│           ▲                    ▲                    ▲           │
            │                    │                    │
│ ┌───────────────────┐┌───────────────────┐┌───────────────────┐ │
  │NetworkShuffleExec ││NetworkShuffleExec ││NetworkShuffleExec │
│ └───────────────────┘└───────────────────┘└───────────────────┘ │
 ─ ─ ─ ─ ─ ─▲─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ▲ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─▲─ ─ ─ ─ ─ ─
            └────────┬───────────┴──────────┬─────────┘
┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─│─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ │ ─ ─ ─ ─ ─ ─ ─ Stage 1
          ┌─────────────────────┐┌─────────────────────┐
│         │   RepartitionExec   ││   RepartitionExec   │          │
          └─────────────────────┘└─────────────────────┘
│                    ▲                      ▲                     │
                     │                      │
│         ┌─────────────────────┐┌─────────────────────┐          │
          │    AggregateExec    ││    AggregateExec    │
│         │      (partial)      ││      (partial)      │          │
          └─────────────────────┘└─────────────────────┘
│                    ▲                      ▲                     │
                     │                      │
│         ┌─────────────────────┐┌─────────────────────┐          │
          │PartitionIsolatorExec││PartitionIsolatorExec│
│         └─────────────────────┘└─────────────────────┘          │
                     ▲                      ▲
│                    │                      │                     │
          ┌─────────────────────┐┌─────────────────────┐
│         │   DataSourceExec    ││   DataSourceExec    │          │
          └─────────────────────┘└─────────────────────┘
└ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
```

The plan is immediately executable, and the same process that planned the distributed query can start executing the head
stage (stage 3). The `NetworkCoalesceExec` in that stage will know from which tasks to gather data from stage 2, and
will issue 3 concurrent Arrow Flight requests to the appropriate physical nodes. Same goes from stage 2 to stage 1,
but with the difference that this time data is repartitioned and shuffled, that way each task in stage 2 handles a
different set of partitions.

This means that:

1. The head stage is executed normally as if the query was not distributed.
2. Upon calling `.execute()` on `NetworkCoalesceExec`, instead of propagating the `.execute()` call on its child,
   the subplan is serialized and sent over the wire to be executed on another worker.
3. The next worker, which is hosting an Arrow Flight Endpoint listening for gRPC requests over an HTTP server, will pick
   up the request containing the serialized chunk of the overall plan and execute it.
4. This is repeated for each stage, and data will start flowing from bottom to top until it reaches the head stage.

## Getting familiar with distributed DataFusion

There are some runnable examples showcasing how to provide a localhost implementation for Distributed DataFusion in
[examples/](examples):

- [localhost_worker.rs](examples/localhost_worker.rs): code that spawns an Arrow Flight Endpoint listening for physical
  plans over the network.
- [localhost_run.rs](examples/localhost_run.rs): code that distributes a query across the spawned Arrow Flight Endpoints
  and executes it.

The integration tests also provide an idea about how to use the library and what can be achieved with it:

- [tpch_validation_test.rs](tests/tpch_validation_test.rs): executes all TPCH queries and performs assertions over the
  distributed plans and the results vs running the queries in single node mode with a small scale factor.
- [custom_config_extension.rs](tests/custom_config_extension.rs): showcases how to propagate custom DataFusion config
  extensions.
- [custom_extension_codec.rs](tests/custom_extension_codec.rs): showcases how to propagate custom physical extension
  codecs.
- [distributed_aggregation.rs](tests/distributed_aggregation.rs): showcases how to manually place `ArrowFlightReadExec`
  nodes in a plan and build a distributed query out of it.

## Development

### Prerequisites

- Rust 1.85.1 or later (specified in `rust-toolchain.toml`)
- Git LFS for test data

### Setup

1. **Clone the repository:**
   ```bash
   git clone git@github.com:datafusion-contrib/datafusion-distributed
   cd datafusion-distributed
   ```

2. **Install Git LFS and fetch test data:**
   ```bash
   git lfs install
   git lfs checkout
   ```

### Running Tests

**Unit and integration tests:**

```bash
cargo test --features integration
```

### Running Examples

**Start localhost workers:**

```bash
# Terminal 1
cargo run --example localhost_worker -- 8080 --cluster-ports 8080,8081

# Terminal 2  
cargo run --example localhost_worker -- 8081 --cluster-ports 8080,8081
```

**Execute distributed queries:**

```bash
cargo run --example localhost_run -- 'SELECT count(*) FROM weather' --cluster-ports 8080,8081
```

### Benchmarks

**Generate TPC-H benchmark data:**

```bash
cd benchmarks
./gen-tpch.sh
```

**Run TPC-H benchmarks:**

```bash
cargo run -p datafusion-distributed-benchmarks --release -- tpch --path benchmarks/data/tpch_sf1
```

### Project Structure

- `src/` - Core library code
    - `flight_service/` - Arrow Flight service implementation
    - `plan/` - Physical plan extensions and operators
    - `stage/` - Execution stage management
    - `common/` - Shared utilities
- `examples/` - Usage examples
- `tests/` - Integration tests
- `benchmarks/` - Performance benchmarks
- `testdata/` - Test datasets
