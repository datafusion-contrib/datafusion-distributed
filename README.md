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
  children. Implemented as an `ArrowFlightReadExec` physical DataFusion node.
- `stage`: a portion of the plan separated by a network boundary from other parts of the plan. Implemented as any
  other physical node in DataFusion.
- `task`: a unit of work inside a stage that executes a subset of its partitions in a specific worker.
- `subplan`: a slice of the overall plan

A distributed DataFusion query is executed in a very similar fashion as a normal DataFusion query with one key
difference:

The physical plan is divided into stages, each stage is assigned tasks that run in parallel in different workers. All of
this is done at the physical plan level, and is implemented as a `PhysicalOptimizerRule` that:

1. Inspects the non-distributed plan, placing network boundaries (`ArrowFlightReadExec` nodes) in the appropriate places
2. Based on the placed network boundaries, divides the plan into stages and assigns tasks to them.

For example, imagine we have a plan that looks like this:

```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │       (final)        │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    RepartionExec     │
   │ (3 input partitions) │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │      (partial)       │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    DataSourceExec    │
   └──────────────────────┘  
```

We want to distribute the aggregation to something like this:

```
                              ┌──────────────────────┐
                              │    ProjectionExec    │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │    AggregateExec     │
                              │       (final)        │
                              └──────────────────────┘  
                                       ▲ ▲ ▲
              ┌────────────────────────┘ │ └─────────────────────────┐
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │    AggregateExec     │   │    AggregateExec     │   │    AggregateExec     │
   │      (partial)       │   │      (partial)       │   │      (partial)       │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
              ▲                          ▲                           ▲
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │    DataSourceExec    │   │    DataSourceExec    │   │    DataSourceExec    │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
```

The first step is to place the `ArrowFlightReadExec` network boundary in the appropriate place (the following drawing
shows the partitioning scheme in each node):

```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └─────────[0]──────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │       (final)        │
   └─────────[0]──────────┘  
              ▲
   ┌──────────┴───────────┐
   │   ArrowFlightRead    │   <- this node was injected to tell the distributed planner 
   │   (3 input tasks)    │      that there must be a network boundary here.
   └──────[0][1][2]───────┘  
           ▲  ▲  ▲
   ┌───────┴──┴──┴────────┐
   │    AggregateExec     │
   │      (partial)       │
   └──────[0][1][2]───────┘  
           ▲  ▲  ▲
   ┌───────┴──┴──┴────────┐
   │    DataSourceExec    │
   └──────[0][1][2]───────┘  
```

Based on that boundary, the plan is divided into stages, and tasks are assigned to each stage. Each task will be
responsible for the different partitions in the original plan.

```                            
                              ┌────── (stage 2) ───────┐
                              │┌──────────────────────┐│
                              ││    ProjectionExec    ││
                              │└──────────┬───────────┘│
                              │┌──────────┴───────────┐│
                              ││    AggregateExec     ││
                              ││       (final)        ││
                              │└──────────┬───────────┘│
                              │┌──────────┴───────────┐│
                              ││  ArrowFlightReadExec ││
                              │└──────[0][1][2]───────┘│
                              └─────────▲─▲─▲──────────┘
               ┌────────────────────────┘ │ └─────────────────────────┐            
               │                          │                           │            
   ┌─── task 0 (stage 1) ───┐ ┌── task 1 (stage 1) ────┐ ┌── task 2 (stage 1) ────┐
   │           │            │ │           │            │ │            │           │
   │┌─────────[0]──────────┐│ │┌─────────[1]──────────┐│ │┌──────────[2]─────────┐│
   ││    AggregateExec     ││ ││    AggregateExec     ││ ││    AggregateExec     ││
   ││      (partial)       ││ ││      (partial)       ││ ││      (partial)       ││
   │└──────────┬───────────┘│ │└──────────┬───────────┘│ │└───────────┬──────────┘│
   │┌─────────[0]──────────┐│ │┌─────────[1]──────────┐│ │┌──────────[2]─────────┐│
   ││   DataSourceExec     ││ ││   DataSourceExec     ││ ││   DataSourceExec     ││
   │└──────────────────────┘│ │└──────────────────────┘│ │└──────────────────────┘│
   └────────────────────────┘ └────────────────────────┘ └────────────────────────┘
```

The plan is immediately executable, and the same process that planned the distributed query can start executing the head
stage (stage 2). The `ArrowFlightReadExec` in that stage will know from which tasks to gather data from stage 1, and
will issue 3 concurrent Arrow Flight requests to the appropriate physical nodes.

This means that:

1. The head stage is executed normally as if the query was not distributed.
2. Upon calling `.execute()` on `ArrowFlightReadExec`, instead of propagating the `.execute()` call on its child,
   the subplan is serialized and sent over the wire to be executed on another worker.
3. The next node, which is hosting an Arrow Flight Endpoint listening for gRPC requests over an HTTP server, will pick
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
