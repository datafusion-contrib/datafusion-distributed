# DataFusion Distributed

Library that brings distributed execution capabilities to [Apache DataFusion](https://github.com/apache/datafusion).

## What can you do with this crate?

This crate is a toolkit that extends [Apache DataFusion](https://github.com/apache/datafusion) with distributed
capabilities,
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

# Benchmarks

![dist-df-vs-df-vs-trino.png](docs/source/_static/images/dist-df-vs-df-vs-trino.png)

# Docs

The user and contributor guide can be found here:

https://datafusion-contrib.github.io/datafusion-distributed

## Getting familiar with distributed DataFusion

There are some runnable examples showcasing how to provide a localhost implementation for Distributed DataFusion in
[examples/](examples):

- [localhost_worker.rs](examples/localhost_worker.rs): code that spawns an Arrow Flight Endpoint listening for physical
  plans over the network.
- [localhost_run.rs](examples/localhost_run.rs): code that distributes a query across the spawned Arrow Flight Endpoints
  and executes it.

The integration tests also provide an idea about how to use the library and what can be achieved with it:

- [tpch_validation_test.rs](tests/tpch_plans_test.rs): executes all TPCH queries and performs assertions over the
  distributed plans.
- [custom_config_extension.rs](tests/custom_config_extension.rs): showcases how to propagate custom DataFusion config
  extensions.
- [custom_extension_codec.rs](tests/custom_extension_codec.rs): showcases how to propagate custom physical extension
  codecs.
- [distributed_aggregation.rs](tests/distributed_aggregation.rs): showcases how to manually place `ArrowFlightReadExec`
  nodes in a plan and build a distributed query out of it.
