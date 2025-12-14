# Getting Started

Distributed DataFusion, rather than taking opinions on your setup and how you are serving queries to users,
it allows you to plug in your own networking stack and spawn your own gRPC servers that act as workers in the cluster.

This project heavily relies on the [Tonic](https://github.com/hyperium/tonic) ecosystem for the networking layer.
Users of this library are responsible for building their own Tonic server, adding the Arrow Flight distributed
DataFusion service to it and spawning it in a port so that it can be reached by other workers in the cluster.

The best way of getting started is to check out the available examples:

- [In-memory cluster example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/in_memory.md)
- [Localhost cluster example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/localhost.md)

A more advanced example can be found in the benchmarks that use a cluster of distributed DataFusion workers
deployed in AWS EC2 machines:

- [AWS EC2 based cluster example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/benchmarks/cdk/bin/worker.rs)

Each feature in the project is showcased and tested in its own isolated integration test, so it's recommended to
follow those for understanding better how specific features work:

- [Pass your own ConfigExtension implementations across network boundaries](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/tests/custom_config_extension.rs)
- [Provide custom protobuf codecs for your own nodes](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/tests/custom_extension_codec.rs)
- [Provide a custom TaskEstimator for controlling the amount of parallelism](TODO)

