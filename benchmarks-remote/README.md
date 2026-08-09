# Remote benchmarks

This directory contains the worker and tooling for benchmarks that run on
remote infrastructure.

- `cdk/` contains the AWS deployment and the local TypeScript benchmark client.
- `src/` contains the remote DataFusion worker binary.

See [`cdk/README.md`](./cdk/README.md) for setup and usage instructions.
