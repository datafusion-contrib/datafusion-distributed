# Remote benchmarks

This directory contains the remote benchmark runner and the infrastructure used
to run distributed benchmarks on Kubernetes.

- `pulumi/` provisions the AWS and k3s foundation.
- `k8s/` contains the Kubernetes workloads and lifecycle scripts.
- `cdk/` contains the existing local TypeScript benchmark client and the legacy
  CDK deployment code. The Kubernetes workflow reuses the client but provisions
  infrastructure through Pulumi.
- `worker/` is the dedicated crate containing the remote DataFusion worker
  binary.

See [`pulumi/README.md`](./pulumi/README.md) to provision the cluster and
[`k8s/README.md`](./k8s/README.md) to publish and run a benchmark workload.
