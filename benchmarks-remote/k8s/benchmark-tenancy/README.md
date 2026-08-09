# Kubernetes benchmark tenancy

This Helm chart creates the stable Kubernetes boundary shared by benchmark
runs. It does not deploy an engine or provision EKS worker nodes.

Each engine has its own namespace, service account, workload, and ingress
boundary. Engine namespaces accept ingress only from pods in the same
namespace, so Spark, Trino, Ballista, and DataFusion cannot talk to one another
during a run. The benchmark harness runs locally and manages workloads through
the authenticated infrastructure wrapper.

Install or update the chart after the EKS cluster exists:

```bash
helm upgrade --install benchmark-tenancy ./benchmarks-remote/k8s/benchmark-tenancy
```

The namespaces carry `helm.sh/resource-policy: keep`. Uninstalling the chart
therefore does not implicitly delete engine workloads or logs; the local
benchmark wrapper is responsible for run-scoped cleanup.

Validate rendered resources locally with:

```bash
./benchmarks-remote/k8s/benchmark-tenancy/test/render.sh
```
