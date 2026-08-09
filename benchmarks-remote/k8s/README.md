# Kubernetes engine workloads

Kubernetes hosts persistent engine services. Benchmark clients run on the
developer machine through the npm commands in `benchmarks-remote/package.json`.

Create the foundation before managing engines:

```bash
npm run foundation-deploy
```

Engine deploy commands publish required artifacts, install or update the Helm
release, and wait for it to become ready. For example:

```bash
npm run datafusion-deploy
npm run datafusion-bench -- --dataset tpch/sf1 --iterations 1
npm run datafusion-destroy
```

Benchmark commands require the engine release and dataset to exist. They do not
run Helm, upload datasets, or uninstall the engine. Deployments continue to use
EKS capacity between runs until their corresponding `<engine>-destroy` command
is invoked.

All measured worker and executor pods load
[`worker-resources.yaml`](./worker-resources.yaml). They request and limit the
same 7 CPUs and 17 GiB of memory, reserving the available benchmark capacity of
one `c5n.2xlarge` node per pod while leaving capacity for Kubernetes system
overhead. Every engine defaults to 12 worker replicas. Engine coordinators run
on the separate system-node type and are not part of the measured worker
capacity.

The scripts use `k8s/.kubeconfig`. Export it to connect directly:

```bash
export KUBECONFIG="$PWD/k8s/.kubeconfig"
kubectl get pods --all-namespaces
```

Destroy the complete AWS foundation only when its buckets, registries, results,
and cluster should also be removed:

```bash
npm run foundation-destroy
```
