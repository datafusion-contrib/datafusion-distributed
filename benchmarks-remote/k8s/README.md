# Local Kubernetes benchmarks

Kubernetes hosts the engine workers only. The benchmark harness stays on the
developer machine and uses the same TypeScript entry points and result files as
the existing EC2 benchmarks.

From `benchmarks-remote/pulumi`, create or update the foundation and install the
tenancy resources:

```bash
npm install
npm run deploy
```

The deploy command writes ignored Pulumi outputs to `.pulumi-outputs.json`, so
the benchmark scripts do not need copied instance IDs, bucket names, or ASG
names.

From `benchmarks-remote/cdk`, build and publish only the DataFusion worker image:

```bash
npm install
npm run datafusion-deploy:k8s
```

Then run the existing benchmark harness locally. Arguments after `--` are
passed unchanged to `datafusion-bench`:

```bash
npm run datafusion-bench:k8s -- --dataset tpch_sf1 --iterations 1
```

The wrapper scales the dedicated DataFusion pool, deploys the worker DaemonSet,
opens an SSM tunnel to one stable coordinator, invokes local
`npm run datafusion-bench`, uploads the local result files, and always removes
the workload and returns the pool to zero capacity.

To delete and recreate the whole benchmark foundation, run these commands from
`benchmarks-remote/pulumi`:

```bash
npm run destroy
npm run deploy
```

The external S3 bucket holding Pulumi state is intentionally not owned by the
stack and remains available for recreation.
