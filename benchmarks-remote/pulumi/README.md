# Pulumi benchmark foundation

This project owns the AWS infrastructure used by the Kubernetes benchmarks.
It creates one small k3s control-plane instance and a dedicated zero-sized EC2
Auto Scaling group for each engine: DataFusion Distributed, Ballista, Spark,
and Trino. Engine images, workloads, and benchmark execution remain outside the
foundation.

The stack also creates:

- a two-AZ VPC with private engine nodes;
- encrypted dataset and result buckets;
- one immutable ECR repository per engine; and
- the EC2 and SSM permissions needed by the k3s server and workers.

Every engine group starts with `minSize=0` and `desiredCapacity=0`. The local
benchmark wrapper changes only the selected engine group and always returns it
to zero.

## Prerequisites

- Node.js 22 or newer.
- Pulumi CLI matching the `@pulumi/pulumi` major version in `package.json`.
- AWS CLI credentials with permission to manage the stack resources.
- An S3 Pulumi state bucket created separately from this stack, or a Pulumi
  Cloud organization.

Install dependencies and create or update the stack:

```bash
npm install
npm run deploy
```

`PULUMI_BIN` may point to a downloaded Pulumi binary when it is not on `PATH`.
The deploy script uses the caller's existing AWS credentials, writes ignored
stack outputs to `.pulumi-outputs.json`, and installs the stable Kubernetes
tenancy resources after k3s is ready.

## Stack configuration

Create the stack and configure the network once:

```bash
pulumi stack init benchmark
pulumi config set aws:region us-east-1
pulumi config set --path 'availabilityZones[0]' us-east-1a
pulumi config set --path 'availabilityZones[1]' us-east-1b
pulumi config set --path 'publicSubnetCidrs[0]' 10.42.0.0/24
pulumi config set --path 'publicSubnetCidrs[1]' 10.42.1.0/24
pulumi config set --path 'privateSubnetCidrs[0]' 10.42.10.0/24
pulumi config set --path 'privateSubnetCidrs[1]' 10.42.11.0/24
pulumi config set k3sVersion v1.35.1+k3s1
```

Optional configuration:

| Key                             | Default            | Purpose                                  |
| ------------------------------- | ------------------ | ---------------------------------------- |
| `namePrefix`                    | `datafusion-bench` | Prefix for physical AWS resources.       |
| `benchmarkInstanceType`         | `c5n.2xlarge`      | Measured engine instance type.           |
| `benchmarkNodeCount`            | `12`               | Maximum nodes in each engine group.      |
| `benchmarkRootVolumeSizeGiB`    | `200`              | Engine node root volume size.            |
| `benchmarkRootVolumeIops`       | `3000`             | Engine node gp3 IOPS.                    |
| `benchmarkRootVolumeThroughput` | `125`              | Engine node gp3 throughput in MiB/s.     |
| `systemInstanceType`            | `m6i.large`        | Persistent k3s server instance type.     |
| `k3sPodCidr`                    | `10.244.0.0/16`    | Pod CIDR; must not overlap the VPC CIDR. |

For an S3 backend:

```bash
pulumi login 's3://<state-bucket>/datafusion-distributed-benchmarks'
```

The state bucket cannot be owned by the stack that stores its state. The stack
resources are protected during normal operation. To intentionally delete the
complete managed footprint, including datasets, results, and engine images,
run:

```bash
npm run destroy
```

The external state bucket and stack configuration remain, so `npm run deploy`
can recreate everything from scratch.

## Local validation

```bash
npm run format
npm run build
npm test
```
