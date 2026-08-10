# Pulumi benchmark foundation

This project owns the AWS infrastructure used by the Kubernetes benchmarks.
It creates an EKS Auto Mode cluster. Engine workloads and benchmark execution
remain outside the foundation and are driven by the local npm commands.

The stack also creates:

- a two-AZ VPC with private engine nodes;
- encrypted dataset and result buckets;
- one immutable ECR repository per engine;
- an IAM role associated with each engine service account through EKS Pod
  Identity; and
- a CodeBuild project for publishing the Spark image.

EKS Auto Mode provisions nodes only for scheduled benchmark pods. The local
benchmark wrapper uninstalls the selected engine after a run, allowing the
cluster to return to zero worker nodes.

## Prerequisites

- Node.js 22 or newer.
- Pulumi CLI matching the `@pulumi/pulumi` major version in `package.json`.
- AWS CLI credentials with permission to manage the stack resources.
- A selected Pulumi backend, or `PULUMI_BACKEND_URL` pointing to an S3 backend
  or Pulumi Cloud organization.
- The AWS KMS alias `alias/datafusion-bench-pulumi-state` for encrypting Pulumi
  state when using an object-storage backend.

Install dependencies and create or update the stack:

```bash
npm install
npm run foundation-deploy
```

`PULUMI_BIN` may point to a downloaded Pulumi binary when it is not on `PATH`.
The deploy script uses the caller's existing AWS credentials and Pulumi backend,
writes ignored stack outputs to `.pulumi-outputs.json`, installs the stable
Kubernetes tenancy resources after EKS is ready, and writes the ignored
worktree-local kubeconfig at `../k8s/.kubeconfig`.

## Stack configuration

The region and network defaults are committed in `Pulumi.benchmark.yaml`.
Credentials, account selection, and state backend choices stay in the caller's
local AWS and Pulumi configuration. Object-storage stacks use the KMS alias
`alias/datafusion-bench-pulumi-state` by default. `PULUMI_SECRETS_PROVIDER` can
select a different Pulumi secrets provider.

Optional configuration:

| Key                         | Default            | Purpose                               |
| --------------------------- | ------------------ | ------------------------------------- |
| `namePrefix`                | `datafusion-bench` | Prefix for physical AWS resources.    |
| `benchmarkInstanceType`     | `c5n.2xlarge`      | Measured engine instance type.        |
| `benchmarkNodeCount`        | `12`               | Default worker replica count.         |
| `systemInstanceType`        | `m6i.large`        | Coordinator pod instance type.        |
| `eksVersion`                | `1.36`             | Exact EKS Kubernetes minor release.   |
| `kubernetesApiAllowedCidrs` | required           | Trusted CIDRs for the public EKS API. |

`KUBERNETES_API_ALLOWED_CIDRS` can provide a comma-separated list. When it is
unset, `npm run foundation-deploy` detects the current public IP and restricts the endpoint
to that `/32`.

## Multiple foundations

`benchmark` is the default stack used for interactive runs. A human can
provision an isolated foundation for automation by creating and configuring a
second stack, then selecting it through `PULUMI_STACK`:

```bash
cd benchmarks-remote/pulumi
pulumi stack init pr-bot --secrets-provider awskms://alias/datafusion-bench-pulumi-state?region=us-east-1
pulumi config set --stack pr-bot namePrefix datafusion-pr-bot
cd ..
PULUMI_STACK=pr-bot npm run foundation-deploy
```

Non-default stacks write ignored, stack-specific files such as
`pulumi/.pulumi-outputs.pr-bot.json` and `k8s/.kubeconfig.pr-bot`. They do not
replace the interactive stack's local configuration. Use the same stack name
for explicit teardown:

```bash
PULUMI_STACK=pr-bot npm run foundation-destroy
```

The state backend is a bootstrap dependency and cannot be owned by the stack
whose state it holds. Set `PULUMI_BACKEND_URL` to have the lifecycle scripts log
in explicitly, or select a backend with `pulumi login` beforehand.

Stack resources are protected during normal operation. To intentionally delete
the complete managed footprint, including datasets, results, and engine
images, run:

```bash
npm run foundation-destroy
```

The external state bucket and stack configuration remain, so `npm run foundation-deploy`
can recreate everything from scratch.

## Local validation

```bash
npm run format
npm run build
npm test
```
