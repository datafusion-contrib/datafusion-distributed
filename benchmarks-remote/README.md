# Remote benchmarks

This directory contains the local benchmark clients and the infrastructure used
to run distributed benchmarks on Kubernetes.

- `src/` contains the local TypeScript benchmark clients.
- `pulumi/` provisions the AWS and EKS foundation.
- `k8s/` contains the engine charts and lifecycle scripts.
- `engines/` contains engine-specific runtime sources.

The foundation, engine workloads, datasets, and benchmark runs have independent
lifecycles. Benchmark commands never provision infrastructure, install engines,
upload datasets, or remove workloads.

## AWS authentication

The commands use the caller's AWS CLI credentials. For an AWS SSO profile,
authenticate and select it before running any npm or kubectl command:

```bash
export AWS_PROFILE=<your-sso-profile>
export AWS_REGION=us-east-1
aws sso login
aws sts get-caller-identity
```

Add the two `export` lines to `~/.zshrc` to make that profile and region the
defaults for new zsh sessions. SSO sessions still expire, so rerun
`aws sso login` when AWS reports missing or expired credentials.

## Foundation lifecycle

Install dependencies and create the EKS cluster, buckets, registries, IAM
resources, and Kubernetes tenancy:

```bash
npm install
npm run foundation-deploy
```

The foundation remains deployed until it is explicitly destroyed:

```bash
npm run foundation-destroy
```

Foundation destruction removes all stack-owned resources, including datasets,
results, and published engine artifacts.

## Dataset lifecycle

List and upload local datasets independently from engines and benchmark runs:

```bash
npm run sync-bucket -- --list
npm run sync-bucket -- --dataset tpch/sf1
npm run sync-bucket -- --dataset tpch/sf1 tpcds/sf10
```

Running `npm run sync-bucket` without `--dataset` uploads every locally
available benchmark dataset. Delete selected datasets explicitly with:

```bash
npm run dataset-destroy -- --dataset tpch/sf1 --yes
```

Dataset names are their paths relative to `testdata/`, such as `tpch/sf10` or
`clickbench/0-100`. Linked Git worktrees automatically reuse generated datasets from the primary
checkout. Set `BENCHMARK_TESTDATA_ROOT` to use a different shared `testdata/`
directory explicitly.

Dataset sync and destroy commands can be rerun after interruption to converge
the contents stored in S3.

## Engine lifecycle

Deploy only the engines needed for a benchmark session:

```bash
npm run datafusion-deploy
npm run trino-deploy
npm run spark-deploy
npm run ballista-deploy
```

Each deploy command publishes any required engine artifacts and installs or
updates that engine's Helm release. The release and its EKS capacity remain
running across benchmark invocations until explicitly destroyed:

```bash
npm run datafusion-destroy
npm run trino-destroy
npm run spark-destroy
npm run ballista-destroy
```

Helm upgrades are atomic and clean up failed revisions. Content-addressed
artifacts and local deployment metadata are also written atomically, so an
interrupted deploy can be rerun safely. Engine and foundation destroy commands
are idempotent and can likewise be rerun after interruption.

## Running benchmarks

An engine and the requested dataset must already be deployed. Benchmark commands
only acquire the run lock, open a local Kubernetes port-forward, execute the
local client, and release the lock:

```bash
npm run datafusion-bench -- --dataset tpch/sf1 --iterations 1
npm run trino-bench -- --dataset tpch/sf1 --iterations 1
npm run spark-bench -- --dataset tpch/sf1 --iterations 1
npm run ballista-bench -- --dataset tpch/sf1 --iterations 1
```

`npm run command -- <engine> <command>` runs a diagnostic command in a worker
pod. `npm run compare` compares locally stored result sets.

### Interrupting a run

Benchmark runs can be stopped with `Ctrl-C` or by terminating the local
command. The runner stops its port-forward and lock heartbeat and removes the
cluster lock before exiting. Results from the previous completed run remain
available when a benchmark is interrupted before it writes new results.

Benchmark results remain under the dataset's local `.results-remote/` directory
and are not uploaded to S3.

The cluster lock expires after five minutes without a heartbeat. This makes a
new run recover automatically even when the previous process could not run its
cleanup handler, such as after a terminal or machine failure. An active lock is
never replaced, and lock release is conditional on run ownership.

## Kubectl access

The lifecycle scripts keep their kubeconfig at `k8s/.kubeconfig`. From this
directory, use it with normal kubectl commands:

```bash
export KUBECONFIG="$PWD/k8s/.kubeconfig"
kubectl get nodes
```

To add the cluster to the default user-wide kubeconfig instead, run:

```bash
aws eks update-kubeconfig --region us-east-1 --name datafusion-bench-eks
```

See [`pulumi/README.md`](./pulumi/README.md) for foundation details and
[`k8s/README.md`](./k8s/README.md) for Kubernetes workload details.
