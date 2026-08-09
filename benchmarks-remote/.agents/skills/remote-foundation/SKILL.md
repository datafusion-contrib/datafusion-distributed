---
name: remote-foundation
description: Manage and verify the Pulumi-based AWS foundation for DataFusion Distributed remote benchmarks. Use when an agent needs to inspect, create, update, troubleshoot, destroy, or recreate the EKS cluster, networking, buckets, registries, IAM, CodeBuild resources, Kubernetes tenancy, Pulumi backend, or kubeconfig.
---

# Remote Foundation

Operate from `benchmarks-remote`. Keep the foundation lifecycle independent from datasets, engine deployments, and benchmark runs.

## Authenticate

1. Use the caller-selected `AWS_PROFILE` and `AWS_REGION`; never hardcode an account, profile, or `aws-vault` wrapper.
2. Run `aws sts get-caller-identity` before Pulumi or Kubernetes operations.
3. If SSO has expired and `AWS_PROFILE` is set, run `aws sso login --profile "$AWS_PROFILE"` once. Stop and request authentication if it does not succeed.
4. Do not set `PULUMI_CONFIG_PASSPHRASE`. The committed stack metadata uses the AWS KMS secrets provider.

## Inspect

- Read `pulumi/README.md` before changing the foundation.
- Treat `pulumi/.pulumi-outputs.json` and `k8s/.kubeconfig` as generated local files; never edit or commit them.
- Use `pulumi preview --stack benchmark` for a read-only infrastructure diff when Pulumi is on `PATH` and the required API CIDR is configured.
- Use `KUBECONFIG="$PWD/k8s/.kubeconfig" kubectl get nodes` and `helm list --all-namespaces` to inspect the deployed cluster.

## Create or update

Run only when foundation management was requested:

```bash
npm install
npm run foundation-deploy
```

The command updates Pulumi, writes stack outputs atomically, refreshes kubeconfig, and installs the stable Kubernetes namespaces and service accounts. Do not invoke it lazily from dataset, engine, or benchmark work.

After deployment, verify that the Pulumi command completed, the generated output file exists, the EKS API is reachable, and `benchmark-system` plus all engine namespaces exist. An empty node list is valid before an engine is deployed because EKS Auto Mode provisions capacity on demand.

## Destroy or recreate

Foundation destruction removes the cluster and all stack-owned bucket contents, datasets, result history, registries, and published artifacts. Run it only with explicit authorization:

```bash
npm run foundation-destroy
```

The external Pulumi backend and KMS configuration remain. To validate a clean lifecycle, run `foundation-destroy`, confirm the stack has no managed resources, then run `foundation-deploy` and repeat the post-deploy checks. Rerun the same lifecycle command if it was interrupted; do not manually edit Pulumi state.

## Validate changes

From `benchmarks-remote/pulumi`, run:

```bash
npm run build
npm test
bash -n deploy.sh destroy.sh
```

Use a live Pulumi preview for networking, IAM, EKS, storage, or provider changes. Never apply a replacement preview to a long-lived cluster unless that replacement is explicitly intended.
