---
name: remote-engine-deployment
description: Manage persistent Kubernetes deployments for the DataFusion, Trino, Spark, and Ballista remote benchmark engines. Use when an agent needs to publish engine code or images, deploy or update an engine, inspect its Helm release and pods, run a diagnostic command, recover an interrupted deployment, or explicitly tear down an engine.
---

# Remote Engine Deployment

Operate from `benchmarks-remote`. Supported engine names are `datafusion`, `trino`, `spark`, and `ballista`.

## Preconditions

1. Use the caller-selected `AWS_PROFILE` and `AWS_REGION`; never hardcode an account, profile, or `aws-vault` wrapper.
2. Run `aws sts get-caller-identity`. If SSO has expired and `AWS_PROFILE` is set, run `aws sso login --profile "$AWS_PROFILE"` once; stop if authentication still fails.
3. Require `pulumi/.pulumi-outputs.json`. If it is missing, report that the foundation must be deployed; do not deploy it implicitly.

## Deploy or update

Run the selected engine command:

```bash
npm run datafusion-deploy
npm run trino-deploy
npm run spark-deploy
npm run ballista-deploy
```

Deploy commands publish content-addressed artifacts when required and perform an atomic Helm install or upgrade. DataFusion and Ballista publish Linux binaries, Spark publishes an ECR image through CodeBuild, and Trino uses its chart image directly. Rerun the same deploy command after interruption.

Keep the default 12 measured workers. Do not set `NODE_COUNT` or alter `worker-resources.yaml` unless the user requests a different benchmark shape. Every measured worker must use the shared node-filling resource configuration and one worker per benchmark node.

Deployment is persistent. Do not destroy the engine after deployment unless teardown was requested.

## Inspect and diagnose

Use the generated kubeconfig:

```bash
export KUBECONFIG="$PWD/k8s/.kubeconfig"
helm list --all-namespaces
kubectl get pods --namespace benchmark-<engine> -o wide
kubectl get nodes -o wide
```

Run a diagnostic command in a worker with:

```bash
npm run command -- <engine> <command> [arguments...]
```

Prefer pod status, events, and relevant container logs when a Helm readiness wait fails. Do not add permanent health-check orchestration to the deployment scripts.

## Destroy

Engine teardown is explicit and leaves the foundation and datasets intact:

```bash
npm run datafusion-destroy
npm run trino-destroy
npm run spark-destroy
npm run ballista-destroy
```

Resolve the exact engine and obtain authorization before teardown. The command refuses to destroy an engine while a benchmark owns the cluster run lock. EKS Auto Mode removes empty capacity asynchronously.

## Validate changes

Render every affected Helm chart with `k8s/worker-resources.yaml`, run `bash -n` for changed shell scripts, and run `npm test`. For runtime changes, deploy the affected engine and leave it installed unless teardown is part of the request.
