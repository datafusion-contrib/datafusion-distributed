# Remote benchmark agent guide

## Scope

This directory owns remote benchmark clients, AWS/EKS foundation code,
Kubernetes engine workloads, and engine-specific runtime sources. Keep its four
lifecycles independent:

- foundation creation and destruction;
- dataset synchronization and removal;
- persistent engine deployment and teardown; and
- benchmark execution from the developer machine.

Never make a benchmark command provision infrastructure, synchronize data,
deploy an engine, or tear one down.

## Operational skills

Use the matching scoped skill before operating live infrastructure:

- [remote-datasets](./.agents/skills/remote-datasets/SKILL.md): discover, sync,
  and explicitly remove datasets.
- [remote-foundation](./.agents/skills/remote-foundation/SKILL.md): inspect,
  deploy, destroy, or recreate the Pulumi/EKS foundation.
- [remote-engine-deployment](./.agents/skills/remote-engine-deployment/SKILL.md):
  publish, deploy, inspect, diagnose, and explicitly destroy engines.
- [remote-benchmark-run](./.agents/skills/remote-benchmark-run/SKILL.md): run
  and assess benchmarks against an existing deployment.

Use the caller's AWS configuration. Do not commit account IDs, profile names,
credentials, generated Pulumi outputs, kubeconfig, or runtime artifact metadata.

## Adding an engine

Follow the existing engine boundary instead of adding special cases to the
generic benchmark wrapper:

1. Add runtime sources under `engines/<engine>/` only when the engine needs a
   project-owned adapter or binary.
2. Add a Helm chart under `k8s/<engine>/`. Expose the benchmark HTTP endpoint as
   service `<engine>` on port 9000.
3. Load measured worker resources from `k8s/worker-resources.yaml`, default to
   12 workers, and retain required pod anti-affinity so workers occupy distinct
   benchmark nodes.
4. Add the namespace and pod-identity association through the shared engine
   list in `pulumi/src/config.ts` and the tenancy chart.
5. Add content-addressed publishing only when required. Keep publishing in the
   engine deployment path, not benchmark execution.
6. Add `<engine>-deploy`, `<engine>-destroy`, `<engine>-bench`, and
   `runner:<engine>-bench` npm commands following the existing naming scheme.
7. Implement the local client under `src/`; keep results local and use literal
   dataset paths.
8. Extend the generic engine validation lists and focused tests. Do not add
   per-engine port selection or lazy readiness, dataset, or deployment logic to
   `k8s/run-benchmark.sh`.

## Validation

Run the narrowest checks first. For changes spanning the remote harness, run:

```bash
npm run build
npm test
```

Render affected Helm charts with `k8s/worker-resources.yaml`, check changed
shell scripts with `bash -n`, and run a live single-query benchmark for every
affected engine. Infrastructure destruction always requires explicit user
authorization.
