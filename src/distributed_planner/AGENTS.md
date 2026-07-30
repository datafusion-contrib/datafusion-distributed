# Distributed planner guide

This guide applies only to `src/distributed_planner/`. It supplements the
repository-root `AGENTS.md`; do not repeat repository-wide conventions here.

## Scope

This module converts DataFusion physical plans into distributed plans. It owns
network-boundary insertion, distribution-oriented rewrites, distributed planner
configuration, and planning statistics.

## Guidelines

While contributing to this part of the codebase, the following should be taken
into account:

### No topology modifications inside [inject_network_boundaries.rs](./inject_network_boundaries.rs)

The `inject_network_boundaries` should only be in charge of injecting 
network boundaries, it should never change the shape or topology of the plan,
and it should never perform replacement of some nodes by their distributed
variants.

Any plan that reaches here should be a valid executable single-node plan.

### Preparatory modifications to the single-node plan

Sometimes, the single-node plan needs to be prepared for distributed execution
before injecting network boundaries.

For example, inserting `BroadcastExec` nodes under the build side of 
`CollectLeft` joins.

All these preparatory modifications of the plan should still leave a perfectly
executable single-node plan, there should be no undesired intermediate 
unexecutable states.
