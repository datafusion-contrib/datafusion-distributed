# Sharing Top-K bounds between tasks

Set `distributed.enable_sibling_topk_filter_pushdown` to share dynamic bounds
between tasks executing the same fetched sort. It is disabled by default and
uses the existing distributed configuration propagation to workers.

```rust
ctx.sql("SET distributed.enable_sibling_topk_filter_pushdown = true")
    .await?;
```

Both the coordinator and every participating worker must run a version that
supports this feature before enabling it. The unchanged message format does not
make opt-in sharing compatible with older workers.

Sharing also requires `distributed.remote_dynamic_filters`,
`datafusion.optimizer.enable_dynamic_filter_pushdown`, and
`datafusion.optimizer.enable_topk_dynamic_filter_pushdown`. These settings are
enabled by default. A sort must already have a dynamic filter consumed within
its task. Additional bounds follow the plan's existing filter-routing hooks and
are offered to data sources through `DataSource::try_pushdown_filters`. This also
supports custom data sources implementing that API. Unsupported paths keep their
existing local filtering. Sharing does not insert filter operators or change
plan topology.

As a local Top-K heap fills, its retained candidates establish a safe bound for
the remaining input. Other tasks receive those bounds through a separate dynamic
filter; their local producer state stays independent. The coordinator combines
available task predicates with `AND`. A received snapshot may include the
recipient's own earlier bound, which is redundant with its current local bound.
Received state is never published as a local producer update. Join filters keep
their existing merge rules.

The shared predicate preserves DataFusion's ordering expressions, NULL ordering,
and ordinary `LIMIT` tie semantics. A complete tie-breaker is necessary when a
query requires deterministic selection among equal sort keys. Sharing does not
compute the exact global kth row early: the final merge still selects the result.

Updates arrive asynchronously and can only affect remaining work. Publishing and
distributing the merged bounds adds coordinator and network work, so the tradeoff
depends on the workload. Enabling the option or completing a query does not by
itself demonstrate that a remote bound pruned data.

The merged predicate retains the latest bound from each producer. With `N`
producers and `N` recipients, copying that predicate to every recipient takes
`O(N²)` payload work per merged update when individual bounds have comparable
sizes. Receivers also decode and remap the predicate. Slow receivers can retain
obsolete snapshots in the existing unbounded control-message queues. These
expression and message allocations are outside Arrow memory-pool accounting;
evaluate process CPU and resident memory as well as query latency when enabling
sharing.

[Run the small example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/sibling_topk.md)
to see the configuration and query API together.
