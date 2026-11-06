# Sharing Top-K bounds between tasks

`SiblingTopKConfig` enables tasks executing the same fetched sort to share their
dynamic bounds. It is disabled by default.

```rust
use datafusion_distributed::{DistributedExt, SiblingTopKConfig};

let mut sharing = SiblingTopKConfig::default();
sharing.enabled = true;
let ctx = ctx.with_distributed_option_extension(sharing);
```

Use `with_distributed_option_extension` so the option reaches workers. The
library restores this extension automatically on each worker. Its configuration
key is `distributed_sibling_topk.enabled`.

Both the coordinator and every participating worker must run a version that
supports this feature before enabling it. The unchanged message format does not
make opt-in sharing compatible with older workers.

Sharing also requires `distributed.remote_dynamic_filters`,
`datafusion.optimizer.enable_dynamic_filter_pushdown`, and
`datafusion.optimizer.enable_topk_dynamic_filter_pushdown`. These settings are
enabled by default. A sort must already have a dynamic filter consumed within
its task. If an additional bound cannot be pushed through a custom worker plan
without changing its operator topology, that task keeps its existing local
filtering and does not apply the sibling bound.

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

[Run the small example](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/examples/sibling_topk.md)
to see the configuration and query API together.
