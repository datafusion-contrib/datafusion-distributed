# Sharing Top-K bounds between tasks

Run from the repository root after checking out the Git LFS test data:

```bash
git lfs checkout
cargo run --features integration --example sibling_topk
```

The example uses the existing in-memory worker harness to run an `ORDER BY ...
LIMIT 10` query with two simulated workers. It registers the opt-in configuration
through `with_distributed_option_extension`; workers restore it automatically.
The printed plan and rows illustrate configuration and query execution, not a
measurement of pruning or performance.

Each task keeps its own Top-K bound and receives other tasks' bounds through a
separate dynamic filter. The coordinator combines available compatible task
predicates with `AND`. That snapshot may include the recipient's own earlier
bound, which is redundant with its current local bound. The final merge still
selects the query result.

See [the feature guide](../docs/source/advanced/09-sibling-topk.md) for prerequisites,
unsupported pushdown paths, and deployment considerations.
