# Materialized CTEs

Run a two-worker example that evaluates `random()` once per source row and
replays the same values for both sides of a self-join:

```sh
cargo run --features integration --example materialized_cte
```

All three rows should report `same_value = true`. The example uses the localhost
test cluster helper, which starts workers on automatically selected ports.

See [materialized CTEs](../docs/source/advanced/09-materialized-ctes.md) for the
current dependency prerequisites and execution limits.
