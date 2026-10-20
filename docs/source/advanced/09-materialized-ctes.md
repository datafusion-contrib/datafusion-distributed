# Materialized CTEs

`WITH ... AS MATERIALIZED` computes a CTE body once per query execution and lets
multiple references replay its result, including references in different worker
stages. Enable the PostgreSQL SQL dialect to use this syntax:

```sql
SET datafusion.sql_parser.dialect = 'PostgreSQL';

WITH values_once AS MATERIALIZED (
    SELECT id, random() AS value FROM source
)
SELECT a.id, a.value = b.value AS same_value
FROM values_once a JOIN values_once b ON a.id = b.id;
```

Each matching pair observes the same random value. Queries without the explicit
`MATERIALIZED` directive retain DataFusion's ordinary CTE behavior.

The body can execute across workers. One producer task collects and materializes
its complete result using DataFusion's memory pool and spill files. Each reference
has its own reader identity and partition range. Tasks executing a reference divide
its partitions between them, so additional tasks do not duplicate rows. The result
is retained for the query's lifetime and is not durable or shared between queries.

Full buffering allows a build-side reader of a self-join to complete before its
probe-side reader starts. It also permits late readers without reexecuting the body.
The single producer's storage and network bandwidth currently limit scalability.
Queries containing materialized CTEs use static task placement even when adaptive
execution is enabled. Recursive queries containing materialized CTEs are currently
rejected because they need separate materializations for each iteration.

## Future runtime filtering

Filters remain local to each reference. No consumer filter is currently pushed into
the shared materialization. Materialization identity and reader identity are separate
so a future protocol can register per-reader runtime filters without changing result
addressing. Such an implementation must:

- Combine the requirements of all readers; one reader's filter cannot remove rows
  needed by another reader. An unfiltered reader requires the complete result.
- Distinguish an unknown filter from a completed filter, including an empty one.
- Avoid waiting for a filter whose computation depends on this materialization.
- Specify when pruning becomes safe. A late filter may reduce replay work, but
  cannot retroactively remove data another reader still needs.

This draft requires DataFusion's native materialized CTE execution plans. DFD's
current upstream dependency version does not yet provide them; the draft must
wait for upstream DFD to adopt that API. This change leaves dependencies unchanged.

Run the two-worker example in `examples/materialized_cte.rs` with
`cargo run --features integration --example materialized_cte`.
