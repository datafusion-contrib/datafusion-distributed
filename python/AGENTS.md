# Python bindings agent guide

## Tests

- Use `inline-snapshot` for query integration assertions, following
  `tests/test_weather_table.py`.
- Snapshot the complete physical plan from `display_indent()` and the complete query result from
  `repr(df)` or the full materialized result when exercising an `ExecutionPlan` directly.
- Pass plan text through `snapshot_utils.anonymize_snapshot` before snapshotting it so
  checkout-specific paths and developer-machine information are never recorded.
- Never replace those snapshots with assertions over derived summaries such as substring
  membership, booleans, batch counts, or row counts. Such assertions can pass while meaningful
  plan or result changes go unnoticed.

## Rust-to-Python exports

- Declare every symbol exported from Rust to Python inline in the `_internal` module function in
  `src/lib.rs`. Do not hide Python module registration behind per-module `register` helpers.
- Keep `datafusion_distributed/_internal.pyi` synchronized with every exported class and function,
  including signatures and return types, whenever the Rust exports change.
