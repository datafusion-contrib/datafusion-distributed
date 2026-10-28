# Iceberg crate guide

## Tests

- Reuse `IcebergTestHarness` from `tests/common/mod.rs` as much as possible.
  Each test chooses its table on the builder, e.g. `with_taxi_dataset()` for
  the generated taxi table or `with_described_file(..)` for a taxi-schema table
  listing only described files; without one, the harness has no table. Use
  `with_metadata` to serve customized metadata for the chosen table.
- Build any other table with `datafusion-distributed-iceberg-test-tools`
  (`test-tools/`) rather than by hand or by committing data files. A
  `FixtureCatalog` is an in-memory catalog over a temp dir.
  `fixtures.table(ident, schema).partition_by(..).create()` creates a table, and
  each `table.append().file(batch)...commit()` adds one snapshot in which every
  batch becomes exactly one data file. Snapshot ids count up from 1, so
  snapshots can show them. `create_taxi_table` generates a deterministic,
  synthetic stand-in for NYC taxi trips: 7 days × 25,000 rows,
  identity-partitioned on `pickup_date`, one file per day.
- Prefer integration tests over unit tests when possible. Each isolated feature
  should have its own dedicated file in `tests/`.
- Tests are executable documentation. A reviewer should understand the scenario,
  configuration, operation, and expected behavior by reading the test itself.
  Optimize for that understanding, not for the fewest lines. Follow the
  [repository test-design rules](../AGENTS.md#test-design-and-review).
- Keep relevant harness configuration and compact inline `insta` snapshots in
  each test. Helpers below the tests should handle fixture mechanics, not hide
  the scenario or its assertions.
