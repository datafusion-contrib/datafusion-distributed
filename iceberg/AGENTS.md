# Iceberg crate guide

## Tests

- Use `IcebergTestHarness` from `datafusion-distributed-iceberg-test-tools`
  for integration-test session, codec, and worker setup. It doesn't depend on this crate, so each test installs
  the integration with
  `with_session_extension(|state| state.with_iceberg_integration(..))`, sets
  Iceberg options by name with `with_config_option("iceberg.*", ..)`, builds
  its table with the test tools, e.g. `create_taxi_table()`, and hands it over
  with `with_table(..)`; without one, the harness has no table. Use
  `with_metadata` to serve customized metadata for that table.
- Never add `with_x_and_y`, `with_x_with_y`, or equivalent constructor or
  registration variants. Add settings to `IcebergIntegrationOptions` or use
  existing composable builder methods. Register custom codecs through
  `with_session_extension` and `IcebergIntegrationOptions::file_io_codec`, so
  the harness installs them on both the coordinator and workers.
- Use `configure_session` for coordinator-only session configuration and
  `with_workers` for distributed execution. Keep these choices in the test's
  builder chain; do not construct replacement sessions or workers by hand.
- Build any other table with `datafusion-distributed-iceberg-test-tools`
  (`test-tools/`) rather than by hand or by committing data files. A
  `FixtureCatalog` is an in-memory catalog over a temp dir.
  `fixtures.table(ident, schema).partition_by(..).create()` creates a table, and
  each `table.append().file(batch)...commit()` adds one snapshot in which every
  batch becomes exactly one data file. Snapshot ids count up from 1, so
  snapshots can show them. `create_taxi_table` generates a deterministic,
  synthetic stand-in for NYC taxi trips: 7 days × 25,000 rows,
  identity-partitioned on `pickup_date`, one file per day.
- Add version 2 position-delete fixtures with
  `table.position_deletes().file([(&data_files[0], 0), (&data_files[1], 3)]).commit()`.
  Each group writes one delete file; referenced data files must share a partition.
  Use the returned descriptors when tests need to inspect delete files.
- Prefer integration tests over unit tests when possible. Each isolated feature
  should have its own dedicated file in `tests/`.
- Tests are executable documentation. A reviewer should understand the scenario,
  configuration, operation, and expected behavior by reading the test itself.
  Optimize for that understanding, not for the fewest lines. Follow the
  [repository test-design rules](../AGENTS.md#test-design-and-review).
- Keep relevant harness configuration and compact inline `insta` snapshots in
  each test. Helpers below the tests should handle fixture mechanics, not hide
  the scenario or its assertions.
