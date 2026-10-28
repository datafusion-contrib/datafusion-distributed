# Iceberg crate guide

## Tests

- Reuse `src/test_utils/harness.rs` and `testdata/iceberg/taxi` as much as
  possible.
- Prefer integration tests over unit tests when possible. Each isolated feature
  should have its own dedicated file in `tests/`.
- Tests are executable documentation. A reviewer should understand the scenario,
  configuration, operation, and expected behavior by reading the test itself.
  Optimize for that understanding, not for the fewest lines. Follow the
  [repository test-design rules](../AGENTS.md#test-design-and-review).
- Keep relevant harness configuration and compact inline `insta` snapshots in
  each test. Helpers below the tests should handle fixture mechanics, not hide
  the scenario or its assertions.
