# Iceberg crate guide

## Tests

- Reuse `src/test_utils/harness.rs` and `testdata/iceberg/taxi` as much as
  possible.
- Prefer integration tests over unit tests when possible. Each isolated feature
  should have its own dedicated file in `tests/`.
- Never create long tests with a lot of setup, the pattern should be several
  tests with a very small body, and have any necessary helper at the bottom
  of the `mod tests {};` block, below the actual tests.
- Prefer snapshot testing with `insta` when possible.
