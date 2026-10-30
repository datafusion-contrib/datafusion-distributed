# Tests

When submitting code, make sure it's always covered by tests. For every
important feature, it's recommended to add a dedicated integration test that
tests it end-to-end.

Please note that LLMs like to make very verbose and redundant tests even for
simple things, so before committing LLM-generated tests, review them and
simplify them as much as possible.

## Choose the right test level

- Add a unit test for a small, deterministic behavior that can be exercised
  without constructing a distributed query. Never couple unit tests to
  implementation details. Unit tests are appropriate for very scoped and clearly
  separated pieces of work, but not great for when there needs to be too many
  moving pieces in the tests.
- Add an integration test in `tests/` for a user-visible behavior. When in
  doubt, always prefer this type of tests. Never expose internals of the project
  to these tests just for the sake of finer grained testing, always test at the
  public API level.

In general, prefer writing integration tests rather than unit tests.

## writing good tests

- Always wrap tests in a `mod tests {};` block.
- Tests are executable documentation. Keep the scenario, relevant configuration,
  operation, and expected behavior understandable in place. Explicit setup is
  preferable to a shorter test that requires navigating through helper functions.
- Reuse the harness for session, codec, worker, and fixture plumbing. Keep the
  scenario's configuration together in its builder chain. Prefer existing builder
  methods and configuration hooks over adding wrappers for individual settings.
- Put fixture and observation helpers below the tests. Helpers should encapsulate
  mechanics, not hide the scenario, execution, and assertions together.
- Give distinct integration scenarios named, directly runnable tests rather than
  hiding them in loops or generated test macros. Loops remain useful for fixture
  construction and checking one coherent invariant.
- Never test implementation details, always prefer testing at the public API
  level.
- Prefer quality to quantity. Adding a lot of tests with overlapping intentions
  not only does not increase coverage, but also introduces maintenance burden in
  the project.

## Red flags in tests

- Too many tests with overlapping intentions.
- Tests dominated by low-level preparation, or short helpers that hide the scenario.
- Tests that do not follow the code pattern of adjacent tests.
- Exposing internal details of tested structs with #[cfg(test)] flags.
- PRs with a high ratio of tests VS actual code.

## Review test changes

- Confirm the test would fail before the production change.
- Check that it covers the intended planning or execution path rather than a
  nearby helper only.
- Remove redundant cases and boilerplate. A small set of clear cases is more
  valuable than generated variations that do not add coverage.

The full [test-design rules](https://github.com/datafusion-contrib/datafusion-distributed/blob/main/AGENTS.md#test-design-and-review) also cover
realistic execution paths, fixture boundaries, and avoiding redundant coverage.
Before submitting, read each changed test without opening helpers or snapshot
files and check that its setup and expected behavior are clear.

## Running Unit Tests

Running unit tests provides the shortest feedback loop during development.

```bash
# Run unit tests
cargo test
```

## Running Integration Tests

Integration tests are slower but cover a wide range of functionality.

```bash
# Run unit and integration tests
cargo test --features integration
```

## Running Benchmark Tests

These tests are slower but provide good coverage, prefer using them just at the
latest stages of the development cycle, as they are slow.

```bash
# Run TPCH integration tests
cargo test --features tpch

# Run TPC-DS integration tests
cargo test --features tpcds

# Run ClickBench integration tests
cargo test --features clickbench
```

## Resources

- [Integration tests directory](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/tests) -
  Feature-specific test examples
