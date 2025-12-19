# Tests

When submitting code, make sure it's always covered by tests. For every important feature,
it's recommended to add a dedicated integration test that tests it end-to-end.

Please note that LLMs like to make very verbose and redundant tests even for simple things,
so before committing LLM-generated tests, review them and simplify them as much as possible.

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

# Run TPCH integration tests
cargo test --features tpch

# Run TPC-DS integration tests
cargo test --features tpc-ds
```

## Resources

- [Integration tests directory](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/tests) -
  Feature-specific test examples
