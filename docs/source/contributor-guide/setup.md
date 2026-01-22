# Setup

## Prerequisites

- Rust toolchain (see `rust-toolchain.toml` for the required version)
- Git LFS for test data

## Clone and Setup

Clone the repository and set up test data:

```bash
git clone https://github.com/datafusion-contrib/datafusion-distributed
cd datafusion-distributed
git lfs install
git lfs checkout
```

## Running Examples

```bash
# In-memory cluster example
cargo run --example in_memory_cluster -- 'SELECT * FROM weather LIMIT 10'

# Localhost workers (requires starting workers first in separate terminals)
cargo run --example localhost_worker -- 8080 --cluster-ports 8080,8081
cargo run --example localhost_run -- 'SELECT * FROM weather LIMIT 10' --cluster-ports 8080,8081
```

## Resources

- [Examples directory](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/examples) - Full working examples
- [Integration tests](https://github.com/datafusion-contrib/datafusion-distributed/tree/main/tests) - Feature-specific examples
