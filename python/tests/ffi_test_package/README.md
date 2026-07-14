# Third-party FFI test package

This optional PyO3 package is compiled independently from both `datafusion-python` and `datafusion-distributed`. It supplies foreign scalar, aggregate, and window UDFs, a foreign table provider, a custom physical executor, and logical/physical extension codecs.

The package has its own Cargo workspace so normal datafusion-distributed builds do not compile it. `python/tests/test_foreign_ffi.py` skips when the package is not installed.

After FFI changes, rebuild all three Python extensions from the same source checkout:

```bash
# Use the datafusion-python development environment, or another environment
# containing maturin, pytest, and pyarrow.
export VIRTUAL_ENV=/path/to/venv

(cd ../datafusion-python && maturin develop --uv)
maturin develop --uv --manifest-path python/Cargo.toml
maturin develop --uv --manifest-path python/tests/ffi_test_package/Cargo.toml

cd python
python -m pytest tests/test_foreign_ffi.py -v
```

Without the final `maturin develop` command, the tests are collected and reported as skipped.

Rust-only validation:

```bash
cargo check --manifest-path python/tests/ffi_test_package/Cargo.toml
cargo fmt --manifest-path python/tests/ffi_test_package/Cargo.toml -- --check
```
