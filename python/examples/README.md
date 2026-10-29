# Python examples

Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and a Rust toolchain, then
bootstrap a fresh clone from the repository root:

```shell
git clone https://github.com/datafusion-contrib/datafusion-distributed.git
cd datafusion-distributed
uv sync --project python --locked
```

The first sync builds the native extension. Later `uv sync` and `uv run` commands rebuild it when
its Rust sources change; `--reinstall-package` is not normally needed.

Start two workers in different terminals:

```shell
uv run --project python python -m datafusion_distributed --port 50051
uv run --project python python -m datafusion_distributed --port 50052
```

Then pass SQL as one quoted argument from another terminal:

```shell
uv run --project python python python/examples/query.py \
  --port 50051 --port 50052 \
  'SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday"'
```

`DistributedSessionContext` otherwise uses the normal DataFusion SQL and DataFrame APIs. The only
application-specific requirement is a worker resolver whose `get_urls()` method returns the workers
currently available for a query.
