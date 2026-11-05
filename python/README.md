# DataFusion Distributed Python bindings

These bindings add distributed planning and Python workers while keeping DataFusion's familiar
Python API. For a runnable two-worker setup from a fresh clone, see the
[query example](examples/README.md).

Create a context with a worker resolver:

```python
from datafusion_distributed import DistributedSessionContext, WorkerResolver


class Workers(WorkerResolver):
    def get_urls(self) -> list[str]:
        return ["http://127.0.0.1:50051"]


ctx = DistributedSessionContext(Workers())
ctx.sql("SELECT 1").show()
```

`DistributedSessionContext` delegates to DataFusion's original planner, protobuf-roundtrips the
complete physical plan, and then applies distributed rewrites. Configuration stays in
`SessionConfig` and reaches decoding through `TaskContext`; execution plans never own it.

## DataFusion type compatibility

The installed `datafusion` package and `datafusion_distributed._internal` are separate native
extensions, so opaque PyO3 values cannot be exchanged directly. Use:

| Value                                                       | Import from                                                                   |
|-------------------------------------------------------------|-------------------------------------------------------------------------------|
| `SessionConfig`, `RuntimeEnvBuilder`, object stores         | `datafusion_distributed`                                                      |
| Expressions, sort expressions, scalar/aggregate/window UDFs | `datafusion`                                                                  |
| Tables                                                      | The distributed context, or a provider implementing DataFusion's FFI protocol |
| UDTFs                                                       | Unsupported until DataFusion provides a table-function bridge                 |

Object stores and credentials are runtime state and must also be configured on every worker.

## Workers

Run a worker from the command line:

```shell
python -m datafusion_distributed --host 127.0.0.1 --port 50051
```

Or embed one in the current process. Port `0` selects an available port:

```python
from datafusion_distributed import Worker

with Worker(host="127.0.0.1", port=0) as worker:
    print(worker.url)
```

Both forms log worker startup and shutdown.

## Development

From the repository root:

```shell
uv sync --project python --locked
```

For RustRover, install its **Python Community Edition** plugin, select
`python/.venv/bin/python`, and mark `python/` as a **Sources Root**.

`uv` rebuilds the extension when watched Rust inputs change. Force other rebuilds with:

```shell
uv sync --project python --reinstall-package datafusion-distributed
```

Review proposed inline snapshot updates with:

```shell
uv run --project python pytest --inline-snapshot=review
```
