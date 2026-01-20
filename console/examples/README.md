# Console Example

This example runs a shared worker instance that exposes the observability
service and then starts the console UI that connects to it.

## Run the worker

In one terminal from the repository root:

```bash
cargo run -p console --example console_worker
```

The worker listens on `127.0.0.1:8080` by default. Use `--port` to change it.
If you change the port, update the address in `console/src/main.rs`.

## Run the console

In a second terminal from the repository root:

```bash
cargo run -p console
```

This following message should appear:

```bash
ping ok (value: 1)
```

Press any key to exit the console.
