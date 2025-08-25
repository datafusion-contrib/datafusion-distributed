# Localhost workers example

This example executes a SQL query in a distributed context.

For this example to work, it's necessary to spawn some localhost workers with the `localhost_worker.rs` example:

## Preparation

This example queries a couple of test parquet we have for integration tests, and those files are stored using `git lfs`,
so pulling the first is necessary.

```shell
git lfs checkout
```

### Spawning the workers

In two different terminals spawn two ArrowFlightEndpoints

```shell
cargo run --example localhost_worker -- 8080 --cluster-ports 8080,8081
```

```shell
cargo run --example localhost_worker -- 8081 --cluster-ports 8080,8081
```

- The positional numeric argument is the port in which each Arrow Flight endpoint will listen
- The `--cluster-ports` parameter tells the Arrow Flight endpoint all the available localhost workers in the cluster

### Issuing a distributed SQL query

Now, DataFusion queries can be issued using these workers as part of the cluster.

```shell
cargo run --example localhost_run -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"' --cluster-ports 8080,8081
```

The head stage will be executed locally in the same process as that `cargo run` command, but further stages will be
delegated to the workers running on ports 8080 and 8081.

Additionally, the `--explain` flag can be passed to render the distributed plan:

```shell
cargo run --example localhost_run -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"' --cluster-ports 8080,8081 --explain
```

### Available tables

Two tables are available in this example:

- `flights_1m`: Flight data with 1m rows
- `weather`: Small dataset of weather data
