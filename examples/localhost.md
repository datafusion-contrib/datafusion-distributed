# Localhost workers example

This example executes a SQL query in a distributed context.

For this example to work, it's necessary to spawn some localhost workers with the `localhost_worker.rs` example:

## Preparation

This example queries a couple of test parquet we have for integration tests, and those files are stored using `git lfs`,
so pulling the first is necessary.

```shell
git lfs install
git lfs checkout
```

### Spawning the workers

In two different terminals spawn two ArrowFlightEndpoints

```shell
cargo run --example localhost_worker -- 8080
```

```shell
cargo run --example localhost_worker -- 8081
```

The positional numeric argument is the port in which each Arrow Flight endpoint will listen to.

### Issuing a distributed SQL query

Now, DataFusion queries can be issued using these workers as part of the cluster.

```shell
cargo run --example localhost_run -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"' --cluster-ports 8080,8081
```

The head stage (the one that outputs data to the user) will be executed locally in the same process as that `cargo run`
command, but further stages will be delegated to the workers running on ports 8080 and 8081.

Additionally, the `--explain` flag can be passed to render the distributed plan:

```shell
cargo run --example localhost_run -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"' --cluster-ports 8080,8081 --show-distributed-plan
```

### Available tables

Two tables are available in this example:

- `flights_1m`: Flight data with 1m rows

```
FL_DATE [INT32]
DEP_DELAY [INT32]
ARR_DELAY [INT32]
AIR_TIME [INT32]
DISTANCE [INT32]
DEP_TIME [FLOAT]
ARR_TIME [FLOAT]
```

- `weather`: Small dataset of weather data

```
MinTemp [DOUBLE]
MaxTemp [DOUBLE]
Rainfall [DOUBLE]
Evaporation [DOUBLE]
Sunshine [BYTE_ARRAY]
WindGustDir [BYTE_ARRAY]
WindGustSpeed [BYTE_ARRAY]
WindDir9am [BYTE_ARRAY]
WindDir3pm [BYTE_ARRAY]
WindSpeed9am [BYTE_ARRAY]
WindSpeed3pm [INT64]
Humidity9am [INT64]
Humidity3pm [INT64]
Pressure9am [DOUBLE]
Pressure3pm [DOUBLE]
Cloud9am [INT64]
Cloud3pm [INT64]
Temp9am [DOUBLE]
Temp3pm [DOUBLE]
RainToday [BYTE_ARRAY]
RISK_MM [DOUBLE]
RainTomorrow [BYTE_ARRAY]
```
