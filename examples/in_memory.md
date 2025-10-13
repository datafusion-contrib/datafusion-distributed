# In-memory cluster example

This examples shows how queries can be run in a distributed context without making any
network IO for communicating between workers.

This is specially useful for testing, as no servers need to be spawned in localhost ports,
the setup is quite easy, and the code coverage for running in this mode is the same as
running in an actual distributed cluster.

## Preparation

This example queries a couple of test parquet we have for integration tests, and those files are stored using `git lfs`,
so pulling the first is necessary.

```shell
git intall checkout
git lfs checkout
```

### Issuing a distributed SQL query

```shell
cargo run --example in_memory_cluster -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"'
```

Additionally, the `--explain` flag can be passed to render the distributed plan:

```shell
cargo run --example in_memory_cluster -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"' --explain 
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
