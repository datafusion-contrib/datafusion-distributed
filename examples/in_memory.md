# In-memory cluster example

This example shows how queries can be run in a distributed context without making any
network IO for communicating between workers.

This is especially useful for testing, as no servers need to be spawned in localhost ports,
the setup is quite easy, and the code coverage for running in this mode is the same as
running in an actual distributed cluster.

## Preparation

This example queries a couple of test parquet we have for integration tests, and those files are stored using `git lfs`,
so pulling the first is necessary.

```shell
git install checkout
git lfs checkout
```

### Issuing a distributed SQL query

The `--show-distributed-plan` flag can be passed to render the distributed plan:

```shell
cargo run --example in_memory_cluster -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"' --show-distributed-plan
```

Not passing the flag will execute the query:

```shell
cargo run --example in_memory_cluster -- 'SELECT count(*), "MinTemp" FROM weather GROUP BY "MinTemp"'
```

### Available tables

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
