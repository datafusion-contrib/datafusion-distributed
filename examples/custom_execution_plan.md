# Custom Execution Plan Example

Demonstrates how to create a distributed custom execution plan with a `numbers(start, end)` table function.

## Components

**NumbersTableFunction** – Table function callable in SQL: `SELECT * FROM numbers(1, 100)`

**NumbersExec** – Execution plan with `ranges_per_task: Vec<Range<i64>>` storing one range per task.
Uses `DistributedTaskContext` to determine which range to generate.

**NumbersExecCodec** – Protobuf-based serialization implementing `PhysicalExtensionCodec`.
Must be registered on both coordinator and workers.

**NumbersTaskEstimator** – Controls distributed parallelism:

- `task_estimation()` - Returns how many tasks needed based on range size and config
- `scale_up_leaf_node()` - Splits single range of numbers into N per-task ranges

**NumbersConfig** – Custom config extension for controlling distributed parallelism (`numbers_per_task: usize`)

## Usage

This example imports the `InMemoryWorkerResolver` and the `InMemoryChannelResolver` used during integration testing
of this project, so it needs the `--features integration` flag on.

This example demonstrates how the bigger the range of numbers is queried, the more tasks are used in executing
the query, for example:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 10) ORDER BY number" \
  --show-distributed-plan
```

This will print a non-distributed plan, as the range of numbers we are querying (`numbers(0, 10)`) is small.

The config parameter `numbers.numbers_per_task` is the one that controls how many distributed tasks are used in the
query, and it's default value is `10`, so querying 10 numbers will not distribute the plan.

However, if we try to query 11 numbers:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 11) ORDER BY number" \
  --show-distributed-plan
```

The distribution rule kicks in, and the plan gets distributed.

Note that the parallelism in the plan has an upper threshold, so for example, if we query 100 numbers:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 100) ORDER BY number" \
  --show-distributed-plan
```

We do not get 100/10 = 10 distributed tasks, we just get 4. This is because the example is configured by default to
simulate a 4-worker cluster. If we increase the worker count, we get a highly distributed plan out with 10 tasks:

```bash
cargo run \
  --features integration \
  --example custom_execution_plan \
  "SELECT DISTINCT number FROM numbers(0, 100) ORDER BY number" \
  --workers 10 \
  --show-distributed-plan
```

