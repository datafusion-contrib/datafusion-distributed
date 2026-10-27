# DataFusion Distributed Iceberg

Read-only Apache Iceberg tables for DataFusion Distributed.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_distributed_iceberg::{IcebergExt, IcebergIntegrationOptions};

async fn example() -> datafusion::error::Result<()> {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_iceberg_integration(IcebergIntegrationOptions::default())
        .build();
    let ctx = SessionContext::new_with_state(state);

    ctx.sql(
        "CREATE EXTERNAL TABLE taxi STORED AS ICEBERG \
     LOCATION 's3://warehouse/taxi/metadata/v1.metadata.json'",
    )
        .await?
        .collect()
        .await?;
    Ok(())
}
```

The default storage factory resolves `file://`, S3 (`s3://`, `s3a://`,
`s3n://`), and GCS (`gs://`, `gcs://`) URIs. Use
`IcebergIntegrationOptions` to supply custom storage or an Iceberg runtime.

By default, Iceberg discovers files progressively during execution. Enable
`iceberg.eager_file_planning` to discover and retain file tasks during physical
planning instead. Retained descriptors use the query memory pool; there is no
file-count limit.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionConfig;
use datafusion_distributed_iceberg::{IcebergConfig, IcebergExt, IcebergIntegrationOptions};

let config = SessionConfig::new()
    .with_option_extension(IcebergConfig::default())
    .set_bool("iceberg.eager_file_planning", true);
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_config(config)
    .with_iceberg_integration(IcebergIntegrationOptions::default())
    .build();
```

```text
Eager: discover → source owns files → assign task-local groups → route → worker reads
Lazy:  scan definition → remote feed reference → discover and dispatch → worker reads
```

`EagerIcebergDataSource` owns eager partition groups. Like standard file scans,
the scale-up handler creates a source per task before routing. The router can
borrow those files from `RouteTaskEvent::task_specialized_plan`:

```rust
use datafusion_distributed_iceberg::EagerIcebergDataSource;

fn assigned_paths(source: &EagerIcebergDataSource) -> Vec<&str> {
    source.planned_files().map(|file| file.data_file_path()).collect()
}
```

Use `planned_partition_tasks(partition)` to inspect a particular local partition.
Eager assignment considers largest files first and places each on the task with
the fewest assigned bytes, cycling that task's local partitions. File identity
breaks equal-size ties; task file count and task index break equal-load ties.
This is the eager policy, with no separate configuration flag. File bytes are
scheduling estimates; assignment neither splits files nor chooses workers.
Task-local sources share descriptor storage and its memory reservation on the coordinator.
The codec sends only each task's assigned groups in its plan; workers restore
those groups without discovering or reassigning files.

Lazy scans remain `LazyIcebergDataSource` instances backed by a work-unit feed.
Both sources use the same reader configuration and file-task-to-batch execution.
Iceberg's `FileIO` handles storage in both cases. Eager planning waits for metadata
discovery and increases worker-plan payloads; lazy scans overlap discovery with
reading and deliver descriptors through the coordinator's feed.

Eager scans derive row counts and bytes from selected files and cap task estimates
by the number of selected files. Predicates or deletes make row counts inexact;
empty selections have exactly zero rows. When column statistics are enabled,
eager scans aggregate selected-file metrics. Lazy scans retain snapshot statistics
and task-size estimates. Task-local eager variants and decoded plans report unknown
statistics rather than applying whole-snapshot aggregates to a subset.

## Selected-file statistics

With `iceberg.column_stats_enabled=true`, eager scans reconstruct column bytes,
min/max bounds, and null counts from the **selected files**. Missing metrics remain
unknown. Bounds are inexact; null counts also become inexact when predicates or
deletes may remove rows. Nested column bytes sum leaf sizes without inventing
parent bounds or null counts.

Snapshot distinct-count estimates are retained as an inexact fallback, capped by
selected-file row counts when known. They are not recomputed for the selected
files and can overestimate the filtered result. Sum statistics remain unknown.

Another manifest pass is needed because the pinned Iceberg API does not retain
column metrics in file tasks. Scan task counts continue to use whole-file bytes.

```bash
cargo test -p datafusion-distributed-iceberg --features integration
```

Local TPC-H Iceberg benchmarks use the separate
[`datafusion-distributed-iceberg-benchmarks`](benchmarks/README.md) package. This keeps benchmark
preparation and execution dependencies out of this read-only integration crate.

Eager source construction and regrouping are independent of distributed scheduling.
`from_file_groups` accepts native tasks grouped by local partition without discovery;
`with_file_groups` derives a source using positions in `planned_files()` (retained-file
order), sharing descriptors and their reservation. Groups may select a subset but
cannot repeat a position. The scale-up handler chooses the distributed assignment;
the codec reconstructs it through the same public API on workers.
