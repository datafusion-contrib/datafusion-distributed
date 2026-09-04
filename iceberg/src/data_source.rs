use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSource;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_expr::{Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayFormatType, SortOrderPushdownResult};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_distributed::WorkUnitFeed;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::puffin::APACHE_DATASKETCHES_THETA_V1;
use iceberg::spec::{
    DataFile, Datum, Manifest, ManifestContentType, ManifestList, PrimitiveLiteral, PrimitiveType,
    SnapshotRef,
};
use iceberg::table::Table;

use crate::common::{convert_filters_to_predicate, df_err, iceberg_err};
use crate::work_unit_wire::FileScanTaskDecoder;
use crate::{IcebergConfig, IcebergWorkUnitFeed};

/// Snapshot summary keys defined by the Iceberg table spec:
/// https://iceberg.apache.org/spec/#optional-snapshot-summary-fields
///
/// iceberg-rust defines them privately:
/// https://github.com/apache/iceberg-rust/blob/4168a0b2950dc5f85588e5cb3ab6796e5228b309/crates/iceberg/src/spec/snapshot_summary.rs#L46-L47
const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILE_SIZE: &str = "total-files-size";

/// Consumes a stream of [iceberg::scan::FileScanTask]s per partition and reads the underlying
/// files into an Arrow stream.
///
/// [iceberg::scan::FileScanTask] are discovered progressively during execution by the
/// [IcebergWorkUnitFeed], and this [DataSource] executes those tasks as they come, also in
/// a streaming fashion. This works seamlessly in both single-node and distributed execution:
///
/// ## Single Node
///
/// [iceberg::scan::FileScanTask] are streamed in-memory, with as many parallel streams as
/// partitions this [IcebergDataSource] exposes:
///
/// ```text
/// ┌────────────────────────────────────────────┐
/// │             IcebergDataSource              │
/// │                                            │
/// │┌──────────────────────────────────────────┐│
/// ││           IcebergWorkUnitFeed            ││
/// ││┌────────────┐┌────────────┐┌────────────┐││
/// │││   Feed 0   ││   Feed 1   ││   Feed 2   │││
/// ││└──────┬─────┘└──────┬─────┘└──────┬─────┘││
/// │└───────┼─────────────┼─────────────┼──────┘│
/// │  .─────▼─────. .─────▼─────. .─────▼─────. │
/// │ (FileScanTask (FileScanTask (FileScanTask )│
/// │  .───────────. `─────┬─────' .───────────. │
/// │ (FileScanTask )      │      (FileScanTask )│
/// │  `─────┬─────'       │       .───────────. │
/// │        │             │      (FileScanTask )│
/// │        │             │       `─────┬─────' │
/// │        │             │             │       │
/// │ ┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐ │
/// │ │Partition 0 ││Partition 1 ││Partition 2 │ │
/// │ │ArrowReader ││ArrowReader ││ArrowReader │ │
/// │ └──────┬─────┘└──────┬─────┘└──────┬─────┘ │
/// │        │             │             │       │
/// │  .─────▼─────.       │       .─────▼─────. │
/// │ ( RecordBatch ).─────▼─────.( RecordBatch )│
/// │  `─────┬─────'( RecordBatch ).───────────. │
/// │        │       `─────┬─────'( RecordBatch )│
/// │        │             │       `───────────' │
/// └────────┼─────────────┼─────────────┼───────┘
///          ▼             ▼             ▼
/// ```
///
/// ## Distributed
///
/// [iceberg::scan::FileScanTask] are streamed over the network, with as many parallel streams as
/// partitions * distributed tasks:
///
/// ```text
///  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///                                      Coordinating Context                                   │
///  │
///   ┌────────────────────────────────────────────────────────────────────────────────────────┐│
///  ││                                  IcebergWorkUnitFeed                                   │
///   │┌─────────────┐┌─────────────┐┌────────────┐┌────────────┐┌─────────────┐┌─────────────┐││
///  │││   Feed 0    ││   Feed 1    ││   Feed 2   ││   Feed 3   ││   Feed 4    ││   Feed 5    ││
///   │└──────┬──────┘└─────┬───────┘└────┬───────┘└───────┬────┘└───────┬─────┘└──────┬──────┘││
///  └└───────┼─────────────┼─────────────┼────────────────┼─────────────┼─────────────┼───────┴
///     .─────▼─────. .─────▼─────. .─────▼─────.    .─────▼─────. .─────▼─────. .─────▼─────.
///    (FileScanTask (FileScanTask (FileScanTask )  (FileScanTask (FileScanTask (FileScanTask )
///     .───────────. `─────┬─────' .───────────.    `─────┬─────' .───────────. `─────┬─────'
///    (FileScanTask )      │      (FileScanTask )         │      (FileScanTask )      │
///     `─────┬─────'       │       .───────────.          │       `───────────'       │
///           │             │      (FileScanTask )         │             │             │
///  Worker 0 │             │       `─────┬─────'          │             │             │ Worker 1
/// ┌ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ┐┌ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ─ ─ ┼ ─ ─ ─ ─ ┐
///   ┌───────┼─────────────┼─────────────┼───────┐┌───────┼─────────────┼─────────────┼───────┐
/// │ │       │     IcebergD│taSource     │       ││       │     IcebergD│taSource     │       │ │
///   │       │             │             │       ││       │             │             │       │
/// │ │┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐ ││┌──────▼─────┐┌──────▼─────┐┌──────▼─────┐ │ │
///   ││Partition 0 ││Partition 1 ││Partition 2 │ │││Partition 0 ││Partition 1 ││Partition 2 │ │
/// │ ││ArrowReader ││ArrowReader ││ArrowReader │ │││ArrowReader ││ArrowReader ││ArrowReader │ │ │
///   │└──────┬─────┘└──────┬─────┘└──────┬─────┘ ││└──────┬─────┘└──────┬─────┘└──────┬─────┘ │
/// │ │       │             │             │       ││       │             │             │       │ │
///   │ .─────▼─────.       │       .─────▼─────. ││       │             ▼             ▼       │
/// │ │( RecordBatch ).─────▼─────.( RecordBatch )││ .─────▼─────. .───────────. .───────────. │ │
///   │ `─────┬─────'( RecordBatch ).───────────. ││( RecordBatch ( RecordBatch ) RecordBatch )│
/// │ │       │       `─────┬─────'( RecordBatch )││ `─────┬─────' `───────────' `─────┬─────' │ │
///   │       │             │       `───────────' ││       │      ( RecordBatch )      │       │
/// │ │       │             │             │       ││       │       `─────┬─────'       │       │ │
///   └───────┼─────────────┼─────────────┼───────┘└───────┼─────────────┼─────────────┼───────┘
/// │         ▼             ▼             ▼       ││       ▼             ▼             ▼         │
///  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─  ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
/// ```
///
/// This distributed mechanism is transparent to this [DataSource].
#[derive(Debug, Clone)]
pub struct IcebergDataSource {
    pub(crate) schema: SchemaRef,
    pub(crate) partitioning: Partitioning,
    pub(crate) fetch: Option<usize>,
    pub(crate) metrics: ExecutionPlanMetricsSet,
    pub(crate) column_stats: Option<Vec<ColumnStatistics>>,
    pub(crate) table_snapshot: Option<SnapshotRef>,
    pub(crate) iceberg_file_io: FileIO,
    pub(crate) iceberg_runtime: iceberg::Runtime,
    pub(crate) feed: WorkUnitFeed<IcebergWorkUnitFeed>,
}

/// Optional fields for building an [IcebergDataSource].
#[derive(Default, Clone)]
pub(crate) struct IcebergDataSourceOptions<'a> {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) fetch: Option<usize>,
    pub(crate) filters: &'a [Expr],
    pub(crate) iceberg_runtime: Option<iceberg::Runtime>,
}

impl IcebergDataSource {
    /// Creates a new [`IcebergDataSource`] object.
    pub(crate) fn new(
        table: iceberg::table::Table,
        schema: SchemaRef,
        partitioning: Partitioning,
        opts: IcebergDataSourceOptions<'_>,
    ) -> Self {
        let output_schema = match opts.projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let projection = opts.projection.map(|v| {
            v.iter()
                .map(|p| schema.field(*p).name().clone())
                .collect::<Vec<String>>()
        });
        // Necessary for time-travel queries
        let table_snapshot = match opts.snapshot_id {
            Some(snapshot_id) => table.metadata().snapshot_by_id(snapshot_id),
            None => table.metadata().current_snapshot(),
        }
        .cloned();
        let predicates = convert_filters_to_predicate(opts.filters);

        Self {
            schema: output_schema,
            iceberg_file_io: table.file_io().clone(),
            partitioning: partitioning.clone(),
            fetch: opts.fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            iceberg_runtime: opts
                .iceberg_runtime
                .unwrap_or_else(iceberg::Runtime::current),
            feed: WorkUnitFeed::new(IcebergWorkUnitFeed {
                iceberg_table: table,
                snapshot_id: opts.snapshot_id,
                projection,
                predicates,
                partitioning,
                sync_manager: Default::default(),
            }),
            table_snapshot,
            column_stats: None,
        }
    }

    /// Creating an instance with per column statistics calculation, including:
    /// - null_count, min_value, max_value, byte_size
    pub(crate) async fn with_column_statistics(
        mut self,
        table: Table,
        projection: Option<&Vec<usize>>,
    ) -> Result<Self> {
        let schema = match &self.table_snapshot {
            Some(snap) => snap.schema(table.metadata()).map_err(df_err)?,
            // empty table
            None => table.metadata().current_schema().clone(),
        };
        let fields = schema.as_struct().fields().to_vec();
        let field_ids: Vec<i32> = match projection {
            Some(projection) => projection.iter().map(|&idx| fields[idx].id).collect(),
            None => fields.iter().map(|f| f.id).collect(),
        };
        self.column_stats =
            Some(compute_column_stats(table, field_ids, self.table_snapshot.clone()).await?);
        Ok(self)
    }
}

impl IcebergDataSource {
    /// Returns the [WorkUnitFeed] implementation that feeds this
    /// DataSource with [iceberg::scan::FileScanTask] messages.
    pub fn feed(&self) -> &WorkUnitFeed<IcebergWorkUnitFeed> {
        &self.feed
    }
}

impl DataSource for IcebergDataSource {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let config = IcebergConfig::from_task_context(&context);

        let reader =
            ArrowReaderBuilder::new(self.iceberg_file_io.clone(), self.iceberg_runtime.clone())
                .with_batch_size(context.session_config().batch_size())
                .with_data_file_concurrency_limit(config.data_file_concurrency_limit)
                .with_row_group_filtering_enabled(config.row_group_filtering_enabled)
                .with_row_selection_enabled(config.row_selection_enabled)
                .build();

        let mut decoder = FileScanTaskDecoder::default();
        let feed = self
            .feed
            .feed(partition, context)?
            .map(move |msg_or_err| match msg_or_err {
                Ok(msg) => decoder.decode(msg).map_err(iceberg_err),
                Err(err) => Err(iceberg_err(err)),
            })
            .boxed();

        let stream = reader
            .read(feed)
            .map(|result| result.stream())
            .map_err(df_err)?
            .map_err(df_err);

        let stream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )) as SendableRecordBatchStream;

        let metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(LimitStream::new(stream, 0, self.fetch, metrics)))
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "format=iceberg")?;
        let Some(feed) = self.feed.inner() else {
            return Ok(());
        };
        if let Some(projection) = &feed.projection {
            write!(f, ", projection=[{}]", projection.join(", "))?;
        }
        if let Some(predicate) = &feed.predicates {
            write!(f, ", predicate={predicate}")?;
        }
        if let Some(fetch) = self.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        Ok(())
    }

    fn output_partitioning(&self) -> Partitioning {
        self.partitioning.clone()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.schema))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        if self.feed.inner().is_none() {
            return Ok(Arc::new(Statistics::new_unknown(&self.schema)));
        }
        let mut stats = stats_from_snapshot(self.table_snapshot.clone(), &self.schema)?;

        if let Some(col_stats) = &self.column_stats {
            stats.column_statistics = col_stats.clone();
        }

        Ok(Arc::new(stats))
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let mut self_clone = self.clone();
        self_clone.fetch = fetch;
        Some(Arc::new(self_clone))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.metrics.clone()
    }

    fn try_pushdown_filters(
        &self,
        filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
        // TODO: Allow this DataSource to be pushed down filters. Some filters might be more
        //  straight forward to accept, like simple predicates, but some others might require
        //  a bit more work, like dynamic filters.
        Ok(FilterPushdownPropagation::with_parent_pushdown_result(
            vec![PushedDown::No; filters.len()],
        ))
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn DataSource>>> {
        // TODO: Allow this DataSource to be pushed down sort expressions.
        Ok(SortOrderPushdownResult::Unsupported)
    }
}

/// Getting stats out of snapshot's additional properties (no I/O overhead)
fn stats_from_snapshot(snapshot: Option<SnapshotRef>, schema: &SchemaRef) -> Result<Statistics> {
    let Some(snap) = snapshot else {
        // A table with no current snapshot has never had a commit. It was created, but zero data files were added
        return Ok(Statistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
        });
    };
    let props = &snap.summary().additional_properties;

    let num_rows = props
        .get(TOTAL_RECORDS)
        .and_then(|v| v.parse().ok())
        .map(Precision::Exact)
        .unwrap_or(Precision::Absent);
    let total_byte_size = props
        .get(TOTAL_FILE_SIZE)
        .and_then(|v| v.parse().ok())
        .map(Precision::Exact)
        .unwrap_or(Precision::Absent);

    Ok(Statistics {
        num_rows,
        total_byte_size,
        column_statistics: vec![ColumnStatistics::new_unknown(); schema.fields().len()],
    })
}

/// Getting number of distinct values for columns in a table
///
/// Note: https://iceberg.apache.org/puffin-spec/#apache-datasketches-theta-v1-blob-type
fn ndv_from_metadata(table: &Table, snapshot_id: i64) -> HashMap<i32, usize> {
    let mut ndvs = HashMap::new();
    let Some(stats) = table.metadata().statistics_for_snapshot(snapshot_id) else {
        return ndvs;
    };
    for blob in &stats.blob_metadata {
        // Getting sketch estimates of ndv in every column
        if blob.r#type != APACHE_DATASKETCHES_THETA_V1 {
            continue;
        }

        let &[field_id] = &blob.fields[..] else {
            continue;
        };
        if let Some(ndv) = blob.properties.get("ndv").and_then(|v| v.parse().ok()) {
            ndvs.insert(field_id, ndv);
        }
    }
    ndvs
}

/// Reading table statistics from data-files concurrently
pub async fn compute_column_stats(
    table: Table,
    fields_ids: Vec<i32>,
    snapshot: Option<SnapshotRef>,
) -> Result<Vec<ColumnStatistics>> {
    match snapshot {
        Some(actual_snapshot) => {
            let metadata = table.metadata();
            let ml_bytes = table
                .file_io()
                .new_input(actual_snapshot.manifest_list())
                .map_err(df_err)?
                .read()
                .await
                .map_err(df_err)?;
            let manifest_list = Arc::new(
                ManifestList::parse_with_version(&ml_bytes, metadata.format_version())
                    .map_err(df_err)?,
            );

            // If a table has delete files (i.e MOR) - we should account for that fact later and change the counts to `inexact`
            let has_deletes = manifest_list
                .entries()
                .iter()
                .any(|f| f.content == ManifestContentType::Deletes);
            // Collecting all of the needed paths before spawning
            let manifest_paths: Vec<_> = manifest_list
                .entries()
                .iter()
                .filter(|mf| mf.content == ManifestContentType::Data)
                .map(|mf| mf.manifest_path.clone())
                .collect();
            let mut join_set = tokio::task::JoinSet::new();

            for path in manifest_paths {
                let table = table.clone();
                let fields_ids = fields_ids.clone();

                join_set.spawn(async move {
                    let manifest =
                        Manifest::parse_avro(&table.file_io().new_input(&path)?.read().await?)?;

                    let mut col_stats: Vec<Option<ColumnStatistics>> = vec![None; fields_ids.len()];

                    for entry in manifest.entries().iter().filter(|e| e.is_alive()) {
                        let df = entry.data_file();

                        for (i, &id) in fields_ids.iter().enumerate() {
                            let next = data_file_col_stats(df, id);
                            col_stats[i] = Some(merge_col_stats(col_stats[i].take(), next));
                        }
                    }

                    Ok::<_, iceberg::Error>(col_stats)
                });
            }

            let mut merged: Vec<Option<ColumnStatistics>> = vec![None; fields_ids.len()];
            while let Some(result) = join_set.join_next().await {
                let manifest_stats = result
                    .map_err(|e| DataFusionError::ExecutionJoin(Box::new(e)))?
                    .map_err(df_err)?;
                for (acc, next) in merged.iter_mut().zip(manifest_stats) {
                    if let Some(next) = next {
                        *acc = Some(merge_col_stats(acc.take(), next));
                    }
                }
            }

            let mut merged: Vec<ColumnStatistics> = merged
                .into_iter()
                .map(|cs| cs.unwrap_or_else(ColumnStatistics::new_unknown))
                .collect();

            let ndvs = ndv_from_metadata(&table, actual_snapshot.snapshot_id());
            for (cs, id) in merged.iter_mut().zip(fields_ids.iter()) {
                if let Some(&ndv) = ndvs.get(id) {
                    cs.distinct_count = Precision::Inexact(ndv);
                }
            }

            // TODO: probably we can do something about the delete files?
            // However it wouldn't be free of cost in terms of performance
            if has_deletes {
                for cs in &mut merged {
                    cs.null_count = cs.null_count.to_inexact();
                }
            }

            Ok(merged)
        }
        None => Ok(vec![ColumnStatistics::new_unknown(); fields_ids.len()]),
    }
}

/// Merging table's column statistics incrementally
fn merge_col_stats(acc: Option<ColumnStatistics>, next: ColumnStatistics) -> ColumnStatistics {
    match acc {
        None => next,
        Some(acc) => ColumnStatistics {
            null_count: acc.null_count.add(&next.null_count),
            min_value: acc.min_value.min(&next.min_value),
            max_value: acc.max_value.max(&next.max_value),
            byte_size: acc.byte_size.add(&next.byte_size),
            sum_value: Precision::Absent,
            distinct_count: Precision::Absent,
        },
    }
}

fn data_file_col_stats(df: &DataFile, id: i32) -> ColumnStatistics {
    ColumnStatistics {
        null_count: df
            .null_value_counts()
            .get(&id)
            .map(|n| Precision::Exact(*n as usize))
            .unwrap_or(Precision::Absent),
        min_value: df
            .lower_bounds()
            .get(&id)
            .and_then(datum_to_scalar)
            .map(Precision::Inexact)
            .unwrap_or(Precision::Absent),
        max_value: df
            .upper_bounds()
            .get(&id)
            .and_then(datum_to_scalar)
            .map(Precision::Inexact)
            .unwrap_or(Precision::Absent),
        byte_size: df
            .column_sizes()
            .get(&id)
            .map(|n| Precision::Inexact(*n as usize))
            .unwrap_or(Precision::Absent),
        sum_value: Precision::Absent,
        distinct_count: Precision::Absent,
    }
}

/// Conversion function of iceberg's Datum
fn datum_to_scalar(d: &Datum) -> Option<ScalarValue> {
    match (d.data_type(), d.literal()) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => {
            Some(ScalarValue::Boolean(Some(*v)))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Some(ScalarValue::Int32(Some(*v))),
        (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Some(ScalarValue::Int64(Some(*v))),
        (PrimitiveType::Float, PrimitiveLiteral::Float(v)) => {
            Some(ScalarValue::Float32(Some(v.into_inner())))
        }
        (PrimitiveType::Double, PrimitiveLiteral::Double(v)) => {
            Some(ScalarValue::Float64(Some(v.into_inner())))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(s)) => {
            Some(ScalarValue::Utf8(Some(s.clone())))
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Some(ScalarValue::Date32(Some(*v))),
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
            Some(ScalarValue::TimestampMicrosecond(Some(*v), None))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => Some(
            ScalarValue::TimestampMicrosecond(Some(*v), Some("UTC".into())),
        ),
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(v)) => Some(
            ScalarValue::Decimal128(Some(*v), *precision as u8, *scale as i8),
        ),
        _ => None,
    }
}
