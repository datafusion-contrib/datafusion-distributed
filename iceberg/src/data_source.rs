use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ColumnStatistics, Statistics, exec_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSource;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_expr::{Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayFormatType, SortOrderPushdownResult};
use datafusion_distributed::WorkUnitFeed;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::spec::SnapshotRef;
use iceberg::table::Table;
use iceberg_datafusion::read::convert_filters_to_predicate;
use tokio::sync::oneshot;

use crate::table_provider::ScanOptions;

pub use crate::column_statistics::compute_column_stats;
use crate::common::{df_err, iceberg_err};
use crate::{IcebergConfig, IcebergWorkUnitFeed};

/// Snapshot summary keys defined by the Iceberg table spec:
/// https://iceberg.apache.org/spec/#optional-snapshot-summary-fields
///
/// iceberg-rust defines them privately:
/// https://github.com/apache/iceberg-rust/blob/4168a0b2950dc5f85588e5cb3ab6796e5228b309/crates/iceberg/src/spec/snapshot_summary.rs#L46-L47
const TOTAL_RECORDS: &str = "total-records";
const TOTAL_FILE_SIZE: &str = "total-files-size";

/// Feed-backed Iceberg scan used by lazy execution.
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

impl IcebergDataSource {
    /// Creates a new [`IcebergDataSource`] object.
    pub(crate) fn new(
        table: iceberg::table::Table,
        schema: SchemaRef,
        partitioning: Partitioning,
        opts: ScanOptions<'_>,
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

    /// Whole-file I/O used for scan parallelism, independent of projected column sizes.
    pub(crate) fn file_scan_bytes(&self) -> Option<usize> {
        self.feed.inner()?;
        match &self.table_snapshot {
            Some(snapshot) => snapshot
                .summary()
                .additional_properties
                .get(TOTAL_FILE_SIZE)?
                .parse()
                .ok(),
            None => Some(0),
        }
    }

    /// Aggregate snapshot column metrics for lazy scans.
    pub(crate) async fn with_column_statistics(
        mut self,
        table: Table,
        projection: Option<&[usize]>,
    ) -> Result<Self> {
        let schema = match &self.table_snapshot {
            Some(snap) => snap.schema(table.metadata()).map_err(df_err)?,
            // empty table
            None => table.metadata().current_schema().clone(),
        };
        let fields = schema.as_struct().fields();
        let fields: Vec<_> = match projection {
            Some(projection) => projection
                .iter()
                .map(|&idx| Arc::clone(&fields[idx]))
                .collect(),
            None => fields.to_vec(),
        };
        let snapshot = self.table_snapshot.clone();
        let (_cancel_on_drop, canceled) = oneshot::channel::<()>();
        let task = self.iceberg_runtime.io().spawn(async move {
            tokio::select! {
                _ = canceled => exec_err!("Iceberg column statistics canceled"),
                stats = async {
                    let field_ids = fields.iter().map(|field| field.id).collect();
                    compute_column_stats(table, field_ids, snapshot).await
                } => stats,
            }
        });
        self.column_stats = Some(task.await.map_err(df_err)??);
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
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

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

        let feed = self
            .feed
            .feed(partition, context)?
            .map(|work_unit_or_err| match work_unit_or_err {
                Ok(work_unit) => work_unit.into_task().map_err(iceberg_err),
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
            // Only replace the file-byte fallback if every projected column has
            // a size. These remain compressed-byte estimates, not Arrow memory.
            if let Some(bytes) = col_stats.iter().try_fold(0_usize, |total, column| {
                total.checked_add(*column.byte_size.get_value()?)
            }) {
                stats.total_byte_size = Precision::Inexact(bytes);
            }
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
