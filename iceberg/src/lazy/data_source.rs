use crate::common::scan_config::{ScanConfig, stats_from_snapshot};
use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::config::ConfigOptions;
use datafusion::datasource::source::DataSource;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_expr::{Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{DisplayFormatType, SortOrderPushdownResult};
use datafusion_distributed::WorkUnitFeed;
use futures::StreamExt;
use iceberg::spec::SnapshotRef;

use crate::IcebergWorkUnitFeed;
use crate::common::iceberg_err;

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
/// partitions this [LazyIcebergDataSource] exposes:
///
/// ```text
/// ┌────────────────────────────────────────────┐
/// │           LazyIcebergDataSource            │
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
pub struct LazyIcebergDataSource {
    pub(crate) scan: ScanConfig,
    pub(crate) column_stats: Option<Vec<ColumnStatistics>>,
    pub(crate) table_snapshot: Option<SnapshotRef>,
    pub(crate) feed: WorkUnitFeed<IcebergWorkUnitFeed>,
}

impl LazyIcebergDataSource {
    /// Returns the [WorkUnitFeed] implementation that feeds this
    /// DataSource with [iceberg::scan::FileScanTask] messages.
    pub fn feed(&self) -> &WorkUnitFeed<IcebergWorkUnitFeed> {
        &self.feed
    }
}

impl DataSource for LazyIcebergDataSource {
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
        let feed = self
            .feed
            .feed(partition, Arc::clone(&context))?
            .map(|work_unit_or_err| match work_unit_or_err {
                Ok(work_unit) => work_unit.into_task().map_err(iceberg_err),
                Err(err) => Err(iceberg_err(err)),
            })
            .boxed();
        self.scan.open(feed, partition, context)
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "format=iceberg")?;
        let Some(feed) = self.feed.inner() else {
            return Ok(());
        };
        write!(f, ", table={}", feed.iceberg_table.identifier())?;
        if let Some(projection) = &feed.projection {
            write!(f, ", projection=[{}]", projection.join(", "))?;
        }
        if let Some(predicate) = &feed.predicates {
            write!(f, ", predicate={predicate}")?;
        }
        if let Some(fetch) = self.scan.fetch {
            write!(f, ", fetch={fetch}")?;
        }
        Ok(())
    }

    fn output_partitioning(&self) -> Partitioning {
        self.scan.partitioning.clone()
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new(Arc::clone(&self.scan.schema))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        if self.feed.inner().is_none() {
            return Ok(Arc::new(Statistics::new_unknown(&self.scan.schema)));
        }
        let mut stats = stats_from_snapshot(self.table_snapshot.clone(), &self.scan.schema)?;

        if let Some(col_stats) = &self.column_stats {
            stats.column_statistics = col_stats.clone();
        }

        Ok(Arc::new(stats))
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let mut self_clone = self.clone();
        self_clone.scan.fetch = fetch;
        Some(Arc::new(self_clone))
    }

    fn fetch(&self) -> Option<usize> {
        self.scan.fetch
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.scan.metrics.clone()
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
