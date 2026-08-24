use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;
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
use datafusion::prelude::Expr;
use datafusion_distributed::WorkUnitFeed;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::{FileIO, StorageConfig};

use crate::common::{convert_filters_to_predicate, df_err, iceberg_err};
use crate::work_unit_wire::FileScanTaskDecoder;
use crate::{IcebergConfig, IcebergWorkUnitFeed};

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
    schema: SchemaRef,
    partitioning: Partitioning,
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    iceberg_file_io: FileIO,
    iceberg_runtime: iceberg::Runtime,
    feed: WorkUnitFeed<IcebergWorkUnitFeed>,
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
        opts: IcebergDataSourceOptions,
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
        }
    }

    pub(crate) fn from_remote(
        schema: SchemaRef,
        partitioning: Partitioning,
        fetch: Option<usize>,
        iceberg_file_io: FileIO,
        iceberg_runtime: iceberg::Runtime,
        feed: WorkUnitFeed<IcebergWorkUnitFeed>,
    ) -> Self {
        Self {
            schema,
            partitioning,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            iceberg_file_io,
            iceberg_runtime,
            feed,
        }
    }

    pub(crate) fn schema_ref(&self) -> &SchemaRef {
        &self.schema
    }

    pub(crate) fn storage_config(&self) -> &StorageConfig {
        self.iceberg_file_io.config()
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
        // TODO: Implement planning time statistics for this DataSource.
        //  At this point, we have information about the iceberg::table::Table which we are about
        //  to read, so maybe there's something we can get from there.
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
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
