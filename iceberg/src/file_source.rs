use std::sync::Arc;

use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics, exec_datafusion_err, exec_err};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource,
};
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::object_store::ObjectStore;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_distributed::{
    DistributedLeafExec, ScaleUpLeafNodeEvent, ScaleUpLeafNodeEventResponse,
};
use futures::{FutureExt, StreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::io::FileIO;
use iceberg::scan::FileScanTask;
use iceberg::table::Table;
use iceberg_datafusion::read::convert_filters_to_predicate;
use tokio::sync::oneshot;

use crate::IcebergConfig;
use crate::column_statistics::{apply_snapshot_ndv, file_statistics, metric_columns};
use crate::common::df_err;
use crate::file_partitions::FilePartitions;
use crate::planned_files::PlannedFiles;
use crate::table_provider::ScanOptions;

/// Iceberg metadata attached to a [`PartitionedFile`]. The descriptor and its
/// memory reservation remain shared when the file is assigned to a task.
#[derive(Debug, Clone)]
pub struct IcebergFileTask {
    pub(crate) planned: Arc<PlannedFiles>,
    pub(crate) index: usize,
}

impl IcebergFileTask {
    /// Complete Iceberg read task, including deletes and residual predicates.
    pub fn task(&self) -> &FileScanTask {
        &self.planned.tasks()[self.index]
    }
}

/// Reads eager Iceberg file groups using iceberg-rust's reader.
#[derive(Debug, Clone)]
pub struct IcebergFileSource {
    pub(crate) table_schema: TableSchema,
    pub(crate) file_io: FileIO,
    pub(crate) runtime: iceberg::Runtime,
    pub(crate) config: IcebergConfig,
    metrics: ExecutionPlanMetricsSet,
    batch_size: usize,
}

impl IcebergFileSource {
    pub(crate) fn new(
        table_schema: TableSchema,
        file_io: FileIO,
        runtime: iceberg::Runtime,
        config: IcebergConfig,
    ) -> Self {
        Self {
            table_schema,
            file_io,
            runtime,
            config,
            metrics: Default::default(),
            batch_size: 8192,
        }
    }
}

impl FileSource for IcebergFileSource {
    fn create_file_opener(
        &self,
        _object_store: Arc<dyn ObjectStore>,
        _config: &FileScanConfig,
        _partition: usize,
    ) -> Result<Arc<dyn FileOpener>> {
        Ok(Arc::new(self.clone()))
    }

    fn table_schema(&self) -> &TableSchema {
        &self.table_schema
    }
    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> {
        Arc::new(Self {
            batch_size,
            ..self.clone()
        })
    }
    fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }
    fn file_type(&self) -> &str {
        "iceberg"
    }
    // An Iceberg task already defines its own range and delete semantics.
    fn supports_repartitioning(&self) -> bool {
        false
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
}

impl FileOpener for IcebergFileSource {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        if file.range.is_some() {
            return exec_err!("Iceberg file tasks cannot be split into byte ranges");
        }
        let task = file
            .extension::<IcebergFileTask>()
            .ok_or_else(|| exec_datafusion_err!("Iceberg file is missing its FileScanTask"))?
            .clone();
        let reader = ArrowReaderBuilder::new(self.file_io.clone(), self.runtime.clone())
            .with_batch_size(self.batch_size)
            .with_data_file_concurrency_limit(self.config.data_file_concurrency_limit)
            .with_row_group_filtering_enabled(self.config.row_group_filtering_enabled)
            .with_row_selection_enabled(self.config.row_selection_enabled)
            .build();
        Ok(async move {
            // Keep the reservation alive for the lifetime of the read stream.
            let descriptor = task.task().clone();
            let tasks = futures::stream::once(async move { Ok(descriptor) }).boxed();
            let stream = reader
                .read(tasks)
                .map_err(df_err)?
                .stream()
                .map(move |batch| {
                    let _reservation = &task;
                    batch.map_err(df_err)
                })
                .boxed();
            Ok(stream)
        }
        .boxed())
    }
}

/// Plan an eager scan directly into DataFusion's file scan representation.
pub(crate) async fn plan_scan(
    table: Table,
    schema: SchemaRef,
    options: ScanOptions<'_>,
    context: Arc<TaskContext>,
    config: IcebergConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let snapshot = match options.snapshot_id {
        Some(id) => table.metadata().snapshot_by_id(id),
        None => table.metadata().current_snapshot(),
    }
    .cloned();
    let iceberg_schema = match &snapshot {
        Some(snapshot) if options.snapshot_id.is_some() => {
            snapshot.schema(table.metadata()).map_err(df_err)?
        }
        _ => Arc::clone(table.metadata().current_schema()),
    };
    let fields = iceberg_schema.as_struct().fields();
    let fields: Vec<_> = match options.projection {
        Some(indices) => indices.iter().map(|&i| Arc::clone(&fields[i])).collect(),
        None => fields.to_vec(),
    };
    let output_schema = match options.projection {
        Some(indices) => Arc::new(schema.project(indices)?),
        None => schema.clone(),
    };
    let mut builder = table.scan();
    if let Some(id) = options.snapshot_id {
        builder = builder.snapshot_id(id);
    }
    if let Some(indices) = options.projection {
        builder = builder.select(indices.iter().map(|&i| schema.field(i).name()));
    }
    let predicate = convert_filters_to_predicate(options.filters);
    let may_filter_rows = predicate.is_some();
    if let Some(predicate) = predicate {
        builder = builder.with_filter(predicate);
    }
    if config.column_stats_enabled {
        builder = builder.with_column_stats(metric_columns(&iceberg_schema, &fields));
    }
    let scan = builder.build().map_err(df_err)?;
    let runtime = options
        .iceberg_runtime
        .unwrap_or_else(iceberg::Runtime::current);
    let partitions = context.session_config().target_partitions();
    let (_cancel_on_drop, canceled) = oneshot::channel::<()>();
    let task = runtime.io().spawn(async move {
        tokio::select! {
            _ = canceled => exec_err!("Iceberg file planning canceled"),
            result = PlannedFiles::collect(scan, &context, may_filter_rows) => result,
        }
    });
    let planned = Arc::new(task.await.map_err(df_err)??);
    let mut files = Vec::with_capacity(planned.tasks().len());
    for (index, task) in planned.tasks().iter().enumerate() {
        files.push(
            PartitionedFile::new(task.data_file_path(), task.length())
                .with_statistics(Arc::new(file_statistics(
                    task,
                    &output_schema,
                    &fields,
                    may_filter_rows,
                    config.column_stats_enabled,
                )))
                .with_extension(IcebergFileTask {
                    planned: Arc::clone(&planned),
                    index,
                }),
        );
    }
    let mut groups = assign_files(files, 1, partitions, config.greedy_file_assignment)?;
    let mut statistics = populate_group_statistics(&mut groups[0], &output_schema)?;
    if config.column_stats_enabled && !planned.tasks().is_empty() {
        apply_snapshot_ndv(&mut statistics, &table, snapshot.as_ref(), &fields);
    }
    let source = IcebergFileSource::new(
        TableSchema::from(output_schema),
        table.file_io().clone(),
        runtime,
        config,
    );
    // FileIO resolves Iceberg URIs itself. The required registered store is unused.
    let scan = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
        .with_file_groups(groups.into_iter().next().expect("one task"))
        .with_statistics(statistics)
        .with_limit(options.fetch)
        .build();
    Ok(DataSourceExec::from_data_source(scan))
}

pub(crate) fn assign_files(
    mut files: Vec<PartitionedFile>,
    tasks: usize,
    partitions: usize,
    greedy: bool,
) -> Result<Vec<Vec<FileGroup>>> {
    if tasks.checked_mul(partitions).is_none_or(|n| n == 0) {
        return exec_err!("Invalid Iceberg file assignment partition count");
    }
    // Restore discovery order after an earlier local grouping.
    files.sort_by_key(|file| file.extension::<IcebergFileTask>().map(|task| task.index));
    let destinations = if greedy {
        let descriptors = files
            .iter()
            .map(|file| {
                file.extension::<IcebergFileTask>()
                    .map(|task| task.task())
                    .ok_or_else(|| exec_datafusion_err!("Iceberg file is missing its FileScanTask"))
            })
            .collect::<Result<Vec<_>>>()?;
        FilePartitions::greedy(&descriptors, tasks, partitions)
    } else {
        FilePartitions::RoundRobin {
            next_file: 0,
            task_count: tasks,
            partitions_per_task: partitions,
        }
    };
    let mut groups = vec![vec![Vec::new(); partitions]; tasks];
    for (file, partition) in files.into_iter().zip(destinations) {
        groups[partition / partitions][partition % partitions].push(file);
    }
    Ok(groups
        .into_iter()
        .map(|groups| groups.into_iter().map(FileGroup::new).collect())
        .collect())
}

pub(crate) fn iceberg_scale_up(
    ev: ScaleUpLeafNodeEvent,
) -> Option<Result<ScaleUpLeafNodeEventResponse>> {
    let exec = ev.plan.downcast_ref::<DataSourceExec>()?;
    let scan = exec.data_source().downcast_ref::<FileScanConfig>()?;
    let source = scan.file_source.downcast_ref::<IcebergFileSource>()?;
    if ev.task_count == 1 {
        return Some(Ok(ScaleUpLeafNodeEventResponse::new(Arc::clone(ev.plan))));
    }
    Some((|| {
        // Scale-up is synchronous; assignment contains no I/O.
        let files = scan
            .file_groups
            .iter()
            .flat_map(|group| group.iter().cloned())
            .collect();
        let groups = assign_files(
            files,
            ev.task_count,
            scan.file_groups.len(),
            IcebergConfig::from_session_config(ev.session_config).greedy_file_assignment,
        )?;
        let mut variants = Vec::with_capacity(groups.len());
        for mut groups in groups {
            let statistics = populate_group_statistics(&mut groups, scan.file_schema())?;
            let source = IcebergFileSource {
                metrics: Default::default(),
                ..source.clone()
            };
            // Aggregate only the file entries assigned to this task.
            let mut template = scan.clone();
            template.file_source = Arc::new(source);
            let config = FileScanConfigBuilder::from(template)
                .with_file_groups(groups)
                .with_statistics(statistics)
                .build();
            variants.push(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>);
        }
        Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(
            DistributedLeafExec::try_new(Arc::clone(ev.plan), variants)?,
        )))
    })())
}

/// Keep file, local-partition and distributed-task statistics aligned after grouping.
pub(crate) fn populate_group_statistics(
    groups: &mut [FileGroup],
    schema: &Schema,
) -> Result<Statistics> {
    for group in groups.iter_mut() {
        let stats = aggregate_file_statistics(group.iter(), schema)?;
        *group = group.clone().with_statistics(Arc::new(stats));
    }
    aggregate_file_statistics(groups.iter().flat_map(|group| group.iter()), schema)
}

fn aggregate_file_statistics<'a>(
    mut files: impl Iterator<Item = &'a PartitionedFile>,
    schema: &Schema,
) -> Result<Statistics> {
    let Some(first) = files.next() else {
        let mut stats = Statistics::new_unknown(schema);
        stats.num_rows = Precision::Exact(0);
        stats.total_byte_size = Precision::Exact(0);
        for column in &mut stats.column_statistics {
            column.null_count = Precision::Exact(0);
            column.distinct_count = Precision::Exact(0);
            column.byte_size = Precision::Inexact(0);
        }
        return Ok(stats);
    };
    let unknown = Statistics::new_unknown(schema);
    let mut stats = Statistics::try_merge_iter(
        std::iter::once(first)
            .chain(files)
            .map(|file| file.statistics.as_deref().unwrap_or(&unknown)),
        schema,
    )?;
    if let Some(bytes) = stats
        .column_statistics
        .iter()
        .try_fold(0usize, |total, col| {
            total.checked_add(*col.byte_size.get_value()?)
        })
    {
        stats.total_byte_size = Precision::Inexact(bytes);
    }
    Ok(stats)
}
