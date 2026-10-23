use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, Statistics, exec_datafusion_err, exec_err};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource,
};
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener};
use datafusion::datasource::source::{DataSource, DataSourceExec};
use datafusion::datasource::table_schema::TableSchema;
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

use crate::common::df_err;
use crate::file_partitions::FilePartitions;
use crate::planned_files::PlannedFiles;
use crate::{IcebergConfig, IcebergDataSource};

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

impl IcebergDataSource {
    /// Finish planning: eager scans become standard file scans; lazy scans retain their feed.
    pub(crate) fn into_scan(self, config: IcebergConfig) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(planned) = self.planned.clone() else {
            return Ok(DataSourceExec::from_data_source(self));
        };
        let statistics = self.partition_statistics(None)?.as_ref().clone();
        let mut files = Vec::with_capacity(planned.tasks().len());
        for (index, task) in planned.tasks().iter().enumerate() {
            files.push(
                PartitionedFile::new(task.data_file_path(), task.length()).with_extension(
                    IcebergFileTask {
                        planned: Arc::clone(&planned),
                        index,
                    },
                ),
            );
        }
        let groups = assign_files(
            files,
            1,
            self.partitioning.partition_count(),
            config.greedy_file_assignment,
        )?;
        let source = IcebergFileSource::new(
            TableSchema::from(self.schema),
            self.iceberg_file_io,
            self.iceberg_runtime,
            config,
        );
        // FileIO resolves Iceberg URIs itself. FileScanConfig still requires a
        // registered store; the built-in local store is unused by this opener.
        let scan = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
            .with_file_groups(groups.into_iter().next().expect("one task"))
            .with_statistics(statistics)
            .with_limit(self.fetch)
            .build();
        Ok(DataSourceExec::from_data_source(scan))
    }
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
        for groups in groups {
            let source = IcebergFileSource {
                metrics: Default::default(),
                ..source.clone()
            };
            // Whole-scan column aggregates do not describe this task's subset.
            let mut template = scan.clone();
            template.file_source = Arc::new(source);
            let config = FileScanConfigBuilder::from(template)
                .with_file_groups(groups)
                .with_statistics(Statistics::new_unknown(scan.file_schema()))
                .build();
            variants.push(DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>);
        }
        Ok(ScaleUpLeafNodeEventResponse::new(Arc::new(
            DistributedLeafExec::try_new(Arc::clone(ev.plan), variants)?,
        )))
    })())
}
