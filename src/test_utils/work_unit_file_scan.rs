//! WorkUnit-based [`FileScanConfig`] alternative for benchmarking purposes.
//!
//! Streams the per-partition `PartitionedFile`s through a [`WorkUnitFeed`]
//! instead of embedding them directly in the serialized plan. Used to measure
//! the latency impact of routing file scan inputs through the work unit
//! pipeline as compared to the regular [`FileScanConfig`] path.

use crate::{DistributedConfig, TaskCountAnnotation, TaskEstimation, TaskEstimator};
use crate::{WorkUnitFeed, WorkUnitFeedProto, WorkUnitFeedProvider};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, internal_datafusion_err};
use datafusion::common::{Statistics, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::source::DataSource;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, Partitioning};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
};
use datafusion_proto::protobuf as df_proto;
use datafusion_proto::protobuf::proto_error;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Per-partition work unit emitted by [`FileScanWorkUnitProvider`]: the (encoded)
/// `PartitionedFile` that the receiving worker partition should scan. `None`
/// means "no file for this slot" — used as padding when the total file count
/// isn't divisible by `task_count` so the global feed indexing layout
/// (`task_index * partitions_per_task + p`) stays consistent across tasks.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileScanWorkUnit {
    #[prost(message, tag = "1")]
    pub file: Option<df_proto::PartitionedFile>,
}

/// Local provider that holds the per-partition `PartitionedFile` assignment
/// for a [`WorkUnitFileScanConfig`]. It is only invoked on the coordinator
/// (the worker side gets a `RemoteFeedProvider` automatically when the plan is
/// decoded).
#[derive(Debug, Clone)]
pub struct FileScanWorkUnitProvider {
    /// One entry per global feed partition (`partitions_per_task * task_count`).
    /// `None` slots are sent as empty work units so the worker emits an empty
    /// stream for that partition.
    file_groups: Vec<FileGroup>,
}

impl FileScanWorkUnitProvider {
    pub fn new(file_groups: Vec<FileGroup>) -> Self {
        Self { file_groups }
    }
}

impl WorkUnitFeedProvider for FileScanWorkUnitProvider {
    type WorkUnit = FileScanWorkUnit;

    fn feed(
        &self,
        partition: usize,
        _ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>> {
        let Some(file_group) = self.file_groups.get(partition) else {
            return Ok(futures::stream::empty().boxed());
        };
        let stream = futures::stream::iter(file_group.files().to_vec()).map(|file| {
            let file_proto: df_proto::PartitionedFile = (&file)
                .try_into()
                .map_err(|e| internal_datafusion_err!("{e}"))?;
            Ok(FileScanWorkUnit {
                file: Some(file_proto),
            })
        });
        Ok(stream.boxed())
    }
}

/// [`DataSource`] that defers obtaining its [`PartitionedFile`](datafusion_datasource::PartitionedFile)
/// until execution time, pulling it off a [`WorkUnitFeed`] before delegating
/// the actual file scan to the wrapped [`FileScanConfig`]. The wrapped config
/// carries no file groups while it is being serialized — the per-partition
/// file travels through the feed instead.
#[derive(Debug, Clone)]
pub struct WorkUnitFileScanConfig {
    pub feed: WorkUnitFeed<FileScanWorkUnitProvider>,
    /// Underlying [`FileScanConfig`] used as a template. `file_groups` is left
    /// empty here; the per-partition assignment arrives via `feed`.
    pub fsc: FileScanConfig,
    /// Number of output partitions exposed to the rest of the plan. On the
    /// coordinator this is per-task partitions before scaling; on a worker this
    /// is the per-task partition count.
    pub partitions: usize,
}

impl WorkUnitFileScanConfig {
    pub fn new(mut fsc: FileScanConfig) -> Self {
        let file_groups = std::mem::take(&mut fsc.file_groups);

        Self {
            partitions: file_groups.len(),
            feed: WorkUnitFeed::new(FileScanWorkUnitProvider::new(file_groups)),
            fsc,
        }
    }
}

impl DataSource for WorkUnitFileScanConfig {
    fn open(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner = self.fsc.clone();
        let schema = inner.projected_schema()?;

        let stream = self
            .feed
            .feed(partition, Arc::clone(&context))?
            .map(move |work_unit| {
                let file = work_unit?.file.expect("missing file");

                let single_file_group = df_proto::FileGroup { files: vec![file] };
                let single_file_group = FileGroup::try_from(&single_file_group)?;

                let new_config = FileScanConfigBuilder::from(inner.clone())
                    .with_file_groups(vec![single_file_group])
                    .build();
                new_config.open(0, Arc::clone(&context))
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WorkUnitFileScan: ",)?;
                self.fsc.fmt_as(t, f)
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "WorkUnitFileScan")?;
                Ok(())
            }
        }
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _repartition_file_min_size: usize,
        _output_ordering: Option<LexOrdering>,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        // Repartitioning is handled by the WorkUnitFileScanTaskEstimator, not
        // by DataFusion's repartition pass.
        Ok(None)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions)
    }

    fn eq_properties(&self) -> EquivalenceProperties {
        self.fsc.eq_properties()
    }

    fn scheduling_type(&self) -> SchedulingType {
        SchedulingType::Cooperative
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        // Statistics for a specific partition are not known at planning time
        // because we don't know which file will land on it; fall back to the
        // aggregate template statistics.
        self.fsc.partition_statistics(None)
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
        let new_template = FileScanConfigBuilder::from(self.fsc.clone())
            .with_limit(limit)
            .build();
        Some(Arc::new(WorkUnitFileScanConfig {
            feed: self.feed.clone(),
            fsc: new_template,
            partitions: self.partitions,
        }))
    }

    fn fetch(&self) -> Option<usize> {
        self.fsc.limit
    }

    fn metrics(&self) -> ExecutionPlanMetricsSet {
        self.feed.metrics()
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &datafusion::physical_expr::projection::ProjectionExprs,
    ) -> Result<Option<Arc<dyn DataSource>>> {
        Ok(None)
    }
}

/// Encodes/decodes a [`DataSourceExec`] wrapping a [`WorkUnitFileScanConfig`].
/// The template [`FileScanConfig`] (with empty `file_groups`) is serialized via
/// DataFusion's default codec — we rely on it being round-trippable for the
/// underlying `FileSource` (Parquet, CSV, etc.).
#[derive(Debug)]
pub struct WorkUnitFileScanCodec;

#[derive(Clone, PartialEq, ::prost::Message)]
struct WorkUnitFileScanProto {
    /// Encoded [`df_proto::PhysicalPlanNode`] representing the template
    /// `DataSourceExec(FileScanConfig)` (with empty `file_groups`). We keep it
    /// as raw bytes so we can use DataFusion's default protobuf converter to
    /// roundtrip it without re-implementing every `FileSource` codec here.
    #[prost(bytes, tag = "1")]
    inner: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    feed: Option<WorkUnitFeedProto>,
    #[prost(uint64, tag = "3")]
    partitions: u64,
}

impl PhysicalExtensionCodec for WorkUnitFileScanCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !inputs.is_empty() {
            return internal_err!(
                "WorkUnitFileScanConfig should have no children, got {}",
                inputs.len()
            );
        }
        let proto = WorkUnitFileScanProto::decode(buf)
            .map_err(|e| proto_error(format!("Failed to decode WorkUnitFileScanProto: {e}")))?;

        let plan_node = df_proto::PhysicalPlanNode::decode(&proto.inner[..])
            .map_err(|e| proto_error(format!("Failed to decode template plan: {e}")))?;
        let template_plan =
            plan_node.try_into_physical_plan(ctx, &DefaultPhysicalExtensionCodec {})?;
        let Some(dse) = template_plan.as_any().downcast_ref::<DataSourceExec>() else {
            return Err(proto_error(
                "Expected the WorkUnitFileScan template plan to be a DataSourceExec",
            ));
        };
        let Some(inner) = dse.data_source().as_any().downcast_ref::<FileScanConfig>() else {
            return Err(proto_error(
                "Expected the WorkUnitFileScan template DataSource to be a FileScanConfig",
            ));
        };
        let Some(feed_proto) = proto.feed else {
            return Err(proto_error("WorkUnitFileScanProto missing feed"));
        };
        Ok(DataSourceExec::from_data_source(WorkUnitFileScanConfig {
            feed: WorkUnitFeed::from_proto(feed_proto)?,
            fsc: inner.clone(),
            partitions: proto.partitions as usize,
        }))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        let Some(dse) = node.as_any().downcast_ref::<DataSourceExec>() else {
            return internal_err!(
                "Expected DataSourceExec wrapping a WorkUnitFileScanConfig, got {}",
                node.name()
            );
        };
        let Some(wfs) = dse
            .data_source()
            .as_any()
            .downcast_ref::<WorkUnitFileScanConfig>()
        else {
            return internal_err!("Expected the inner DataSource to be a WorkUnitFileScanConfig");
        };

        // Encode the template DataSourceExec(FileScanConfig) as a regular
        // PhysicalPlanNode using DataFusion's default codec.
        let plan_node = df_proto::PhysicalPlanNode::try_from_physical_plan(
            DataSourceExec::from_data_source(wfs.fsc.clone()),
            &DefaultPhysicalExtensionCodec {},
        )?;
        let mut inner_bytes = Vec::new();
        plan_node.encode(&mut inner_bytes).map_err(|e| {
            proto_error(format!(
                "Failed to encode WorkUnitFileScan template plan: {e}"
            ))
        })?;

        let proto = WorkUnitFileScanProto {
            inner: inner_bytes,
            feed: Some(wfs.feed.to_proto()),
            partitions: wfs.partitions as u64,
        };
        proto
            .encode(buf)
            .map_err(|e| proto_error(format!("Failed to encode WorkUnitFileScanProto: {e}")))
    }
}

/// [`PhysicalOptimizerRule`] that rewrites every leaf `DataSourceExec`
/// containing a [`FileScanConfig`] to one that wraps a
/// [`WorkUnitFileScanConfig`]. Every individual file from every original
/// `FileGroup` is moved into a separate slot of the [`WorkUnitFeed`] (one
/// `PartitionedFile` per output partition), and the template config is left
/// with empty file groups.
#[derive(Debug, Default)]
pub struct WorkUnitFileScanRule;

impl PhysicalOptimizerRule for WorkUnitFileScanRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            let Some(dse) = node.as_any().downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            let Some(fsc) = dse.data_source().as_any().downcast_ref::<FileScanConfig>() else {
                return Ok(Transformed::no(node));
            };

            let new_ds = WorkUnitFileScanConfig::new(fsc.clone());
            Ok(Transformed::yes(DataSourceExec::from_data_source(new_ds)))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "WorkUnitFileScanRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// [`TaskEstimator`] for [`WorkUnitFileScanConfig`] leaves that delegates to
/// the built-in [`FileScanConfigTaskEstimator`]: we synthesize a regular
/// `DataSourceExec(FileScanConfig)` carrying the same files currently stored
/// in the work-unit feed, hand it to the underlying estimator, and then
/// re-wrap the result back into our work-unit-flavored data source.
///
/// `FileScanConfigTaskEstimator::scale_up_leaf_node` returns a
/// `PartitionIsolatorExec(DataSourceExec(FileScanConfig))`. We unwrap that
/// here: the `PartitionIsolatorExec` itself must not appear in the final plan
/// because the per-task feed routing already handles per-task isolation. We
/// only keep the inner `FileScanConfig`'s file groups, flatten them back into
/// one `PartitionedFile` per feed slot, and feed them into a freshly built
/// `WorkUnitFileScanConfig`.
#[derive(Debug, Default)]
pub struct WorkUnitFileScanTaskEstimator;

impl TaskEstimator for WorkUnitFileScanTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        let dse = plan.as_any().downcast_ref::<DataSourceExec>()?;
        let wfs = dse
            .data_source()
            .as_any()
            .downcast_ref::<WorkUnitFileScanConfig>()?;

        // Same as FileScanConfigTaskEstimator.task_estimation.
        let d_cfg = cfg.extensions.get::<DistributedConfig>()?;

        let mut partitioned_files = 0;
        for file_group in &wfs.feed.inner()?.file_groups {
            partitioned_files += file_group.len();
        }

        let task_count = partitioned_files.div_ceil(d_cfg.files_per_task);

        Some(TaskEstimation {
            task_count: TaskCountAnnotation::Desired(task_count),
        })
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        _cfg: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let dse = plan.as_any().downcast_ref::<DataSourceExec>()?;
        let wfs = dse
            .data_source()
            .as_any()
            .downcast_ref::<WorkUnitFileScanConfig>()?;

        let wuf_provider = wfs.feed.inner()?;

        // Same as FileScanConfigTaskEstimator.scale_up_leaf_node
        let mut new_file_groups = vec![];
        for file_group in wuf_provider.file_groups.clone() {
            new_file_groups.extend(file_group.split_files(task_count));
        }

        let new_provider = FileScanWorkUnitProvider::new(new_file_groups);
        Some(DataSourceExec::from_data_source(WorkUnitFileScanConfig {
            feed: WorkUnitFeed::new(new_provider),
            fsc: wfs.fsc.clone(),
            partitions: wfs.partitions,
        }) as Arc<dyn ExecutionPlan>)
    }
}
