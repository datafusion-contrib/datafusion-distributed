use crate::config_extension_ext::set_distributed_option_extension;
use crate::execution_plans::AnyMessage;
use crate::{DistributedConfig, PartitionIsolatorExec};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result;
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// Annotation attached to a single [ExecutionPlan] that determines how many distributed tasks
/// it should run on.
#[derive(Debug, Clone)]
pub enum TaskCountAnnotation {
    /// The desired number of distributed tasks for this node. The final task count for the
    /// annotated node might not be exactly this number, it is more like a hint, so depending
    /// on the desired task count of adjacent nodes, the final task count might change.
    Desired(usize),
    /// Sets a maximum number of distributed tasks for this node. Typically used with the inner
    /// value of 1, stating that this node cannot be executed in a distributed fashion.
    Maximum(usize),
}

impl From<TaskCountAnnotation> for usize {
    fn from(annotation: TaskCountAnnotation) -> Self {
        annotation.as_usize()
    }
}

impl TaskCountAnnotation {
    pub fn as_usize(&self) -> usize {
        match self {
            Self::Desired(desired) => *desired,
            Self::Maximum(maximum) => *maximum,
        }
    }

    pub(crate) fn limit(self, limit: usize) -> Self {
        match self {
            Self::Desired(desired) => Self::Desired(desired.min(limit)),
            Self::Maximum(maximum) => Self::Maximum(maximum.min(limit)),
        }
    }
}

/// Result of running a [TaskEstimator] on a leaf node. It tells the distributed planner hints
/// about how many tasks should be used in [Stage]s that contain leaf nodes.
pub struct TaskEstimation<T = ()> {
    /// The number of tasks that should be used in the [Stage] containing the leaf node.
    ///
    /// Even if implementations get to decide this number, there are situations where it can
    /// get overridden:
    /// - If a [Stage] contains multiple leaf nodes, the one that declares the biggest
    ///   task_count wins.
    /// - If there are less available workers than this number, the number of available workers
    ///   is chosen.
    pub task_count: TaskCountAnnotation,

    /// Arbitrary data this task estimator is expected to handle. Whenever a [TaskEstimation]
    /// provides an estimation, it has the option to place any arbitrary information here, that
    /// will be available afterward at the moment of calling [TaskEstimator::distribute_plan].
    pub user_data: T,
}

impl TaskEstimation<()> {
    /// Tells the distributed planner that the evaluated stage can have **at maximum** the provided
    /// number of tasks, setting a hard upper limit.
    ///
    /// Returning `TaskEstimation::maximum(1)` tells the distributed planner that the evaluated
    /// stage cannot be distributed.
    ///
    /// Even if a `TaskEstimation::maximum(N)` is provided, any other node in the same stage
    /// providing a value of `TaskEstimation::maximum(M)` where `M` < `N` will have preference.
    pub fn maximum(value: usize) -> Self {
        TaskEstimation {
            task_count: TaskCountAnnotation::Maximum(value),
            user_data: (),
        }
    }

    /// Tells the distributed planner that the evaluated can **optimally** have the provided
    /// number of tasks, setting a soft task count hint that can be overridden by others.
    ///
    /// The provided `TaskEstimation::desired(N)` can be overridden by:
    /// - Other nodes providing a `TaskEstimation::desired(M)` where `M` > `N`.
    /// - Any other node providing a `TaskEstimation::maximum(M)` where `M` can be anything.
    pub fn desired(value: usize) -> Self {
        TaskEstimation {
            task_count: TaskCountAnnotation::Desired(value),
            user_data: (),
        }
    }
}

impl<Any> TaskEstimation<Any> {
    pub fn with_data<T>(self, data: T) -> TaskEstimation<T> {
        TaskEstimation {
            task_count: self.task_count,
            user_data: data,
        }
    }
}

/// Details about how the plan should be executed in a distributed context.
pub struct DistributedPlan {
    /// The modified plan, adapted to be executed in a distributed context.
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    /// User-defined data that will be streamed to the different workers and injected into the
    /// provided `plan` at runtime.
    ///
    /// If not empty, the vector is expected to have a length of P*T, where P is the
    /// number of partitions in the plan, and T is the number of tasks provided as argument in
    /// [TaskEstimator::distribute_plan]
    pub(crate) partition_feeds: Vec<BoxStream<'static, Result<Box<dyn AnyMessage>>>>,
}

impl DistributedPlan {
    /// TODO: docs
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            partition_feeds: vec![],
        }
    }

    /// TODO: docs
    pub fn with_partition_feeds<T: Message + 'static>(
        mut self,
        feeds: Vec<impl Stream<Item = Result<T>> + Send + 'static>,
    ) -> Self {
        self.partition_feeds = feeds
            .into_iter()
            .map(|stream| stream.map_ok(|msg| Box::new(msg) as Box<dyn AnyMessage>))
            .map(|boxed| boxed.boxed())
            .collect();
        self
    }
}

/// Given a leaf node, provides an estimation about how many tasks should be used in the
/// stage containing it, and if the leaf node should be replaced by some other.
///
/// The distributed planner will try many [TaskEstimator]s in order until one provides an
/// estimation for a specific leaf node. Once that's done, upper stages will get their task
/// count calculated based on whether lower stages are reducing the cardinality of the data
/// or increasing it.
pub trait TaskEstimator {
    /// User provided arbitrary data. Can be set to `()` if the [TaskEstimator] implementation
    /// is not expected to use this.
    type Data: Send + Sync + 'static;

    /// Function applied to each node that returns a [TaskEstimation] hinting how many
    /// tasks should be used in the [Stage] containing that node.
    ///
    /// All the [TaskEstimator] registered in the session will be applied to the node
    /// until one returns an estimation.
    ///
    ///
    /// If no estimation is returned from any of the registered [TaskEstimator]s, then:
    /// - If the node is a leaf node,`Maximum(1)` is assumed, hinting the distributed planner
    ///   that the leaf node cannot be distributed across tasks.
    /// - If the node is a normal node in the plan, then the maximum task count from its children
    ///   is inherited.
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation<Self::Data>>;

    /// Returns a modified version of the original plan adapted to the fact that it will be executed
    /// in multiple distributed tasks.
    ///
    /// Even if this [TaskEstimator] returned a specific task count, the distributed planner might
    /// decide to not respect for several reasons:
    /// - there is another node declaring a lower [TaskCountAnnotation::Maximum]
    /// - there is another node declaring a higher [TaskCountAnnotation::Desired]
    ///
    /// For this reason, the implementation of [TaskEstimator::distribute_plan] should be prepared
    /// to handle a different task count than the one it provided in [TaskEstimator::task_estimation].
    fn distribute_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_estimation: TaskEstimation<Self::Data>,
        cfg: &ConfigOptions,
    ) -> Option<DistributedPlan>;
}

impl TaskEstimator for usize {
    type Data = ();

    fn task_estimation(
        &self,
        inputs: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation<()>> {
        if inputs.children().is_empty() {
            Some(TaskEstimation {
                task_count: TaskCountAnnotation::Desired(*self),
                user_data: (),
            })
        } else {
            None
        }
    }

    fn distribute_plan(
        &self,
        _: &Arc<dyn ExecutionPlan>,
        _: TaskEstimation<Self::Data>,
        _: &ConfigOptions,
    ) -> Option<DistributedPlan> {
        None
    }
}

/// Type-erased version of [TaskEstimator] that allows storing heterogeneous estimators
/// with different `Data` types in a single collection.
pub(crate) trait ErasedTaskEstimator: Send + Sync {
    fn task_estimation_erased(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation<Box<dyn Any + Send + Sync>>>;

    fn distribute_plan_erased(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_estimation: TaskEstimation<Box<dyn Any + Send + Sync>>,
        cfg: &ConfigOptions,
    ) -> Option<DistributedPlan>;
}

impl<T: TaskEstimator + Send + Sync> ErasedTaskEstimator for T {
    fn task_estimation_erased(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation<Box<dyn Any + Send + Sync>>> {
        self.task_estimation(plan, cfg).map(|est| TaskEstimation {
            task_count: est.task_count,
            user_data: Box::new(est.user_data) as Box<dyn Any + Send + Sync>,
        })
    }

    fn distribute_plan_erased(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_estimation: TaskEstimation<Box<dyn Any + Send + Sync>>,
        cfg: &ConfigOptions,
    ) -> Option<DistributedPlan> {
        let data = *task_estimation.user_data.downcast::<T::Data>().ok()?;
        self.distribute_plan(
            plan,
            TaskEstimation {
                task_count: task_estimation.task_count,
                user_data: data,
            },
            cfg,
        )
    }
}

/// Data produced by [CombinedTaskEstimator::task_estimation] that tracks which estimator
/// produced the result so that [CombinedTaskEstimator::distribute_plan] can route back
/// to the correct estimator.
pub(crate) struct CombinedEstimationData {
    estimator: Arc<dyn ErasedTaskEstimator>,
    data: Box<dyn Any + Send + Sync>,
}

pub(crate) fn set_distributed_task_estimator(
    cfg: &mut SessionConfig,
    estimator: impl TaskEstimator + Send + Sync + 'static,
) {
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg
            .__private_task_estimator
            .user_provided
            .push(Arc::new(estimator) as Arc<dyn ErasedTaskEstimator>);
    } else {
        let mut estimators = CombinedTaskEstimator::default();
        estimators
            .user_provided
            .push(Arc::new(estimator) as Arc<dyn ErasedTaskEstimator>);
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_task_estimator: estimators,
                ..Default::default()
            },
        )
    }
}

/// [TaskEstimator] implementation that acts on [DataSourceExec] nodes that contain
/// [FileScanConfig]s data sources (e.g., Parquet or CSV files). it will read the
/// [DistributedConfig].`files_per_task` field and assigns as many tasks as needed so that
/// no task handles more than the configured files.
#[derive(Debug)]
struct FileScanConfigTaskEstimator;

impl TaskEstimator for FileScanConfigTaskEstimator {
    type Data = ();

    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation<Self::Data>> {
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let file_scan: &FileScanConfig = dse.data_source().as_any().downcast_ref()?;

        let d_cfg = cfg.extensions.get::<DistributedConfig>()?;

        // Count how many partitioned files we have in the FileScanConfig.
        let mut partitioned_files = 0;
        for file_group in &file_scan.file_groups {
            partitioned_files += file_group.len();
        }

        // Based on the user-provided files_per_task configuration, do the math to calculate
        // how many tasks should be used, without surpassing the number of available workers.
        let task_count = partitioned_files.div_ceil(d_cfg.files_per_task);

        Some(TaskEstimation {
            task_count: TaskCountAnnotation::Desired(task_count),
            user_data: (),
        })
    }

    fn distribute_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_estimation: TaskEstimation<Self::Data>,
        _cfg: &ConfigOptions,
    ) -> Option<DistributedPlan> {
        let task_count = task_estimation.task_count.as_usize();
        if task_count == 1 {
            return None;
        }
        // Based on the task count, attempt to scale up the partitions in the DataSourceExec by
        // repartitioning it. This will result in a DataSourceExec with potentially a lot of
        // partitions, but as we are going to wrap it with PartitionIsolatorExec, that's fine.
        let dse: &DataSourceExec = plan.as_any().downcast_ref()?;
        let file_scan: &FileScanConfig = dse.data_source().as_any().downcast_ref()?;

        let mut new_file_scan = file_scan.clone();
        new_file_scan.file_groups.clear();
        for file_group in file_scan.file_groups.clone() {
            new_file_scan
                .file_groups
                .extend(file_group.split_files(task_count));
        }
        let plan = DataSourceExec::from_data_source(new_file_scan);
        Some(DistributedPlan::new(Arc::new(PartitionIsolatorExec::new(
            plan, task_count,
        ))))
    }
}

/// Tries multiple user-provided [TaskEstimator]s until one returns an estimation. If none
/// returns an estimation, a set of default [TaskEstimation] implementations is tried. Right
/// now the only default [TaskEstimation] is [FileScanConfigTaskEstimator].
#[derive(Clone, Default)]
pub(crate) struct CombinedTaskEstimator {
    pub(crate) user_provided: Vec<Arc<dyn ErasedTaskEstimator>>,
}

impl CombinedTaskEstimator {
    pub(crate) fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        cfg: &ConfigOptions,
    ) -> Option<TaskEstimation<CombinedEstimationData>> {
        for estimator in &self.user_provided {
            if let Some(result) = estimator.task_estimation_erased(plan, cfg) {
                return Some(TaskEstimation {
                    task_count: result.task_count,
                    user_data: CombinedEstimationData {
                        estimator: Arc::clone(estimator),
                        data: result.user_data,
                    },
                });
            }
        }
        // We want to execute the default estimators last so that the user-provided ones have
        // a chance of providing an estimation.
        // If none of the user-provided returned an estimation, the default ones are used.
        let default_estimator: Arc<dyn ErasedTaskEstimator> = Arc::new(FileScanConfigTaskEstimator);
        if let Some(result) = default_estimator.task_estimation_erased(plan, cfg) {
            return Some(TaskEstimation {
                task_count: result.task_count,
                user_data: CombinedEstimationData {
                    estimator: default_estimator,
                    data: result.user_data,
                },
            });
        }
        None
    }

    pub(crate) fn distribute_plan(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_estimation: TaskEstimation<CombinedEstimationData>,
        cfg: &ConfigOptions,
    ) -> Option<DistributedPlan> {
        let CombinedEstimationData { estimator, data } = task_estimation.user_data;
        estimator.distribute_plan_erased(
            plan,
            TaskEstimation {
                task_count: task_estimation.task_count,
                user_data: data,
            },
            cfg,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::networking::WorkerResolverExtension;
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::test_utils::parquet::register_parquet_tables;
    use datafusion::error::DataFusionError;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_first_user_estimator_wins() -> Result<(), DataFusionError> {
        let mut combined = CombinedTaskEstimator::default();
        combined.push(10);
        combined.push(20);

        let node = make_data_source_exec().await?;
        assert_eq!(combined.task_count(node, |cfg| cfg), 10);
        Ok(())
    }

    #[tokio::test]
    async fn test_continues_until_some() -> Result<(), DataFusionError> {
        let mut combined = CombinedTaskEstimator::default();
        combined.push(|_: &Arc<dyn ExecutionPlan>, _: &ConfigOptions| None);
        combined.push(30);

        let node = make_data_source_exec().await?;
        assert_eq!(combined.task_count(node, |cfg| cfg), 30);
        Ok(())
    }

    #[tokio::test]
    async fn test_defaults_to_file_scan_config_task_estimator() -> Result<(), DataFusionError> {
        let mut combined = CombinedTaskEstimator::default();
        combined.push(|_: &Arc<dyn ExecutionPlan>, _: &ConfigOptions| None);

        let node = make_data_source_exec().await?;
        assert_eq!(combined.task_count(node, |cfg| cfg), 3);
        Ok(())
    }

    impl CombinedTaskEstimator {
        fn push(&mut self, value: impl TaskEstimator + Send + Sync + 'static) {
            self.user_provided.push(Arc::new(value));
        }

        fn task_count(
            &self,
            node: Arc<dyn ExecutionPlan>,
            f: impl FnOnce(DistributedConfig) -> DistributedConfig,
        ) -> usize {
            let mut cfg = ConfigOptions::default();
            let d_cfg = DistributedConfig {
                files_per_task: 1,
                __private_worker_resolver: WorkerResolverExtension(Arc::new(
                    InMemoryWorkerResolver::new(3),
                )),
                ..Default::default()
            };
            cfg.extensions.insert(f(d_cfg));
            self.task_estimation(&node, &cfg)
                .unwrap()
                .task_count
                .as_usize()
        }
    }

    async fn make_data_source_exec() -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let ctx = SessionContext::new();
        register_parquet_tables(&ctx).await?;
        let mut plan = ctx
            .sql("SELECT * FROM weather")
            .await?
            .create_physical_plan()
            .await?;
        while !plan.children().is_empty() {
            plan = Arc::clone(plan.children()[0])
        }
        Ok(plan)
    }

    impl<F: Fn(&Arc<dyn ExecutionPlan>, &ConfigOptions) -> Option<TaskEstimation>> TaskEstimator for F {
        type Data = ();

        fn task_estimation(
            &self,
            plan: &Arc<dyn ExecutionPlan>,
            cfg: &ConfigOptions,
        ) -> Option<TaskEstimation> {
            self(plan, cfg)
        }

        fn distribute_plan(
            &self,
            _plan: &Arc<dyn ExecutionPlan>,
            _task_estimation: TaskEstimation<Self::Data>,
            _cfg: &ConfigOptions,
        ) -> Option<DistributedPlan> {
            None
        }
    }
}
