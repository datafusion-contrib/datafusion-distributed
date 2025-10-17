use super::{NetworkShuffleExec, PartitionIsolatorExec};
use crate::execution_plans::{DistributedExec, NetworkCoalesceExec};
use crate::stage::Stage;
use datafusion::common::plan_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::streaming::StreamingTableExec;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, repartition::RepartitionExec},
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use uuid::Uuid;

/// Physical optimizer rule that inspects the plan, places the appropriate network
/// boundaries and breaks it down into stages that can be executed in a distributed manner.
///
/// The rule has two steps:
///
/// 1. Inject the appropriate distributed execution nodes in the appropriate places.
///
///  This is done by looking at specific nodes in the original plan and enhancing them
///  with new additional nodes:
///  - a [DataSourceExec] is wrapped with a [PartitionIsolatorExec] for exposing just a subset
///    of the [DataSourceExec] partitions to the rest of the plan.
///  - a [CoalescePartitionsExec] is followed by a [NetworkCoalesceExec] so that all tasks in the
///    previous stage collapse into just 1 in the next stage.
///  - a [SortPreservingMergeExec] is followed by a [NetworkCoalesceExec] for the same reasons as
///    above
///  - a [RepartitionExec] with a hash partition is wrapped with a [NetworkShuffleExec] for
///    shuffling data to different tasks.
///
///
/// 2. Break down the plan into stages
///  
///  Based on the network boundaries ([NetworkShuffleExec], [NetworkCoalesceExec], ...) placed in
///  the plan by the first step, the plan is divided into stages and tasks are assigned to each
///  stage.
///
///  This step might decide to not respect the amount of tasks each network boundary is requesting,
///  like when a plan is not parallelizable in different tasks (e.g. a collect left [HashJoinExec])
///  or when a [DataSourceExec] has not enough partitions to be spread across tasks.
#[derive(Debug, Default)]
pub struct DistributedPhysicalOptimizerRule {
    /// Upon shuffling data, this defines how many tasks are employed into performing the shuffling.
    /// ```text
    ///  ( task 1 )  ( task 2 ) ( task 3 )
    ///      ▲           ▲          ▲
    ///      └────┬──────┴─────┬────┘
    ///       ( task 1 )  ( task 2 )       N tasks
    /// ```
    /// This parameter defines N
    network_shuffle_tasks: Option<usize>,
    /// Upon merging multiple tasks into one, this defines how many tasks are merged.
    /// ```text
    ///              ( task 1 )
    ///                  ▲
    ///      ┌───────────┴──────────┐
    ///  ( task 1 )  ( task 2 ) ( task 3 )  N tasks
    /// ```
    /// This parameter defines N
    network_coalesce_tasks: Option<usize>,
}

impl DistributedPhysicalOptimizerRule {
    pub fn new() -> Self {
        DistributedPhysicalOptimizerRule {
            network_shuffle_tasks: None,
            network_coalesce_tasks: None,
        }
    }

    /// Sets the amount of tasks employed in performing shuffles.
    pub fn with_network_shuffle_tasks(mut self, tasks: usize) -> Self {
        self.network_shuffle_tasks = Some(tasks);
        self
    }

    /// Sets the amount of input tasks for every task coalescing operation.
    pub fn with_network_coalesce_tasks(mut self, tasks: usize) -> Self {
        self.network_coalesce_tasks = Some(tasks);
        self
    }
}

impl PhysicalOptimizerRule for DistributedPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // We can only optimize plans that are not already distributed
        let plan = self.apply_network_boundaries(plan)?;
        Self::distribute_plan(plan)
    }

    fn name(&self) -> &str {
        "DistributedPhysicalOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl DistributedPhysicalOptimizerRule {
    fn apply_network_boundaries(
        &self,
        mut plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if plan.output_partitioning().partition_count() > 1 {
            // Coalescing partitions here will allow us to put a NetworkCoalesceExec on top
            // of the plan, executing it in parallel.
            plan = Arc::new(CoalescePartitionsExec::new(plan))
        }

        let result =
            plan.transform_up(|plan| {
                // If this node is a DataSourceExec, we need to wrap it with PartitionIsolatorExec so
                // that not all tasks have access to all partitions of the underlying DataSource.
                if plan.as_any().is::<DataSourceExec>() {
                    let node = PartitionIsolatorExec::new(plan);

                    return Ok(Transformed::yes(Arc::new(node)));
                }

                // If this is a hash RepartitionExec, introduce a shuffle.
                if let (Some(node), Some(tasks)) = (
                    plan.as_any().downcast_ref::<RepartitionExec>(),
                    self.network_shuffle_tasks,
                ) {
                    if !matches!(node.partitioning(), Partitioning::Hash(_, _)) {
                        return Ok(Transformed::no(plan));
                    }
                    let node = NetworkShuffleExec::try_new(plan, tasks)?;

                    return Ok(Transformed::yes(Arc::new(node)));
                }

                // If this is a CoalescePartitionsExec, it means that the original plan is trying to
                // merge all partitions into one. We need to go one step ahead and also merge all tasks
                // into one.
                if let (Some(node), Some(tasks)) = (
                    plan.as_any().downcast_ref::<CoalescePartitionsExec>(),
                    self.network_coalesce_tasks,
                ) {
                    // If the immediate child is a PartitionIsolatorExec, it means that the rest of the
                    // plan is just a couple of non-computational nodes that are probably not worth
                    // distributing.
                    if node.input().as_any().is::<PartitionIsolatorExec>() {
                        return Ok(Transformed::no(plan));
                    }

                    let plan = plan.clone().with_new_children(vec![Arc::new(
                        NetworkCoalesceExec::new(Arc::clone(node.input()), tasks),
                    )])?;

                    return Ok(Transformed::yes(plan));
                }

                // The SortPreservingMergeExec node will try to coalesce all partitions into just 1.
                // We need to account for it and help it by also coalescing all tasks into one, therefore
                // a NetworkCoalesceExec is introduced.
                if let (Some(node), Some(tasks)) = (
                    plan.as_any().downcast_ref::<SortPreservingMergeExec>(),
                    self.network_coalesce_tasks,
                ) {
                    let plan = plan.clone().with_new_children(vec![Arc::new(
                        NetworkCoalesceExec::new(Arc::clone(node.input()), tasks),
                    )])?;

                    return Ok(Transformed::yes(plan));
                }

                Ok(Transformed::no(plan))
            })?;
        Ok(result.data)
    }

    /// Takes a plan with certain network boundaries in it ([NetworkShuffleExec], [NetworkCoalesceExec], ...)
    /// and breaks it down into stages.
    ///
    /// This can be used a standalone function for distributing arbitrary plans in which users have
    /// manually placed network boundaries, or as part of the [DistributedPhysicalOptimizerRule] that
    /// places the network boundaries automatically as a standard [PhysicalOptimizerRule].
    pub fn distribute_plan(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let stage = match Self::_distribute_plan_inner(Uuid::new_v4(), plan.clone(), &mut 1, 0, 1) {
            Ok(stage) => stage,
            Err(err) => {
                return match get_distribute_plan_err(&err) {
                    Some(DistributedPlanError::NonDistributable(_)) => plan
                        .transform_down(|plan| {
                            // If the node cannot be distributed, rollback all the network boundaries.
                            if let Some(nb) = plan.as_network_boundary() {
                                return Ok(Transformed::yes(nb.rollback()?));
                            }
                            Ok(Transformed::no(plan))
                        })
                        .map(|v| v.data),
                    _ => Err(err),
                };
            }
        };
        let plan = stage.plan.decoded()?;
        Ok(Arc::new(DistributedExec::new(Arc::clone(plan))))
    }

    fn _distribute_plan_inner(
        query_id: Uuid,
        plan: Arc<dyn ExecutionPlan>,
        num: &mut usize,
        depth: usize,
        n_tasks: usize,
    ) -> Result<Stage, DataFusionError> {
        let mut distributed = plan.clone().transform_down(|plan| {
            // We cannot break down CollectLeft hash joins into more than 1 task, as these need
            // a full materialized build size with all the data in it.
            //
            // Maybe in the future these can be broadcast joins?
            if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>() {
                if n_tasks > 1 && node.mode == PartitionMode::CollectLeft {
                    return Err(limit_tasks_err(1));
                }
            }

            // We cannot distribute [StreamingTableExec] nodes, so abort distribution.
            if plan.as_any().is::<StreamingTableExec>() {
                return Err(non_distributable_err(StreamingTableExec::static_name()))
            }

            if let Some(node) = plan.as_any().downcast_ref::<PartitionIsolatorExec>() {
                // If there's only 1 task, no need to perform any isolation.
                if n_tasks == 1 {
                    return Ok(Transformed::yes(Arc::clone(plan.children().first().unwrap())));
                }
                let node = node.ready(n_tasks)?;
                return Ok(Transformed::new(Arc::new(node), true, TreeNodeRecursion::Jump));
            }

            let Some(mut dnode) = plan.as_network_boundary().map(Referenced::Borrowed) else {
                return Ok(Transformed::no(plan));
            };

            let stage = loop {
                let input_stage_info = dnode.as_ref().get_input_stage_info(n_tasks)?;
                // If the current stage has just 1 task, and the next stage is only going to have
                // 1 task, there's no point in having a network boundary in between, they can just
                // communicate in memory.
                if n_tasks == 1 && input_stage_info.task_count == 1 {
                    let mut n = dnode.as_ref().rollback()?;
                    if let Some(node) = n.as_any().downcast_ref::<PartitionIsolatorExec>() {
                        // Also trim PartitionIsolatorExec out of the plan.
                        n = Arc::clone(node.children().first().unwrap());
                    }
                    return Ok(Transformed::yes(n));
                }
                match Self::_distribute_plan_inner(query_id, input_stage_info.plan, num, depth + 1, input_stage_info.task_count) {
                    Ok(v) => break v,
                    Err(e) => match get_distribute_plan_err(&e) {
                        None => return Err(e),
                        Some(DistributedPlanError::LimitTasks(limit)) => {
                            // While attempting to build a new stage, a failure was raised stating
                            // that no more than `limit` tasks can be used for it, so we are going
                            // to limit the amount of tasks to the requested number and try building
                            // the stage again.
                            if input_stage_info.task_count == *limit {
                                return plan_err!("A node requested {limit} tasks for the stage its in, but that stage already has that many tasks");
                            }
                            dnode = Referenced::Arced(dnode.as_ref().with_input_task_count(*limit)?);
                        }
                        Some(DistributedPlanError::NonDistributable(_)) => {
                            // This full plan is non-distributable, so abort any task and stage
                            // assignation.
                            return Err(e);
                        }
                    },
                }
            };
            let node = dnode.as_ref().with_input_stage(stage)?;
            Ok(Transformed::new(node, true, TreeNodeRecursion::Jump))
        })?;

        // The head stage is executable, and upon execution, it will lazily assign worker URLs to
        // all tasks. This must only be done once, so the executable StageExec must only be called
        // once on 1 partition.
        if depth == 0 && distributed.data.output_partitioning().partition_count() > 1 {
            distributed.data = Arc::new(CoalescePartitionsExec::new(distributed.data));
        }

        let stage = Stage::new(query_id, *num, distributed.data, n_tasks);
        *num += 1;
        Ok(stage)
    }
}

/// Necessary information for building a [Stage] during distributed planning.
///
/// [NetworkBoundary]s return this piece of data so that the distributed planner know how to
/// build the next [Stage] from which the [NetworkBoundary] is going to receive data.
///
/// Some network boundaries might perform some modifications in their children, like scaling
/// up the number of partitions, or injecting a specific [ExecutionPlan] on top.
pub struct InputStageInfo {
    /// The head plan of the [Stage] that is about to be built.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The amount of tasks the [Stage] will have.
    pub task_count: usize,
}

/// This trait represents a node that introduces the necessity of a network boundary in the plan.
/// The distributed planner, upon stepping into one of these, will break the plan and build a stage
/// out of it.
pub trait NetworkBoundary: ExecutionPlan {
    /// Returns the information necessary for building the next stage from which this
    /// [NetworkBoundary] is going to collect data.
    fn get_input_stage_info(&self, task_count: usize) -> Result<InputStageInfo>;

    /// re-assigns a different number of input tasks to the current [NetworkBoundary].
    ///
    /// This will be called if upon building a stage, a [DistributedPlanError::LimitTasks] error
    /// is returned, prompting the [NetworkBoundary] to choose a different number of input tasks.
    fn with_input_task_count(&self, input_tasks: usize) -> Result<Arc<dyn NetworkBoundary>>;

    /// Called when a [Stage] is correctly formed. The [NetworkBoundary] can use this
    /// information to perform any internal transformations necessary for distributed execution.
    ///
    /// Typically, [NetworkBoundary]s will use this call for transitioning from "Pending" to "ready".
    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>>;

    /// Returns the assigned input [Stage], if any.
    fn input_stage(&self) -> Option<&Stage>;

    /// The planner might decide to remove this [NetworkBoundary] from the plan if it decides that
    /// it's not going to bring any benefit. The [NetworkBoundary] will be replaced with whatever
    /// this function returns.
    fn rollback(&self) -> Result<Arc<dyn ExecutionPlan>> {
        let children = self.children();
        if children.len() != 1 {
            return plan_err!(
                "Expected distributed node {} to have exactly 1 children, but got {}",
                self.name(),
                children.len()
            );
        }
        Ok(Arc::clone(children.first().unwrap()))
    }
}

/// Extension trait for downcasting dynamic types to [NetworkBoundary].
pub trait NetworkBoundaryExt {
    /// Downcasts self to a [NetworkBoundary] if possible.
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary>;
    /// Returns whether self is a [NetworkBoundary] or not.
    fn is_network_boundary(&self) -> bool {
        self.as_network_boundary().is_some()
    }
}

impl NetworkBoundaryExt for dyn ExecutionPlan {
    fn as_network_boundary(&self) -> Option<&dyn NetworkBoundary> {
        if let Some(node) = self.as_any().downcast_ref::<NetworkShuffleExec>() {
            Some(node)
        } else if let Some(node) = self.as_any().downcast_ref::<NetworkCoalesceExec>() {
            Some(node)
        } else {
            None
        }
    }
}

/// Helper enum for storing either borrowed or owned trait object references
enum Referenced<'a, T: ?Sized> {
    Borrowed(&'a T),
    Arced(Arc<T>),
}

impl<T: ?Sized> Referenced<'_, T> {
    fn as_ref(&self) -> &T {
        match self {
            Self::Borrowed(r) => r,
            Self::Arced(arc) => arc.as_ref(),
        }
    }
}

/// Error thrown during distributed planning that prompts the planner to change something and
/// try again.
#[derive(Debug)]
enum DistributedPlanError {
    /// Prompts the planner to limit the amount of tasks used in the stage that is currently
    /// being planned.
    LimitTasks(usize),
    /// Signals the planner that this whole plan is non-distributable. This can happen if
    /// certain nodes are present, like [StreamingTableExec], which are typically used in
    /// queries that rather performing some execution, they perform some introspection.
    NonDistributable(&'static str),
}

impl Display for DistributedPlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributedPlanError::LimitTasks(n) => write!(f, "LimitTasksErr: {n}"),
            DistributedPlanError::NonDistributable(name) => write!(f, "NonDistributable: {name}"),
        }
    }
}

impl Error for DistributedPlanError {}

/// Builds a [DistributedPlanError::LimitTasks] error. This error prompts the distributed planner
/// to try rebuilding the current stage with a limited amount of tasks.
pub fn limit_tasks_err(limit: usize) -> DataFusionError {
    DataFusionError::External(Box::new(DistributedPlanError::LimitTasks(limit)))
}

/// Builds a [DistributedPlanError::NonDistributable] error. This error prompts the distributed
/// planner to not distribute the query at all.
pub fn non_distributable_err(name: &'static str) -> DataFusionError {
    DataFusionError::External(Box::new(DistributedPlanError::NonDistributable(name)))
}

fn get_distribute_plan_err(err: &DataFusionError) -> Option<&DistributedPlanError> {
    let DataFusionError::External(err) = err else {
        return None;
    };
    err.downcast_ref()
}

#[cfg(test)]
mod tests {
    use crate::distributed_physical_optimizer_rule::DistributedPhysicalOptimizerRule;
    use crate::test_utils::parquet::register_parquet_tables;
    use crate::{assert_snapshot, display_plan_ascii};
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::sync::Arc;

    /* shema for the "weather" table

     MinTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     MaxTemp [type=DOUBLE] [repetitiontype=OPTIONAL]
     Rainfall [type=DOUBLE] [repetitiontype=OPTIONAL]
     Evaporation [type=DOUBLE] [repetitiontype=OPTIONAL]
     Sunshine [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustDir [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindGustSpeed [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindDir3pm [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed9am [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     WindSpeed3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Humidity3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Pressure9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Pressure3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     Cloud9am [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Cloud3pm [type=INT64] [convertedtype=INT_64] [repetitiontype=OPTIONAL]
     Temp9am [type=DOUBLE] [repetitiontype=OPTIONAL]
     Temp3pm [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainToday [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
     RISK_MM [type=DOUBLE] [repetitiontype=OPTIONAL]
     RainTomorrow [type=BYTE_ARRAY] [convertedtype=UTF8] [repetitiontype=OPTIONAL]
    */

    #[tokio::test]
    async fn test_select_all() {
        let query = r#"SELECT * FROM weather"#;
        let plan = sql_to_explain(query, 1).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
        └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0,p1,p2,p3] t1:[p0,p1,p2,p3] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7] t1:[p0,p1,p2,p3,p4,p5,p6,p7] 
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │       PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
            │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_partitions_per_task() {
        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0,p1,p2,p3] t1:[p0,p1,p2,p3] 
          │ SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
          │   ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
          │     AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       CoalesceBatchesExec: target_batch_size=8192
          │         [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7] t1:[p0,p1,p2,p3,p4,p5,p6,p7] 
            │ RepartitionExec: partitioning=Hash([RainToday@0], 8), input_partitions=4
            │   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │       PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
            │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday" "#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   CoalesceBatchesExec: target_batch_size=8192
        │     HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
        │       CoalescePartitionsExec
        │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        │       DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join_distributed() {
        let query = r#"
        WITH a AS (
            SELECT
                AVG("MinTemp") as "MinTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'yes'
            GROUP BY "RainTomorrow"
        ), b AS (
            SELECT
                AVG("MaxTemp") as "MaxTemp",
                "RainTomorrow"
            FROM weather
            WHERE "RainToday" = 'no'
            GROUP BY "RainTomorrow"
        )
        SELECT
            a."MinTemp",
            b."MaxTemp"
        FROM a
        LEFT JOIN b
        ON a."RainTomorrow" = b."RainTomorrow"

        "#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   CoalesceBatchesExec: target_batch_size=8192
        │     HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
        │       CoalescePartitionsExec
        │         [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        │       ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
        │         AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
        │           CoalesceBatchesExec: target_batch_size=8192
        │             [Stage 3] => NetworkShuffleExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0,p1,p2,p3] t1:[p0,p1,p2,p3] 
          │ ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7] t1:[p0,p1,p2,p3,p4,p5,p6,p7] 
            │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 8), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
            │     CoalesceBatchesExec: target_batch_size=8192
            │       FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
            │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │           PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
            │             DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
            └──────────────────────────────────────────────────
          ┌───── Stage 3 ── Tasks: t0:[p0,p1,p2,p3] t1:[p0,p1,p2,p3] 
          │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=4
          │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
          │           PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
          │             DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = no, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_sort() {
        let query = r#"SELECT * FROM weather ORDER BY "MinTemp" DESC "#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── Tasks: t0:[p0,p1] t1:[p2,p3] 
          │ SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
          │   PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
          │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"SELECT DISTINCT "RainToday", "WindGustDir" FROM weather"#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── DistributedExec ── Tasks: t0:[p0] 
        │ CoalescePartitionsExec
        │   [Stage 2] => NetworkCoalesceExec: output_partitions=8, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2 ── Tasks: t0:[p0,p1,p2,p3] t1:[p0,p1,p2,p3] 
          │ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │   CoalesceBatchesExec: target_batch_size=8192
          │     [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1 ── Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7] t1:[p0,p1,p2,p3,p4,p5,p6,p7] 
            │ RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 8), input_partitions=4
            │   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │       PartitionIsolatorExec: t0:[p0,p1,__] t1:[__,__,p0] 
            │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_show_columns() {
        let query = r#"SHOW COLUMNS from weather"#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        CoalescePartitionsExec
          ProjectionExec: expr=[table_catalog@0 as table_catalog, table_schema@1 as table_schema, table_name@2 as table_name, column_name@3 as column_name, data_type@5 as data_type, is_nullable@4 as is_nullable]
            CoalesceBatchesExec: target_batch_size=8192
              FilterExec: table_name@2 = weather
                RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
                  StreamingTableExec: partition_sizes=1, projection=[table_catalog, table_schema, table_name, column_name, is_nullable, data_type]
        ");
    }

    async fn sql_to_explain(query: &str, tasks: usize) -> Result<String, DataFusionError> {
        sql_to_explain_with_rule(
            query,
            DistributedPhysicalOptimizerRule::new()
                .with_network_shuffle_tasks(tasks)
                .with_network_coalesce_tasks(tasks),
        )
        .await
    }

    async fn sql_to_explain_with_rule(
        query: &str,
        rule: DistributedPhysicalOptimizerRule,
    ) -> Result<String, DataFusionError> {
        let config = SessionConfig::new()
            .with_target_partitions(4)
            .with_information_schema(true);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .with_config(config)
            .build();

        let ctx = SessionContext::new_with_state(state);
        register_parquet_tables(&ctx).await?;

        let df = ctx.sql(query).await?;

        let physical_plan = df.create_physical_plan().await?;
        Ok(display_plan_ascii(physical_plan.as_ref()))
    }
}
