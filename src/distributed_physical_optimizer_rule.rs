use super::{NetworkHashShuffleExec, PartitionIsolatorExec, StageExec};
use crate::execution_plans::NetworkCoalesceTasksExec;
use datafusion::common::plan_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{repartition::RepartitionExec, ExecutionPlan},
};
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Default)]
pub struct DistributedPhysicalOptimizerRule {
    /// maximum number of partitions per task. This is used to determine how many
    /// tasks to create for each stage
    network_shuffle_exec_tasks: Option<usize>,

    coalesce_partitions_exec_tasks: Option<usize>,
}

impl DistributedPhysicalOptimizerRule {
    pub fn new() -> Self {
        DistributedPhysicalOptimizerRule {
            network_shuffle_exec_tasks: None,
            coalesce_partitions_exec_tasks: None,
        }
    }

    pub fn with_network_shuffle_exec_tasks(mut self, tasks: usize) -> Self {
        self.network_shuffle_exec_tasks = Some(tasks);
        self
    }

    pub fn with_coalesce_partitions_exec_tasks(mut self, tasks: usize) -> Self {
        self.coalesce_partitions_exec_tasks = Some(tasks);
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
        if plan.as_any().is::<StageExec>() {
            return Ok(plan);
        }

        let plan = self.apply_network_boundaries(plan)?;
        let plan = Self::distribute_plan(plan)?;
        Ok(Arc::new(plan))
    }

    fn name(&self) -> &str {
        "DistributedPhysicalOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

impl DistributedPhysicalOptimizerRule {
    pub fn apply_network_boundaries(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let result = plan.transform_up(|plan| {
            if plan.as_any().is::<DataSourceExec>() {
                let node = PartitionIsolatorExec::new_pending(plan);

                return Ok(Transformed::yes(Arc::new(node)));
            }

            if let Some(node) = plan.as_any().downcast_ref::<RepartitionExec>() {
                let Some(tasks) = self.network_shuffle_exec_tasks else {
                    return Ok(Transformed::no(plan));
                };
                if !matches!(node.partitioning(), Partitioning::Hash(_, _)) {
                    return Ok(Transformed::no(plan));
                }
                let node = NetworkHashShuffleExec::from_repartition_exec(node, tasks)?;

                return Ok(Transformed::yes(Arc::new(node)));
            }

            if let Some(node) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() {
                let Some(tasks) = self.coalesce_partitions_exec_tasks else {
                    return Ok(Transformed::no(plan));
                };
                if node
                    .children()
                    .first()
                    .is_some_and(|v| v.as_any().is::<PartitionIsolatorExec>())
                {
                    return Ok(Transformed::no(plan));
                }
                let node = NetworkCoalesceTasksExec::from_coalesce_partitions_exec(node, tasks)?;

                let plan = plan.with_new_children(vec![Arc::new(node)])?;

                return Ok(Transformed::yes(plan));
            }

            if let Some(node) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
                let Some(tasks) = self.coalesce_partitions_exec_tasks else {
                    return Ok(Transformed::no(plan));
                };
                let node = NetworkCoalesceTasksExec::from_sort_preserving_merge_exec(node, tasks)?;

                let plan = plan.with_new_children(vec![Arc::new(node)])?;

                return Ok(Transformed::yes(plan));
            }

            Ok(Transformed::no(plan))
        })?;
        Ok(result.data)
    }

    pub fn distribute_plan(plan: Arc<dyn ExecutionPlan>) -> Result<StageExec, DataFusionError> {
        Self::_distribute_plan_inner(Uuid::new_v4(), plan, &mut 1, 0, 1)
    }

    fn _distribute_plan_inner(
        query_id: Uuid,
        plan: Arc<dyn ExecutionPlan>,
        num: &mut usize,
        depth: usize,
        n_tasks: usize,
    ) -> Result<StageExec, DataFusionError> {
        let mut inputs = vec![];

        let distributed = plan.clone().transform_down(|plan| {
            if let Some(node) = plan.as_any().downcast_ref::<HashJoinExec>() {
                if n_tasks > 1 && node.mode == PartitionMode::CollectLeft {
                    return Err(limit_tasks_err(1))
                }
            }

            if let Some(node) = plan.as_any().downcast_ref::<PartitionIsolatorExec>() {
                if n_tasks == 1 {
                    return Ok(Transformed::yes(Arc::clone(plan.children().first().unwrap())));
                }
                let node = node.ready(n_tasks)?;
                return Ok(Transformed::new(Arc::new(node), true, TreeNodeRecursion::Jump));
            }

            let mut dnode = if let Some(node) = plan.as_any().downcast_ref::<NetworkHashShuffleExec>() {
                Arc::new(node.clone()) as Arc<dyn DistributedExecutionPlan>
            } else if let Some(node) = plan.as_any().downcast_ref::<NetworkCoalesceTasksExec>() {
                Arc::new(node.clone()) as Arc<dyn DistributedExecutionPlan>
            } else {
                return Ok(Transformed::no(plan));
            };

            let stage = loop {
                let (inner_plan, in_tasks) = dnode.to_stage_info(n_tasks)?;
                match Self::_distribute_plan_inner(query_id, inner_plan, num, depth + 1, in_tasks) {
                    Ok(v) => break v,
                    Err(e) => match get_distribute_plan_err(&e) {
                        None => return Err(e),
                        Some(DistributedPlanError::LimitTasks(limit)) => {
                            if in_tasks == *limit {
                                return plan_err!("A node requested {limit} tasks for the stage its in, but that stage already has that many tasks");
                            }
                            dnode = dnode.with_input_tasks(*limit);
                        }
                    },
                }
            };
            let node = dnode.to_distributed(stage.num, &stage.plan)?;
            inputs.push(stage);
            Ok(Transformed::new(node, true, TreeNodeRecursion::Jump))
        })?;

        let inputs = inputs.into_iter().map(Arc::new).collect();
        let mut stage = StageExec::new(query_id, *num, distributed.data, inputs, n_tasks);
        *num += 1;

        stage.depth = depth;

        Ok(stage)
    }
}

pub trait DistributedExecutionPlan: ExecutionPlan {
    fn to_stage_info(
        &self,
        n_tasks: usize,
    ) -> Result<(Arc<dyn ExecutionPlan>, usize), DataFusionError>;

    fn with_input_tasks(&self, input_tasks: usize) -> Arc<dyn DistributedExecutionPlan>;

    fn to_distributed(
        &self,
        stage_num: usize,
        stage_head: &Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError>;
}

#[derive(Debug)]
enum DistributedPlanError {
    LimitTasks(usize),
}

impl Display for DistributedPlanError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DistributedPlanError::LimitTasks(n) => {
                write!(f, "LimitTasksErr: {n}")
            }
        }
    }
}

impl Error for DistributedPlanError {}

pub fn limit_tasks_err(limit: usize) -> DataFusionError {
    DataFusionError::External(Box::new(DistributedPlanError::LimitTasks(limit)))
}

fn get_distribute_plan_err(err: &DataFusionError) -> Option<&DistributedPlanError> {
    let DataFusionError::External(err) = err else {
        return None;
    };
    err.downcast_ref()
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::distributed_physical_optimizer_rule::DistributedPhysicalOptimizerRule;
    use crate::test_utils::parquet::register_parquet_tables;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::displayable;
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
        ┌───── Stage 1   Tasks: t0:[p0,p1,p2]
        │ DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
        └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 2   Tasks: t0:[p0]
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
        │       ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
        │         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │           CoalesceBatchesExec: target_batch_size=8192
        │             NetworkHashShuffleExec read_from=Stage 1, input_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3] t1:[p4,p5,p6,p7]
          │ RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
          │   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
          │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,p2,p3,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3]
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
        ┌───── Stage 2   Tasks: t0:[p0]
        │ ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │     SortExec: expr=[count(*)@0 ASC NULLS LAST], preserve_partitioning=[true]
        │       ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
        │         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │           CoalesceBatchesExec: target_batch_size=8192
        │             NetworkHashShuffleExec read_from=Stage 1, input_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3] t1:[p4,p5,p6,p7]
          │ RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
          │   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
          │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,p2,p3,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3]
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday" "#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 2   Tasks: t0:[p0,p1,p2]
        │ CoalesceBatchesExec: target_batch_size=8192
        │   HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
        │     CoalescePartitionsExec
        │       NetworkCoalesceTasksExec read_from=Stage 1, output_partitions=6, input_tasks=2
        │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1] t1:[p2,p3]
          │ PartitionIsolatorExec Tasks: t0:[p0,p1,__,__] t1:[__,__,p0,p1]
          │   DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
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
        ┌───── Stage 4   Tasks: t0:[p0,p1,p2,p3]
        │ CoalesceBatchesExec: target_batch_size=8192
        │   HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
        │     CoalescePartitionsExec
        │       NetworkCoalesceTasksExec read_from=Stage 2, output_partitions=8, input_tasks=2
        │     ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
        │       AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
        │         CoalesceBatchesExec: target_batch_size=8192
        │           NetworkHashShuffleExec read_from=Stage 3, input_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3] t1:[p4,p5,p6,p7]
          │ ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
          │   AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       NetworkHashShuffleExec read_from=Stage 1, input_partitions=4, input_tasks=2
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7] t1:[p8,p9,p10,p11,p12,p13,p14,p15]
            │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 8), input_partitions=4
            │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
            │     CoalesceBatchesExec: target_batch_size=8192
            │       FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
            │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │           PartitionIsolatorExec Tasks: t0:[p0,p1,p2,p3,p4,p5,p6,p7,__,__,__,__,__,__,__,__] t1:[__,__,__,__,__,__,__,__,p0,p1,p2,p3,p4,p5,p6,p7]
            │             DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
            └──────────────────────────────────────────────────
          ┌───── Stage 3   Tasks: t0:[p0,p1,p2,p3] t1:[p4,p5,p6,p7]
          │ RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=4
          │   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
          │     CoalesceBatchesExec: target_batch_size=8192
          │       FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
          │         RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
          │           PartitionIsolatorExec Tasks: t0:[p0,p1,p2,p3,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3]
          │             DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = no, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
          └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_sort() {
        let query = r#"SELECT * FROM weather ORDER BY "MinTemp" DESC "#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 1   Tasks: t0:[p0]
        │ SortPreservingMergeExec: [MinTemp@0 DESC]
        │   SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[true]
        │     DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
        └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"SELECT DISTINCT "RainToday", "WindGustDir" FROM weather"#;
        let plan = sql_to_explain(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 2   Tasks: t0:[p0,p1,p2,p3]
        │ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
        │   CoalesceBatchesExec: target_batch_size=8192
        │     NetworkHashShuffleExec read_from=Stage 1, input_partitions=4, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1   Tasks: t0:[p0,p1,p2,p3] t1:[p4,p5,p6,p7]
          │ RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 4), input_partitions=4
          │   RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
          │     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
          │       PartitionIsolatorExec Tasks: t0:[p0,p1,p2,p3,__,__,__,__] t1:[__,__,__,__,p0,p1,p2,p3]
          │         DataSourceExec: file_groups={3 groups: [[/testdata/weather/result-000000.parquet], [/testdata/weather/result-000001.parquet], [/testdata/weather/result-000002.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
          └──────────────────────────────────────────────────
        ");
    }

    async fn sql_to_explain(query: &str, tasks: usize) -> Result<String, DataFusionError> {
        sql_to_explain_with_rule(
            query,
            DistributedPhysicalOptimizerRule::new()
                .with_network_shuffle_exec_tasks(tasks)
                .with_coalesce_partitions_exec_tasks(tasks),
        )
        .await
    }

    async fn sql_to_explain_with_rule(
        query: &str,
        rule: DistributedPhysicalOptimizerRule,
    ) -> Result<String, DataFusionError> {
        let config = SessionConfig::new().with_target_partitions(4);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(rule))
            .with_config(config)
            .build();

        let ctx = SessionContext::new_with_state(state);
        register_parquet_tables(&ctx).await?;

        let df = ctx.sql(query).await?;

        let physical_plan = df.create_physical_plan().await?;
        let display = displayable(physical_plan.as_ref()).indent(true).to_string();

        Ok(display)
    }
}
