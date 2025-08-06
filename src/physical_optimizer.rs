use std::sync::Arc;

use datafusion::{
    common::{
        internal_datafusion_err,
        tree_node::{Transformed, TreeNode, TreeNodeRewriter},
    },
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        displayable, repartition::RepartitionExec, ExecutionPlan, ExecutionPlanProperties,
    },
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::{plan::PartitionIsolatorExec, ArrowFlightReadExec};

use super::stage::ExecutionStage;

#[derive(Debug, Default)]
pub struct DistributedPhysicalOptimizerRule {
    /// Optional codec to assist in serializing and deserializing any custom
    /// ExecutionPlan nodes
    codec: Option<Arc<dyn PhysicalExtensionCodec>>,
    /// maximum number of partitions per task. This is used to determine how many
    /// tasks to create for each stage
    partitions_per_task: Option<usize>,
}

impl DistributedPhysicalOptimizerRule {
    pub fn new() -> Self {
        DistributedPhysicalOptimizerRule {
            codec: None,
            partitions_per_task: None,
        }
    }

    /// Set a codec to use to assist in serializing and deserializing
    /// custom ExecutionPlan nodes.
    pub fn with_codec(mut self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Set the maximum number of partitions per task. This is used to determine how many
    /// tasks to create for each stage.
    ///
    /// If a stage holds a plan with 10 partitions, and this is set to 3,
    /// then the stage will be split into 4 tasks:
    /// - Task 1: partitions 0, 1, 2
    /// - Task 2: partitions 3, 4, 5
    /// - Task 3: partitions 6, 7, 8
    /// - Task 4: partitions 9
    ///
    /// Each task will be executed on a separate host
    pub fn with_maximum_partitions_per_task(mut self, partitions_per_task: usize) -> Self {
        self.partitions_per_task = Some(partitions_per_task);
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
        if plan.as_any().is::<ExecutionStage>() {
            return Ok(plan);
        }
        println!(
            "DistributedPhysicalOptimizerRule: optimizing plan: {}",
            displayable(plan.as_ref()).indent(false)
        );

        let mut planner = StagePlanner::new(self.codec.clone(), self.partitions_per_task);
        plan.rewrite(&mut planner)?;
        planner
            .finish()
            .map(|stage| stage as Arc<dyn ExecutionPlan>)
    }

    fn name(&self) -> &str {
        "DistributedPhysicalOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// StagePlanner is a TreeNodeRewriter that walks the plan tree and creates
/// a tree of ExecutionStage nodes that represent discrete stages of execution
/// can are separated by a data shuffle.
///
/// See https://howqueryengineswork.com/13-distributed-query.html for more information
/// about distributed execution.
struct StagePlanner {
    /// used to keep track of the current plan head
    plan_head: Option<Arc<dyn ExecutionPlan>>,
    /// Current depth in the plan tree, as we walk the tree
    depth: usize,
    /// Input stages collected so far. Each entry is a tuple of (plan tree depth, stage).
    /// This allows us to keep track of the depth in the plan tree
    /// where we created the stage.   That way when we create a new
    /// stage, we can tell if it is a peer to the current input stages or
    /// should be a parent (if its depth is a smaller number)
    input_stages: Vec<(usize, ExecutionStage)>,
    /// current stage number
    stage_counter: usize,
    /// Optional codec to assist in serializing and deserializing any custom
    codec: Option<Arc<dyn PhysicalExtensionCodec>>,
    /// partitions_per_task is used to determine how many tasks to create for each stage
    partitions_per_task: Option<usize>,
}

impl StagePlanner {
    fn new(
        codec: Option<Arc<dyn PhysicalExtensionCodec>>,
        partitions_per_task: Option<usize>,
    ) -> Self {
        StagePlanner {
            plan_head: None,
            depth: 0,
            input_stages: vec![],
            stage_counter: 1,
            codec,
            partitions_per_task,
        }
    }

    fn finish(mut self) -> Result<Arc<ExecutionStage>> {
        let stage = if self.input_stages.is_empty() {
            ExecutionStage::new(
                self.stage_counter,
                self.plan_head
                    .take()
                    .ok_or_else(|| internal_datafusion_err!("No plan head set"))?,
                vec![],
            )
        } else if self.depth < self.input_stages[0].0 {
            // There is more plan above the last stage we created, so we need to
            // create a new stage that includes the last plan head
            ExecutionStage::new(
                self.stage_counter,
                self.plan_head
                    .take()
                    .ok_or_else(|| internal_datafusion_err!("No plan head set"))?,
                self.input_stages
                    .into_iter()
                    .map(|(_, stage)| Arc::new(stage))
                    .collect(),
            )
        } else {
            // We have a plan head, and we are at the same depth as the last stage we created,
            // so we can just return the last stage
            self.input_stages.last().unwrap().1.clone()
        };

        // assign the proper tree depth to each stage in the tree
        fn assign_tree_depth(stage: &ExecutionStage, depth: usize) {
            stage
                .depth
                .store(depth as u64, std::sync::atomic::Ordering::Relaxed);
            for input in stage.child_stages_iter() {
                assign_tree_depth(input, depth + 1);
            }
        }
        assign_tree_depth(&stage, 0);

        Ok(Arc::new(stage))
    }
}

impl TreeNodeRewriter for StagePlanner {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        self.depth += 1;
        Ok(Transformed::no(plan))
    }

    fn f_up(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        self.depth -= 1;

        // keep track of where we are
        self.plan_head = Some(plan.clone());

        // determine if we need to shuffle data, and thus create a new stage
        // at this shuffle boundary
        if let Some(repartition_exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
            // time to create a stage here so include all previous seen stages deeper than us as
            // our input stages
            let child_stages = self
                .input_stages
                .iter()
                .rev()
                .take_while(|(depth, _)| *depth > self.depth)
                .map(|(_, stage)| stage.clone())
                .collect::<Vec<_>>();

            self.input_stages.retain(|(depth, _)| *depth <= self.depth);

            let maybe_isolated_plan = if let Some(partitions_per_task) = self.partitions_per_task {
                let child = repartition_exec
                    .children()
                    .first()
                    .ok_or(internal_datafusion_err!(
                        "RepartitionExec has no children, cannot create PartitionIsolatorExec"
                    ))?
                    .clone()
                    .clone(); // just clone the Arcs
                let isolated = Arc::new(PartitionIsolatorExec::new(child, partitions_per_task));
                plan.clone().with_new_children(vec![isolated])?
            } else {
                plan.clone()
            };

            let mut stage = ExecutionStage::new(
                self.stage_counter,
                maybe_isolated_plan,
                child_stages.into_iter().map(Arc::new).collect(),
            );

            if let Some(partitions_per_task) = self.partitions_per_task {
                stage = stage.with_maximum_partitions_per_task(partitions_per_task);
            }
            if let Some(codec) = self.codec.as_ref() {
                stage = stage.with_codec(codec.clone());
            }

            self.input_stages.push((self.depth, stage));

            // As we are walking up the plan tree, we've now put what we've encountered so far
            // into a stage.   We want to replace this plan now with an ArrowFlightReadExec
            // which will be able to consume from this stage over the network.
            //
            // That way as we walk further up the tree and build the next stage, the leaf
            // node in that plan will be an ArrowFlightReadExec that can read from
            //
            // Note that we use the original plans partitioning and schema for ArrowFlightReadExec.
            // If we divide it up in to tasks, then that parittion will need to be gathered from
            // among them
            let name = format!("Stage {:<3}", self.stage_counter);
            let read = Arc::new(ArrowFlightReadExec::new(
                plan.output_partitioning().clone(),
                plan.schema(),
                self.stage_counter,
            ));

            self.stage_counter += 1;

            Ok(Transformed::yes(read as Self::Node))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::physical_optimizer::DistributedPhysicalOptimizerRule;
    use crate::test_utils::register_parquet_tables;
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
        let plan = sql_to_explain(query).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 1   Task: partitions: 0,unassigned]
        │partitions [out:1            ] DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
        │
        └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation() {
        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;
        let plan = sql_to_explain(query).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 3   Task: partitions: 0,unassigned]
        │partitions [out:1  <-- in:1  ] ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │partitions [out:1  <-- in:4  ]   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │partitions [out:4  <-- in:4  ]     SortExec: expr=[count(Int64(1))@2 ASC NULLS LAST], preserve_partitioning=[true]
        │partitions [out:4  <-- in:4  ]       ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
        │partitions [out:4  <-- in:4  ]         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │partitions [out:4  <-- in:4  ]           CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:4            ]             ArrowFlightReadExec: Stage 2  
        │
        └──────────────────────────────────────────────────
          ┌───── Stage 2   Task: partitions: 0..3,unassigned]
          │partitions [out:4  <-- in:4  ] RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=4
          │partitions [out:4            ]   ArrowFlightReadExec: Stage 1  
          │
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Task: partitions: 0..3,unassigned]
            │partitions [out:4  <-- in:1  ] RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │partitions [out:1  <-- in:1  ]   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │partitions [out:1            ]     DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday], file_type=parquet
            │
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_aggregation_with_partitions_per_task() {
        let query =
            r#"SELECT count(*), "RainToday" FROM weather GROUP BY "RainToday" ORDER BY count(*)"#;
        let plan = sql_to_explain_partitions_per_task(query, 2).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 3   Task: partitions: 0,unassigned]
        │partitions [out:1  <-- in:1  ] ProjectionExec: expr=[count(*)@0 as count(*), RainToday@1 as RainToday]
        │partitions [out:1  <-- in:4  ]   SortPreservingMergeExec: [count(Int64(1))@2 ASC NULLS LAST]
        │partitions [out:4  <-- in:4  ]     SortExec: expr=[count(Int64(1))@2 ASC NULLS LAST], preserve_partitioning=[true]
        │partitions [out:4  <-- in:4  ]       ProjectionExec: expr=[count(Int64(1))@1 as count(*), RainToday@0 as RainToday, count(Int64(1))@1 as count(Int64(1))]
        │partitions [out:4  <-- in:4  ]         AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
        │partitions [out:4  <-- in:4  ]           CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:4            ]             ArrowFlightReadExec: Stage 2  
        │
        └──────────────────────────────────────────────────
          ┌───── Stage 2   Task: partitions: 0,1,unassigned],Task: partitions: 2,3,unassigned]
          │partitions [out:4  <-- in:2  ] RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=2
          │partitions [out:2  <-- in:4  ]   PartitionIsolatorExec [providing upto 2 partitions]
          │partitions [out:4            ]     ArrowFlightReadExec: Stage 1  
          │
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Task: partitions: 0,1,unassigned],Task: partitions: 2,3,unassigned]
            │partitions [out:4  <-- in:2  ] RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=2
            │partitions [out:2  <-- in:1  ]   PartitionIsolatorExec [providing upto 2 partitions]
            │partitions [out:1  <-- in:1  ]     AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
            │partitions [out:1            ]       DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday], file_type=parquet
            │
            └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_left_join() {
        let query = r#"SELECT a."MinTemp", b."MaxTemp" FROM weather a LEFT JOIN weather b ON a."RainToday" = b."RainToday" "#;
        let plan = sql_to_explain(query).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 1   Task: partitions: 0,unassigned]
        │partitions [out:1  <-- in:1  ] CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:1  <-- in:1  ]   HashJoinExec: mode=Partitioned, join_type=Left, on=[(RainToday@1, RainToday@1)], projection=[MinTemp@0, MaxTemp@2]
        │partitions [out:1            ]     DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MinTemp, RainToday], file_type=parquet
        │partitions [out:1            ]     DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MaxTemp, RainToday], file_type=parquet
        │
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
        let plan = sql_to_explain(query).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 5   Task: partitions: 0..3,unassigned]
        │partitions [out:4  <-- in:4  ] CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:4  <-- in:1  ]   HashJoinExec: mode=CollectLeft, join_type=Left, on=[(RainTomorrow@1, RainTomorrow@1)], projection=[MinTemp@0, MaxTemp@2]
        │partitions [out:1  <-- in:4  ]     CoalescePartitionsExec
        │partitions [out:4  <-- in:4  ]       ProjectionExec: expr=[avg(weather.MinTemp)@1 as MinTemp, RainTomorrow@0 as RainTomorrow]
        │partitions [out:4  <-- in:4  ]         AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MinTemp)]
        │partitions [out:4  <-- in:4  ]           CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:4            ]             ArrowFlightReadExec: Stage 2  
        │partitions [out:4  <-- in:4  ]     ProjectionExec: expr=[avg(weather.MaxTemp)@1 as MaxTemp, RainTomorrow@0 as RainTomorrow]
        │partitions [out:4  <-- in:4  ]       AggregateExec: mode=FinalPartitioned, gby=[RainTomorrow@0 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
        │partitions [out:4  <-- in:4  ]         CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:4            ]           ArrowFlightReadExec: Stage 4  
        │
        └──────────────────────────────────────────────────
          ┌───── Stage 4   Task: partitions: 0..3,unassigned]
          │partitions [out:4  <-- in:4  ] RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=4
          │partitions [out:4  <-- in:4  ]   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MaxTemp)]
          │partitions [out:4  <-- in:4  ]     CoalesceBatchesExec: target_batch_size=8192
          │partitions [out:4  <-- in:4  ]       FilterExec: RainToday@1 = no, projection=[MaxTemp@0, RainTomorrow@2]
          │partitions [out:4            ]         ArrowFlightReadExec: Stage 3  
          │
          └──────────────────────────────────────────────────
            ┌───── Stage 3   Task: partitions: 0..3,unassigned]
            │partitions [out:4  <-- in:1  ] RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │partitions [out:1            ]   DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MaxTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = no, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= no AND no <= RainToday_max@1, required_guarantees=[RainToday in (no)]
            │
            │
            └──────────────────────────────────────────────────
            ┌───── Stage 2   Task: partitions: 0..3,unassigned]
            │partitions [out:4  <-- in:4  ] RepartitionExec: partitioning=Hash([RainTomorrow@0], 4), input_partitions=4
            │partitions [out:4  <-- in:4  ]   AggregateExec: mode=Partial, gby=[RainTomorrow@1 as RainTomorrow], aggr=[avg(weather.MinTemp)]
            │partitions [out:4  <-- in:4  ]     CoalesceBatchesExec: target_batch_size=8192
            │partitions [out:4  <-- in:4  ]       FilterExec: RainToday@1 = yes, projection=[MinTemp@0, RainTomorrow@2]
            │partitions [out:4            ]         ArrowFlightReadExec: Stage 1  
            │
            └──────────────────────────────────────────────────
              ┌───── Stage 1   Task: partitions: 0..3,unassigned]
              │partitions [out:4  <-- in:1  ] RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
              │partitions [out:1            ]   DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MinTemp, RainToday, RainTomorrow], file_type=parquet, predicate=RainToday@1 = yes, pruning_predicate=RainToday_null_count@2 != row_count@3 AND RainToday_min@0 <= yes AND yes <= RainToday_max@1, required_guarantees=[RainToday in (yes)]
              │
              │
              └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_sort() {
        let query = r#"SELECT * FROM weather ORDER BY "MinTemp" DESC "#;
        let plan = sql_to_explain(query).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 1   Task: partitions: 0,unassigned]
        │partitions [out:1  <-- in:1  ] SortExec: expr=[MinTemp@0 DESC], preserve_partitioning=[false]
        │partitions [out:1            ]   DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[MinTemp, MaxTemp, Rainfall, Evaporation, Sunshine, WindGustDir, WindGustSpeed, WindDir9am, WindDir3pm, WindSpeed9am, WindSpeed3pm, Humidity9am, Humidity3pm, Pressure9am, Pressure3pm, Cloud9am, Cloud3pm, Temp9am, Temp3pm, RainToday, RISK_MM, RainTomorrow], file_type=parquet
        │
        └──────────────────────────────────────────────────
        ");
    }

    #[tokio::test]
    async fn test_distinct() {
        let query = r#"SELECT DISTINCT "RainToday", "WindGustDir" FROM weather"#;
        let plan = sql_to_explain(query).await.unwrap();
        assert_snapshot!(plan, @r"
        ┌───── Stage 3   Task: partitions: 0..3,unassigned]
        │partitions [out:4  <-- in:4  ] AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
        │partitions [out:4  <-- in:4  ]   CoalesceBatchesExec: target_batch_size=8192
        │partitions [out:4            ]     ArrowFlightReadExec: Stage 2  
        │
        └──────────────────────────────────────────────────
          ┌───── Stage 2   Task: partitions: 0..3,unassigned]
          │partitions [out:4  <-- in:4  ] RepartitionExec: partitioning=Hash([RainToday@0, WindGustDir@1], 4), input_partitions=4
          │partitions [out:4            ]   ArrowFlightReadExec: Stage 1  
          │
          └──────────────────────────────────────────────────
            ┌───── Stage 1   Task: partitions: 0..3,unassigned]
            │partitions [out:4  <-- in:1  ] RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=1
            │partitions [out:1  <-- in:1  ]   AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday, WindGustDir@1 as WindGustDir], aggr=[]
            │partitions [out:1            ]     DataSourceExec: file_groups={1 group: [[/testdata/weather.parquet]]}, projection=[RainToday, WindGustDir], file_type=parquet
            │
            └──────────────────────────────────────────────────
        ");
    }

    async fn sql_to_explain(query: &str) -> Result<String, DataFusionError> {
        sql_to_explain_with_rule(query, DistributedPhysicalOptimizerRule::new()).await
    }

    async fn sql_to_explain_partitions_per_task(
        query: &str,
        partitions_per_task: usize,
    ) -> Result<String, DataFusionError> {
        sql_to_explain_with_rule(
            query,
            DistributedPhysicalOptimizerRule::new()
                .with_maximum_partitions_per_task(partitions_per_task),
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
