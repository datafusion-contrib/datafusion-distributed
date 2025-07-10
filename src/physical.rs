// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        analyze::AnalyzeExec, coalesce_partitions::CoalescePartitionsExec,
        joins::NestedLoopJoinExec, repartition::RepartitionExec, sorts::sort::SortExec,
        ExecutionPlan,
    },
};

use crate::{
    analyze::{DistributedAnalyzeExec, DistributedAnalyzeRootExec},
    logging::info,
    stage::DFRayStageExec,
    util::display_plan_with_partition_counts,
};

/// This optimizer rule walks up the physical plan tree
/// and inserts RayStageExec nodes where appropriate to denote where we will
/// split the plan into stages.
///
/// The RayStageExec nodes are merely markers to inform where to break the plan
/// up.
///
/// Later, the plan will be examined again to actually split it up.
/// These RayStageExecs serve as markers where we know to break it up on a
/// network boundary and we can insert readers and writers as appropriate.
#[derive(Debug)]
pub struct DFRayStageOptimizerRule {}

impl Default for DFRayStageOptimizerRule {
    fn default() -> Self {
        Self::new()
    }
}
impl DFRayStageOptimizerRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for DFRayStageOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        info!(
            "optimizing physical plan:\n{}",
            display_plan_with_partition_counts(&plan)
        );

        let mut stage_counter = 0;

        let up = |plan: Arc<dyn ExecutionPlan>| {
            if plan.as_any().downcast_ref::<RepartitionExec>().is_some()
                || plan.as_any().downcast_ref::<SortExec>().is_some()
                || plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some()
            {
                // insert a stage marker here so we know where to break up the physical plan later
                let stage = Arc::new(DFRayStageExec::new(plan, stage_counter));
                stage_counter += 1;
                Ok(Transformed::yes(stage as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(plan))
            }
        };

        let plan = plan.clone().transform_up(up)?.data;
        let final_plan =
            Arc::new(DFRayStageExec::new(plan, stage_counter)) as Arc<dyn ExecutionPlan>;

        info!(
            "optimized physical plan:\n{}",
            display_plan_with_partition_counts(&final_plan)
        );
        Ok(final_plan)
    }

    fn name(&self) -> &str {
        "RayStageOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;

    use datafusion::execution::context::SessionContext;
    use datafusion::physical_plan::displayable;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_optimize_with_explain_analyze() {
        // Create a session context
        let ctx = SessionContext::new();

        // Define a schema for the in-memory table
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create some data for the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        // Register the in-memory table
        ctx.register_batch("test_table", batch).unwrap();

        // Run the EXPLAIN ANALYZE query
        let df = ctx
            .sql("EXPLAIN ANALYZE SELECT * FROM test_table")
            .await
            .unwrap();
        // get the physical plan from the dataframe
        let physical_plan = df.create_physical_plan().await.unwrap();

        // Apply the optimizer
        let optimizer = DFRayStageOptimizerRule::new();
        let optimized_physical_plan = optimizer
            .optimize(physical_plan.clone(), &Default::default())
            .unwrap();

        let data_source = ctx
            .table_provider("test_table")
            .await
            .unwrap()
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap();

        let coalesce = Arc::new(CoalescePartitionsExec::new(data_source));

        let analyze = Arc::new(DistributedAnalyzeRootExec::new(coalesce, false, false));

        let target_plan = DFRayStageExec::new(
            analyze, 0, // stage counter
        );

        assert_eq!(
            displayable(optimized_physical_plan.as_ref())
                .indent(true)
                .to_string(),
            displayable(&target_plan).indent(true).to_string()
        );
    }
}
