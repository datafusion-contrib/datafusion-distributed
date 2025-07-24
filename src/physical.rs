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
        joins::NestedLoopJoinExec, repartition::RepartitionExec, sorts::sort::SortExec,
        ExecutionPlan,
    },
};

use crate::{logging::info, stage::DDStageExec, util::display_plan_with_partition_counts};

/// Physical optimizer rule for inserting distributed execution stage boundaries
///
/// This optimizer rule implements the core logic for transforming a single-node physical
/// execution plan into a distributed execution plan by inserting stage markers. It walks
/// the physical plan tree and identifies operations that require network boundaries,
/// inserting `DDStageExec` nodes as stage markers at these points.
///
/// # Stage Boundary Detection
/// The rule identifies stage boundaries at operations that require data redistribution
/// or coordination across multiple nodes:
/// - **`RepartitionExec`**: Data redistribution/shuffling operations
/// - **`SortExec`**: Global sorting requiring data collection
/// - **`NestedLoopJoinExec`**: Cross joins requiring data broadcast
///
/// # Optimization Process
/// 1. **Tree Traversal**: Bottom-up traversal of the physical plan tree
/// 2. **Boundary Detection**: Identifies operations requiring network boundaries
/// 3. **Stage Insertion**: Inserts `DDStageExec` markers with unique stage IDs
/// 4. **Final Stage**: Wraps the entire plan in a final stage marker
///
/// # Stage Lifecycle
/// The inserted `DDStageExec` nodes serve as markers for later processing:
/// - **Planning Phase**: Used to identify where to split the execution plan
/// - **Distribution Phase**: Replaced with actual network readers/writers
/// - **Execution Phase**: Enable independent execution of plan segments
///
/// # Example Transformation
/// ```text
/// Original Plan:        Distributed Plan:
/// ProjectionExec        DDStageExec(2)
/// └── SortExec          └── ProjectionExec
///     └── FilterExec        └── DDStageExec(1)
///                               └── SortExec
///                                   └── DDStageExec(0)
///                                       └── FilterExec
/// ```
///
/// # Integration
/// This rule runs as part of DataFusion's physical optimization pipeline,
/// typically after standard optimizations but before plan distribution.
#[derive(Debug)]
pub struct DDStageOptimizerRule {}

impl Default for DDStageOptimizerRule {
    /// Creates a new stage optimizer rule with default configuration
    ///
    /// This provides the standard implementation for creating a stage optimizer
    /// rule without any custom configuration. The rule operates with fixed
    /// logic for detecting stage boundaries.
    fn default() -> Self {
        Self::new()
    }
}

impl DDStageOptimizerRule {
    /// Creates a new distributed stage optimizer rule
    ///
    /// This constructor initializes the optimizer rule that will insert stage
    /// boundaries in physical execution plans. The rule is stateless and can
    /// be reused across multiple plan optimizations.
    ///
    /// # Returns
    /// * `DDStageOptimizerRule` - New optimizer rule ready for plan transformation
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for DDStageOptimizerRule {
    /// Transforms a physical execution plan by inserting distributed stage boundaries
    ///
    /// This method implements the core optimization logic that converts a single-node
    /// physical plan into a distributed execution plan. It performs a bottom-up traversal
    /// of the plan tree, inserting stage markers at operations that require network
    /// boundaries for distributed execution.
    ///
    /// # Transformation Process
    /// 1. **Initial Logging**: Displays the input plan with partition counts for debugging
    /// 2. **Tree Traversal**: Bottom-up traversal using DataFusion's transform API
    /// 3. **Boundary Detection**: Identifies operations requiring data redistribution:
    ///    - `RepartitionExec`: Hash/range partitioning for data shuffling
    ///    - `SortExec`: Global sorting requiring data collection and redistribution
    ///    - `NestedLoopJoinExec`: Cross joins requiring data broadcast to all nodes
    /// 4. **Stage Insertion**: Wraps detected operations in `DDStageExec` markers
    /// 5. **Final Wrapping**: Adds a final stage marker around the entire plan
    /// 6. **Result Logging**: Displays the transformed plan for verification
    ///
    /// # Stage Numbering
    /// Stages are numbered sequentially starting from 0, with each detected boundary
    /// operation receiving a unique stage ID. The final wrapper stage gets the highest ID.
    ///
    /// # Plan Structure Impact
    /// - **Original**: Linear execution on single node
    /// - **Transformed**: Segmented execution with network boundaries
    /// - **Result**: Enables parallel execution across distributed workers
    ///
    /// # Arguments
    /// * `plan` - Input physical execution plan to transform
    /// * `_config` - DataFusion configuration options (unused in current implementation)
    ///
    /// # Returns
    /// * `Arc<dyn ExecutionPlan>` - Transformed plan with distributed stage markers
    ///
    /// # Errors
    /// Returns DataFusion errors if plan transformation fails during tree traversal
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &datafusion::config::ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Log the input plan for debugging and analysis
        info!(
            "optimizing physical plan:\n{}",
            display_plan_with_partition_counts(&plan)
        );

        // Initialize stage counter for unique stage identification
        let mut stage_counter = 0;

        // Define transformation function for bottom-up tree traversal
        let up = |plan: Arc<dyn ExecutionPlan>| {
            // Check if current node requires a stage boundary
            if plan.as_any().downcast_ref::<RepartitionExec>().is_some()
                || plan.as_any().downcast_ref::<SortExec>().is_some()
                || plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some()
            {
                // Insert stage marker for distributed execution boundary
                let stage = Arc::new(DDStageExec::new(plan, stage_counter));
                stage_counter += 1;
                Ok(Transformed::yes(stage as Arc<dyn ExecutionPlan>))
            } else {
                // No stage boundary needed, pass through unchanged
                Ok(Transformed::no(plan))
            }
        };

        // Apply transformation bottom-up through the plan tree
        let plan = plan.clone().transform_up(up)?.data;

        // Wrap the entire transformed plan in a final stage marker
        let final_plan = Arc::new(DDStageExec::new(plan, stage_counter)) as Arc<dyn ExecutionPlan>;

        // Log the optimized plan for verification and debugging
        info!(
            "optimized physical plan:\n{}",
            display_plan_with_partition_counts(&final_plan)
        );
        Ok(final_plan)
    }

    /// Returns the name identifier for this optimizer rule
    ///
    /// This name is used by DataFusion's optimization framework for logging,
    /// debugging, and rule identification purposes. It helps track which
    /// optimizations have been applied to a query plan.
    ///
    /// # Returns
    /// * `&str` - Static string identifier for the stage optimizer rule
    fn name(&self) -> &str {
        "DDStageOptimizerRule"
    }

    /// Indicates whether this optimizer rule requires schema validation
    ///
    /// Returns true to enable DataFusion's schema validation after applying
    /// this optimization rule. This ensures that the stage boundary insertions
    /// don't violate schema contracts or introduce type inconsistencies.
    ///
    /// # Returns
    /// * `bool` - Always true to enable post-optimization schema validation
    fn schema_check(&self) -> bool {
        true
    }
}
