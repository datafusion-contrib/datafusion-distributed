use std::sync::Arc;

use datafusion::{
    catalog::memory::DataSourceExec,
    common::{
        internal_datafusion_err, internal_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor},
        DataFusionError,
    },
    config::ConfigOptions,
    datasource::physical_plan::{FileScanConfig, FileSource},
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        displayable, execution_plan::need_data_exchange, ExecutionPlan, ExecutionPlanProperties,
    },
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::ArrowFlightReadExec;

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
        if need_data_exchange(plan.clone()) {
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

            let mut stage = ExecutionStage::new(
                self.stage_counter,
                plan.clone(),
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
            let name = format!("Stage {:<3}", self.stage_counter);
            let read = Arc::new(ArrowFlightReadExec::new(plan.properties(), &name));

            self.stage_counter += 1;

            Ok(Transformed::yes(read as Self::Node))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}
