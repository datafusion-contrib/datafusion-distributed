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
    physical_plan::{
        displayable, execution_plan::need_data_exchange, ExecutionPlan, ExecutionPlanProperties,
    },
};

use crate::ArrowFlightReadExec;

use super::stage::ExecutionStage;

/// StagePlanner is a TreeNodeRewriter that walks the plan tree and creates
/// a tree of ExecutionStage nodes that represent discrete stages of execution
/// can are separated by a data shuffle.
///
/// See https://howqueryengineswork.com/13-distributed-query.html for more information
/// about distributed execution.
pub struct StagePlanner {
    /// used to keep track of the current plan head
    plan_head: Option<Arc<dyn ExecutionPlan>>,
    /// Current depth in the plan tree, as we walk the tree
    depth: usize,
    /// Input stages collected so far. Each entry is a tuple of (tree depth, stage).
    /// This allows us to keep track of the depth in the plan tree
    /// where we created the stage.   That way when we create a new
    /// stage, we can tell if it is a peer to the current input stages or
    /// should be a parent (if its depth is a smaller number)
    input_stages: Vec<(usize, Arc<ExecutionStage>)>,
    /// current stage number
    stage_counter: usize,
}

/// Create an ExecutionStage from a plan.   
///
/// The resulting ExecutionStage cannot be executed directly, but is used as input,
/// together with a vec of Worker Addresses, to further break up the stages into [`ExecutionTask`]s
/// Which are ultimately what facilitates distributed execution.
impl TryFrom<Arc<dyn ExecutionPlan>> for ExecutionStage {
    type Error = DataFusionError;
    fn try_from(plan: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let mut planner = StagePlanner {
            plan_head: Some(plan.clone()),
            depth: 0,
            input_stages: vec![],
            stage_counter: 1,
        };

        plan.rewrite(&mut planner)?;
        planner.finish().and_then(|arc_stage| {
            Arc::into_inner(arc_stage).ok_or(internal_datafusion_err!(
                "Failed to convert Arc<ExecutionStage> to ExecutionStage"
            ))
        })
    }
}

impl StagePlanner {
    pub fn new() -> Self {
        StagePlanner {
            plan_head: None,
            depth: 0,
            input_stages: vec![],
            stage_counter: 0,
        }
    }

    pub fn finish(mut self) -> Result<Arc<ExecutionStage>> {
        if self.input_stages.is_empty() {
            return internal_err!("No input stages found, did you forget to call rewrite()?");
        }

        if self.depth < self.input_stages[0].0 {
            Ok(Arc::new(ExecutionStage::new(
                self.stage_counter,
                self.plan_head
                    .take()
                    .ok_or_else(|| internal_datafusion_err!("No plan head set"))?,
                self.input_stages
                    .iter()
                    .map(|(_, stage)| stage.clone())
                    .collect::<Vec<_>>(),
            )))
        } else {
            Ok(self.input_stages.remove(0).1)
        }
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

            println!(
                "\n\n\n ---------------------- \ncreating a stage, depth = {}, input_stages ={}, child_stages = {}, plan=\n{}",
                self.depth,
                self.input_stages
                    .iter()
                    .map(|(depth, stage)| format!("({},{})", depth, stage.num))
                    .collect::<Vec<_>>()
                    .join(", "),
                child_stages
                    .iter()
                    .map(|stage| format!("{}", stage.num))
                    .collect::<Vec<_>>()
                    .join(", "),

                displayable(plan.as_ref()).indent(false)
            );

            self.input_stages.retain(|(depth, _)| *depth <= self.depth);

            let stage = Arc::new(ExecutionStage::new(
                self.stage_counter,
                plan.clone(),
                child_stages,
            ));

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
