use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;

/// A unit of isolation for a portion of a physical execution plan
/// that can be executed independently.
///
/// see https://howqueryengineswork.com/13-distributed-query.html
///
#[derive(Debug, Clone)]
pub struct ExecutionStage {
    /// Our stage number
    pub num: usize,
    /// The physical execution plan that this stage will execute.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The input stages to this stage
    pub inputs: Vec<Arc<ExecutionStage>>,
}

impl ExecutionStage {
    /// Creates a new `ExecutionStage` with the given plan and inputs.
    pub fn new(num: usize, plan: Arc<dyn ExecutionPlan>, inputs: Vec<Arc<ExecutionStage>>) -> Self {
        println!(
            "Creating ExecutionStage: {}, with inputs {}",
            num,
            inputs
                .iter()
                .map(|s| format!("{}", s.num))
                .collect::<Vec<_>>()
                .join(", ")
        );
        ExecutionStage { num, plan, inputs }
    }

    /// Returns the name of this stage
    pub fn name(&self) -> String {
        format!("Stage {:<3}", self.num)
    }

    /// Returns the name of this stage including child stage numbers if any.
    pub fn name_with_children(&self) -> String {
        let child_str = if self.inputs.is_empty() {
            "".to_string()
        } else {
            format!(
                " Child Stages:[{}] ",
                self.inputs
                    .iter()
                    .map(|s| format!("{}", s.num))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        format!("Stage {:<3}{}", self.num, child_str)
    }
}
