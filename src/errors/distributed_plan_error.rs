use std::error::Error;
use std::fmt::{Display, Formatter};

use datafusion::error::DataFusionError;

/// Error thrown during distributed planning that prompts the planner to change something and
/// try again.
#[derive(Debug)]
pub enum DistributedPlanError {
    /// Prompts the planner to limit the amount of tasks used in the stage that is currently
    /// being planned.
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

/// Builds a [DistributedPlanError::LimitTasks] error. This error prompts the distributed planner
/// to try rebuilding the current stage with a limited amount of tasks.
pub fn limit_tasks_err(limit: usize) -> DataFusionError {
    DataFusionError::External(Box::new(DistributedPlanError::LimitTasks(limit)))
}

pub fn get_distribute_plan_err(err: &DataFusionError) -> Option<&DistributedPlanError> {
    let DataFusionError::External(err) = err else {
        return None;
    };
    err.downcast_ref()
}
