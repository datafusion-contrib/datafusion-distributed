use datafusion::common::DataFusionError;
use std::error::Error;
use std::fmt::{Display, Formatter};

/// Error thrown during distributed planning that prompts the planner to change something and
/// try again.
#[derive(Debug)]
pub enum DistributedPlanError {
    /// Prompts the planner to limit the amount of tasks used in the stage that is currently
    /// being planned.
    LimitTasks(usize),
    /// Signals the planner that this whole plan is non-distributable. This can happen if
    /// certain nodes are present, like `StreamingTableExec`, which are typically used in
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

pub(crate) fn get_distribute_plan_err(err: &DataFusionError) -> Option<&DistributedPlanError> {
    let DataFusionError::External(err) = err else {
        return None;
    };
    err.downcast_ref()
}
