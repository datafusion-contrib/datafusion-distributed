use datafusion::physical_plan::ExecutionPlan;

use crate::task::WorkerAddr;
use std::{fmt::Debug, sync::Arc};

pub trait WorkerDiscovery: Send + Sync + Debug {
    fn get_worker_addresses(&self) -> Vec<WorkerAddr>;
}

pub trait WorkerAssignment: Send + Sync + Debug {
    /// allows custom logic to allow or reject a plan being assigned to a worker.
    fn try_assign(
        &self,
        worker_address: &WorkerAddr,
        plan: Arc<dyn ExecutionPlan>,
        partition_group: Vec<usize>,
    ) -> bool;
}

#[derive(Debug, Default)]
pub struct DefaultWorkerAssignment {}

impl WorkerAssignment for DefaultWorkerAssignment {
    /// Accepts any assignment of a plan to a worker.
    fn try_assign(
        &self,
        _worker_address: &WorkerAddr,
        _plan: Arc<dyn ExecutionPlan>,
        _partition_group: Vec<usize>,
    ) -> bool {
        true
    }
}
