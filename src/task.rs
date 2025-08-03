use core::fmt;
use std::collections::{self, HashMap};
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::{execution::SendableRecordBatchStream, physical_plan::ExecutionPlan};
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use prost::Message;

use datafusion::error::Result;
use datafusion_proto::protobuf::PhysicalPlanNode;

use crate::remote::WorkerAssignment;

/// An ExecutionTask is a finer grained unit of work compared to an ExecutionStage.
/// One ExecutionStage will create one or more ExecutionTasks
///
/// When a task is execute()'d if will execute its plan and return a stream of record batches.
///
/// If the task has input tasks, then it those input tasks will be executed on remote resources
/// and will be provided the remainder of the task tree.
///
/// For example if our task tree looks like this:
///
/// ```text
///                       ┌────────┐
///                       │ Task 1 │
///                       └───┬────┘
///                           │
///                    ┌──────┴───────┐
///               ┌────┴───┐     ┌────┴───┐
///               │ Task 2 │     │ Task 3 │
///               └────┬───┘     └────────┘
///                    │
///             ┌──────┴───────┐
///        ┌────┴───┐     ┌────┴───┐
///        │ Task 4 │     │ Task 5 │
///        └────────┘     └────────┘                    
///
/// ```
///  
/// Then executing Task 1 will run its plan locally.  Task 1 has two inputs, Task 2 and Task 3.  We
/// know these will execute on remote resources.   As such the plan for Task 1 must contain an
/// [`ArrowFlightReadExec`] node that will read the results of Task 2 and Task 3 and coalese the
/// results.
///
/// When Task 1's [`ArrowFlightReadExec`] node is executed, it makes an ArrowFlightRequest to the
/// host assigned in the Task.  It provides the following Task tree serialilzed in the body of the
/// Arrow Flight Ticket:
///
/// ```text
///               ┌────────┐     
///               │ Task 2 │    
///               └────┬───┘   
///                    │
///             ┌──────┴───────┐
///        ┌────┴───┐     ┌────┴───┐
///        │ Task 4 │     │ Task 5 │
///        └────────┘     └────────┘                    
///
/// ```
///
/// The receiving ArrowFlightEndpoint will then execute Task 2 and will repeated this process.
///
/// When Task 4 is executed, it has no input tasks, so it is assumed that the plan included in that
/// Task can complete on its own; its likely holding a leaf node in the overall phyysical plan and
/// producing data from a [`DataSourceExec`].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionTask {
    /// The address of the worker that will execute this task.  A None value is interpreted as
    /// unassinged.
    #[prost(message, optional, tag = "1")]
    pub worker_addr: Option<WorkerAddr>,
    /// The partitions that we can execute from this plan
    #[prost(uint64, repeated, tag = "2")]
    pub partition_group: Vec<u64>,
}

/// The host and port of a worker that will execute the task.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct WorkerAddr {
    /// The host name or IP address of the worker.
    #[prost(string, tag = "1")]
    pub host: String,
    /// The port number of the worker, a u32 vs u16 as prost doesn't like u16
    #[prost(uint32, tag = "2")]
    pub port: u32,
}

impl Display for ExecutionTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Task: partitions: {}\n{}]",
            format_pg(&self.partition_group),
            self.worker_addr
                .as_ref()
                .map(|addr| format!("worker: {}:{}", addr.host, addr.port))
                .unwrap_or("worker: unassigned".to_string())
        )
    }
}

fn format_pg(partition_group: &[u64]) -> String {
    if partition_group.len() > 2 {
        format!(
            "{}..{}",
            partition_group[0],
            partition_group[partition_group.len() - 1]
        )
    } else {
        partition_group
            .iter()
            .map(|pg| format!("{pg}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}
