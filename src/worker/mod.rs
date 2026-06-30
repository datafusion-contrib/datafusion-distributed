#[cfg(feature = "flight")]
mod flight;
pub mod generated;
#[cfg(feature = "flight")]
mod impl_coordinator_channel;
mod impl_execute_task;
mod impl_set_plan;
mod in_memory;
mod session_builder;
mod single_write_multi_read;
#[cfg(feature = "flight")]
mod spawn_select_all;
mod task_data;
#[cfg(any(test, feature = "integration"))]
pub(crate) mod test_utils;
mod transport;
mod worker_connection_pool;
mod worker_service;

// Surface an out-of-crate transport executes fragments in-process through.
pub use impl_execute_task::{collect_plan_metrics_protos, execute_local_task};
pub use single_write_multi_read::SingleWriteMultiRead;
#[cfg(feature = "flight")]
pub(crate) use worker_connection_pool::LocalWorkerContext;
pub(crate) use worker_connection_pool::WorkerConnectionPool;

#[cfg(feature = "flight")]
pub use flight::FlightWorkerTransport;
pub use in_memory::InMemoryWorkerTransport;
pub use transport::{WorkerConnection, WorkerDispatch, WorkerDispatchRequest, WorkerTransport};

pub use session_builder::{
    DefaultSessionBuilder, MappedWorkerSessionBuilder, MappedWorkerSessionBuilderExt,
    WorkerQueryContext, WorkerSessionBuilder,
};
pub use task_data::TaskData;
pub use worker_service::{ResultTaskData, TaskDataEntries, Worker};
