mod coordinator;
mod remote_work_unit_feed;
mod work_unit;
#[allow(clippy::module_inception)]
mod work_unit_feed;
mod work_unit_feed_provider;
mod work_unit_feed_registry;

pub use coordinator::collect_task_work_unit_feeds;
pub use remote_work_unit_feed::{
    RemoteWorkUnitFeedRegistry, RemoteWorkUnitFeedRxs, RemoteWorkUnitFeedTxs, WorkUnitRx,
    WorkUnitTx, set_received_time, set_sent_time,
};
#[cfg(feature = "flight")]
pub(crate) use remote_work_unit_feed::{
    build_work_unit_batch_msg, set_work_unit_received_time, set_work_unit_send_time,
};
pub(crate) use work_unit_feed_registry::{WorkUnitFeedRegistry, set_distributed_work_unit_feed};

pub use work_unit::WorkUnit;
pub use work_unit_feed::{WorkUnitFeed, WorkUnitFeedProto};
pub use work_unit_feed_provider::{DistributedWorkUnitFeedContext, WorkUnitFeedProvider};
