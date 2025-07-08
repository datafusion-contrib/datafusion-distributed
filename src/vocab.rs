use std::{collections::HashMap, fmt::Display, sync::Arc};

use parking_lot::Mutex;

pub use crate::protobuf::AnnotatedTaskOutput;
pub use crate::protobuf::Host;
pub use crate::protobuf::Hosts;
pub use crate::protobuf::PartitionAddrs;
pub use crate::protobuf::StageAddrs;
pub use crate::protobuf::StageData;

/// a map of stage_id, partition to a list (name,endpoint address) that can
/// serve this (stage_id, and partition).   It is assumed that to consume a
/// partition, the consumer will consume the partition from all endpoinst and
/// merge the results.
///
/// This is on a per query basis
pub type Addrs = HashMap<u64, HashMap<u64, Vec<Host>>>;

/// used to hold an Addrs as an extenstion for datafusion SessionContext
pub(crate) struct CtxStageAddrs(pub Addrs);

/// used to hold a worker name as an extension for datafusion SessionContext
pub(crate) struct CtxName(pub String);

/// used to hold a partition group as an extension for datafusion SessionContext
pub(crate) struct CtxPartitionGroup(pub Vec<u64>);

#[derive(Default)]
pub(crate) struct CtxAnnotatedOutputs(pub Arc<Mutex<Vec<AnnotatedTaskOutput>>>);

impl Display for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.addr)
    }
}
