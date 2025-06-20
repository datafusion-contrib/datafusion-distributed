use std::collections::HashMap;

/// a map of stage_id, partition to a list (name,endpoint address) that can
/// serve this (stage_id, and partition).   It is assumed that to consume a
/// partition, the consumer will consume the partition from all endpoinst and
/// merge the results.
///
/// This is on a per query basis
pub type Addrs = HashMap<u64, HashMap<u64, Vec<(String, String)>>>;

/// used to hold an Addrs as an extenstion for datafusion SessionContext
pub(crate) struct CtxStageAddrs(pub Addrs);

/// used to hold a worker name as an extension for datafusion SessionContext
pub(crate) struct CtxName(pub String);

/// used to hold a partition group as an extension for datafusion SessionContext
pub struct CtxPartitionGroup(pub Vec<u64>);
