use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::distributed_codec::StageKey;
use std::time::{SystemTime, UNIX_EPOCH};

/// A collection of metrics for a set of tasks in an ExecutionPlan. each
/// entry should have a distinct [StageKey].
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MetricsCollection {
    #[prost(message, repeated, tag = "1")]
    pub tasks: Vec<TaskMetrics>,
}

/// TaskMetrics represents the metrics for a single task.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TaskMetrics {
    /// stage_key uniquely identifies this task.
    ///
    /// This field is always present. It's marked optional due to protobuf rules.
    #[prost(message, optional, tag = "1")]
    pub stage_key: Option<StageKey>,
    /// metrics[i] is the set of metrics for plan node `i` where plan nodes are in pre-order
    /// traversal order.
    #[prost(message, repeated, tag = "2")]
    pub metrics: Vec<MetricsSetProto>,
}

// FlightAppMetadata represents all types of app_metadata which we use in the distributed execution.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FlightAppMetadata {
    #[prost(uint64, tag = "1")]
    pub partition: u64,
    // Unix timestamp in nanoseconds at which this message was created.
    #[prost(uint64, tag = "2")]
    pub created_timestamp_unix_nanos: u64,
    // content should always be Some, but it is optional due to protobuf rules.
    #[prost(oneof = "AppMetadata", tags = "10")]
    pub content: Option<AppMetadata>,
}

impl FlightAppMetadata {
    pub fn new(partition: u64) -> Self {
        Self {
            partition,
            created_timestamp_unix_nanos: current_unix_timestamp_nanos(),
            content: None,
        }
    }

    pub fn set_content(&mut self, content: AppMetadata) {
        self.content = Some(content);
    }
}

fn current_unix_timestamp_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

#[derive(Clone, PartialEq, ::prost::Oneof)]
pub enum AppMetadata {
    #[prost(message, tag = "10")]
    MetricsCollection(MetricsCollection),
    // Note: For every additional enum variant, ensure to add tags to [FlightAppMetadata]. ex. `#[prost(oneof = "AppMetadata", tags = "1,2,3")]` etc.
    // If you don't the proto will compile but you may encounter errors during serialization/deserialization.
}
