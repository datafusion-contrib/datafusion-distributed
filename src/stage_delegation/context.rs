use datafusion_proto::protobuf;

/// Contains the necessary context for actors in a stage to perform a distributed query.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageContext {
    /// Unique identifier of the stage
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(uint64, tag = "2")]
    pub delegate: u64,
    #[prost(string, repeated, tag = "3")]
    pub actors: Vec<String>,
    #[prost(message, optional, tag = "4")]
    pub partitioning: Option<protobuf::Partitioning>,
    #[prost(uint64, tag = "5")]
    pub prev_actors: u64,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActorContext {
    #[prost(uint64, tag = "1")]
    pub caller_actor_idx: u64,
    #[prost(uint64, tag = "2")]
    pub actor_idx: u64,
}
