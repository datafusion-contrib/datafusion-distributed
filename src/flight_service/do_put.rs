use crate::flight_service::service::ArrowFlightEndpoint;
use crate::stage_delegation::StageContext;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::FlightData;
use futures::StreamExt;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoPut {
    #[prost(oneof = "DoPutInner", tags = "1")]
    pub inner: Option<DoPutInner>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum DoPutInner {
    #[prost(message, tag = "1")]
    StageContext(StageContextExt),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageContextExt {
    #[prost(message, optional, tag = "1")]
    pub stage_context: Option<StageContext>,
    #[prost(string, tag = "2")]
    pub stage_id: String,
    #[prost(uint64, tag = "3")]
    pub actor_idx: u64,
}

impl DoPut {
    pub fn new_stage_context_flight_data(
        stage_id: String,
        actor_idx: usize,
        next_stage_context: StageContext,
    ) -> FlightData {
        let this = Self {
            inner: Some(DoPutInner::StageContext(StageContextExt {
                stage_id,
                actor_idx: actor_idx as u64,
                stage_context: Some(next_stage_context),
            })),
        };

        FlightData::new().with_data_body(this.encode_to_vec())
    }
}

impl ArrowFlightEndpoint {
    pub(super) async fn put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<ArrowFlightEndpoint as FlightService>::DoPutStream>, Status> {
        let mut stream = request.into_inner();
        while let Some(msg) = stream.message().await? {
            let action = DoPut::decode(msg.data_body).map_err(|err| {
                Status::invalid_argument(format!("Cannot decode DoPut message: {err}"))
            })?;
            let Some(action) = action.inner else {
                return Err(Status::invalid_argument("DoPut message is empty"));
            };
            match action {
                DoPutInner::StageContext(stage_context_ext) => {
                    let Some(stage_context) = stage_context_ext.stage_context else {
                        return Err(Status::invalid_argument("StageContext is empty"));
                    };
                    let stage_id = stage_context_ext.stage_id;
                    let actor_idx = stage_context_ext.actor_idx as usize;
                    self.stage_delegation
                        .add_delegate_info(stage_id, actor_idx, stage_context)
                        .map_err(|err| {
                            Status::internal(format!(
                                "Cannot add delegate to stage_delegation: {err}"
                            ))
                        })?;
                }
            }
        }
        Ok(Response::new(futures::stream::empty().boxed()))
    }
}
