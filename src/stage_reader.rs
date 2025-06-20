use std::{fmt::Formatter, sync::Arc};

use arrow_flight::Ticket;
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{internal_datafusion_err, internal_err},
    error::{DataFusionError, Result},
    execution::SendableRecordBatchStream,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs,
        DisplayFormatType,
        ExecutionPlan,
        Partitioning,
        PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
    },
};
use futures::{StreamExt, stream::TryStreamExt};
use prost::Message;

use crate::{
    logging::trace,
    protobuf::FlightTicketData,
    util::{CombinedRecordBatchStream, get_client},
    vocab::{CtxName, CtxStageAddrs},
};

pub(crate) struct QueryId(pub String);

/// An [`ExecutionPlan`] that will produce a stream of batches fetched from
/// another stage which is hosted by a [`crate::stage_service::StageService`]
/// separated from a network boundary
///
/// Note that discovery of the service is handled by populating an instance of
/// [`crate::stage_service::ServiceClients`] and storing it as an extension in
/// the [`datafusion::execution::TaskContext`] configuration.
#[derive(Debug)]
pub struct DFRayStageReaderExec {
    properties: PlanProperties,
    schema: SchemaRef,
    pub stage_id: u64,
}

impl DFRayStageReaderExec {
    pub fn try_new_from_input(input: Arc<dyn ExecutionPlan>, stage_id: u64) -> Result<Self> {
        let properties = input.properties().clone();

        Self::try_new(properties.partitioning.clone(), input.schema(), stage_id)
    }

    pub fn try_new(partitioning: Partitioning, schema: SchemaRef, stage_id: u64) -> Result<Self> {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitioning.partition_count()),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Ok(Self {
            properties,
            schema,
            stage_id,
        })
    }
}
impl DisplayAs for DFRayStageReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "RayStageReaderExec[{}] (output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for DFRayStageReaderExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn name(&self) -> &str {
        "RayStageReaderExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        // TODO: handle more general case
        unimplemented!()
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let name = format!("RayStageReaderExec[{}-{}]:", self.stage_id, partition);
        trace!("{name} execute: partition {partition}");

        let stage_addrs = &context
            .session_config()
            .get_extension::<CtxStageAddrs>()
            .ok_or(internal_datafusion_err!(
                "{name} Flight Client not in context"
            ))?
            .0;

        let query_id = &context
            .session_config()
            .get_extension::<QueryId>()
            .ok_or(internal_datafusion_err!("{} QueryId not in context", name))?
            .0;

        let ctx_name = &context
            .session_config()
            .get_extension::<CtxName>()
            .unwrap_or(Arc::new(CtxName("unknown ctx".to_string())))
            .0;

        trace!(" trying to get clients for {:?}", stage_addrs);
        let clients = stage_addrs
            .get(&(self.stage_id))
            .ok_or(internal_datafusion_err!(
                "{} No flight clients found for stage {}, have stages {:?}",
                name,
                self.stage_id,
                stage_addrs.keys()
            ))
            .map(|h| {
                h.get(&(partition as u64)).ok_or(internal_datafusion_err!(
                    "{} No flight clients found for {}:{}, have partitions {:?}",
                    name,
                    self.stage_id,
                    partition,
                    h.keys()
                ))
            })??
            .iter()
            .map(|(name, addr)| get_client(name, addr))
            .collect::<Result<Vec<_>>>()?;

        trace!("got clients.  {name} num clients: {}", clients.len());

        let ftd = FlightTicketData {
            query_id: query_id.clone(),
            stage_id: self.stage_id,
            partition: partition as u64,
            requestor_name: ctx_name.clone(),
        };

        let ticket = Ticket {
            ticket: ftd.encode_to_vec().into(),
        };

        let schema = self.schema.clone();

        let num_clients = clients.len();

        let stream = async_stream::stream! {
            let mut error = false;

            let mut streams = vec![];
            for (i, mut client) in clients.into_iter().enumerate() {
                let name = name.clone();
                trace!("{name} Getting flight stream {}/{}",i+1, num_clients);
                match client.do_get(ticket.clone()).await {
                    Ok(flight_stream) => {
                        trace!("{name} Got flight stream. headers:{:?}", flight_stream.headers());
                        let rbr_stream = RecordBatchStreamAdapter::new(schema.clone(),
                            flight_stream
                                .map_err(move |e| internal_datafusion_err!("{name} Error consuming flight stream from {}: {e}", client.destination)));

                        streams.push(Box::pin(rbr_stream) as SendableRecordBatchStream);
                    },
                    Err(e) => {
                        error = true;
                        yield internal_err!("{name} Error getting flight stream from {}: {e}", client.destination);
                    }
                }
            }
            if !error {
                let mut combined = CombinedRecordBatchStream::new(schema.clone(),streams);

                while let Some(maybe_batch) = combined.next().await {
                    yield maybe_batch;
                }
            }

        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}
