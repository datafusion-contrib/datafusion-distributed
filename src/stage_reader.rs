use std::{fmt::Formatter, sync::Arc};

use arrow_flight::{decode::FlightRecordBatchStream, error::FlightError, Ticket};
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{internal_datafusion_err, internal_err},
    error::{DataFusionError, Result},
    execution::SendableRecordBatchStream,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
    prelude::SessionConfig,
};
use futures::{stream::TryStreamExt, StreamExt};
use prost::Message;

use crate::{
    logging::{error, trace}, protobuf::{FlightDataMetadata, FlightTicketData}, transport, util::CombinedRecordBatchStream, vocab::{CtxAnnotatedOutputs, CtxHost, CtxStageAddrs}
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
pub struct DDStageReaderExec {
    properties: PlanProperties,
    schema: SchemaRef,
    pub stage_id: u64,
}

impl DDStageReaderExec {
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
impl DisplayAs for DDStageReaderExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "DDStageReaderExec[{}] (output_partitioning={:?})",
            self.stage_id,
            self.properties().partitioning
        )
    }
}

impl ExecutionPlan for DDStageReaderExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn name(&self) -> &str {
        "DDStageReaderExec"
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
        let name = format!("DDStageReaderExec[{}-{}]:", self.stage_id, partition);
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
            .get_extension::<CtxHost>()
            .map(|ctx_host| ctx_host.0.to_string())
            .unwrap_or("unknown_context_host!".to_string());

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
            .map(transport::get)
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
        let ctx_name_capture = ctx_name.clone();

        let stream = async_stream::stream! {
            let mut error = false;

            let mut streams = vec![];
            for (i, mut client) in clients.into_iter().enumerate() {
                let name = name.clone();
                trace!("{name} - {ctx_name_capture} Getting flight stream {}/{}",i+1, num_clients);
                match client.do_get(ticket.clone()).await {
                    Ok(flight_stream) => {
                        trace!("{name} - {ctx_name_capture} Got flight stream. headers:{:?}", flight_stream.headers());
                        let rbr_stream = make_flight_metadata_saver_stream(
                            ctx_name_capture.clone(),
                            context.session_config(),
                            schema.clone(),
                            flight_stream,
                        );

                        streams.push(rbr_stream);
                    },
                    Err(e) => {
                        error = true;
                        yield internal_err!("{name} - {ctx_name_capture} Error getting flight stream from {}: {e}", client.host);
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

fn make_flight_metadata_saver_stream(
    name: String,
    config: &SessionConfig,
    schema: SchemaRef,
    stream: FlightRecordBatchStream,
) -> SendableRecordBatchStream {
    let mut decoder = stream.into_inner();

    let task_outputs = config
        .get_extension::<CtxAnnotatedOutputs>()
        .unwrap_or(Arc::new(CtxAnnotatedOutputs::default()))
        .0
        .clone();

    let name_capture = name.clone();

    #[allow(unused_assignments)] // clippy can't understand our assignment to done in the macro
    let new_stream = async_stream::stream! {
        let mut done = false;
        while !done {
            match (done, decoder.next().await) {
                (false, Some(Ok(flight_data))) => {
                    let app_metadata_bytes = flight_data.app_metadata();

                    if !app_metadata_bytes.is_empty() {
                        trace!("{name} Received trailing metadata from flight stream");
                        // decode the metadata
                        match FlightDataMetadata::decode(&app_metadata_bytes[..])
                            .map(|fdm| fdm.annotated_task_outputs.unwrap_or_default()) {
                            Ok(outputs) => {
                                trace!("{name} Decoded flight data metadata annotated outputs: {:?}", outputs);
                                task_outputs.lock().extend(outputs.outputs);
                            }
                            Err(e) => {
                                yield Err(FlightError::DecodeError(format!(
                                    "{name} Failed to decode flight data metadata: {e:#?}"
                                )));
                            }
                        }

                        // ok we consumed the last flight message and extracted the trailing
                        // metadata, we don't want to yield this payload as there are no records in
                        // it
                        done = true;
                    } else {
                        // just normal data, yield to consumer
                        trace!("received normal data");
                        yield Ok(flight_data.inner);
                    }
                },
                (false, Some(Err(e))) => {
                    // propagate this error
                    yield Err(e);
                }
                (false, None) => {
                    done = true;
                }
                (true,_) => {
                    // already done, ignore any further data
                    error!("{name} Unreachable block. flight stream already done");
                }
            }
        }
        trace!("Done with flight stream {name} - no more data to yield");
    };

    let trailing_stream =
        FlightRecordBatchStream::new_from_flight_data(new_stream).map_err(move |e| {
            internal_datafusion_err!(
                "{name_capture} Failed to consume from our flight data stream: {e:#?}"
            )
        });

    Box::pin(RecordBatchStreamAdapter::new(schema, trailing_stream))
}
