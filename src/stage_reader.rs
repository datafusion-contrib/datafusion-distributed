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
    logging::{error, trace},
    protobuf::{FlightDataMetadata, FlightTicketData},
    util::{get_client, CombinedRecordBatchStream},
    vocab::{CtxAnnotatedOutputs, CtxHost, CtxStageAddrs},
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
    /// Creates a DDStageReaderExec from an existing execution plan
    ///
    /// This method copies the partitioning and schema properties from an input plan
    /// and creates a stage reader that can replace that plan in distributed execution.
    /// Used during plan transformation when converting DDStageExec to DDStageReaderExec.
    ///
    /// # Arguments
    /// * `input` - The original execution plan to be replaced by a distributed reader
    /// * `stage_id` - Unique identifier for the stage that workers will execute
    ///
    /// # Returns
    /// * `DDStageReaderExec` - A reader that will fetch results from workers executing the input plan
    pub fn try_new_from_input(input: Arc<dyn ExecutionPlan>, stage_id: u64) -> Result<Self> {
        let properties = input.properties().clone();

        Self::try_new(properties.partitioning.clone(), input.schema(), stage_id)
    }

    /// Creates a DDStageReaderExec with specified partitioning and schema
    ///
    /// This is the core constructor that creates a distributed stage reader.
    /// The reader will connect to workers executing the specified stage and
    /// fetch results according to the partitioning scheme.
    ///
    /// # Arguments
    /// * `partitioning` - How the data is partitioned across workers
    /// * `schema` - Schema of the data that will be read from workers
    /// * `stage_id` - Unique identifier for the stage to read from
    ///
    /// # Returns
    /// * `DDStageReaderExec` - A reader configured to fetch distributed results
    pub fn try_new(partitioning: Partitioning, schema: SchemaRef, stage_id: u64) -> Result<Self> {
        // Create execution plan properties for this distributed reader
        // The reader preserves the partition count but uses UnknownPartitioning
        // since we're reading from remote workers rather than local partitions
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(partitioning.partition_count()),
            EmissionType::Incremental, // Results stream incrementally
            Boundedness::Bounded,      // Query results are finite
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

    /// Executes the distributed stage reader by connecting to workers and fetching results
    ///
    /// This is the core method that implements distributed query execution.
    /// It connects to multiple worker nodes that are executing the specified stage,
    /// sends them tickets to fetch their computed results, and combines all streams
    /// into a single result stream for the client.
    ///
    /// # Arguments
    /// * `partition` - Partition number to execute (typically 0 for coalesced results)
    /// * `context` - DataFusion execution context containing worker addresses and metadata
    ///
    /// # Returns
    /// * `SendableRecordBatchStream` - Combined stream from all workers executing this stage
    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let name = format!("DDStageReaderExec[{}-{}]:", self.stage_id, partition);
        trace!("{name} execute: partition {partition}");

        // Extract worker addresses from the execution context
        // These were populated during query planning and distribution
        let stage_addrs = &context
            .session_config()
            .get_extension::<CtxStageAddrs>()
            .ok_or(internal_datafusion_err!(
                "{name} Flight Client not in context"
            ))?
            .0;

        // Get the query ID for this execution
        let query_id = &context
            .session_config()
            .get_extension::<QueryId>()
            .ok_or(internal_datafusion_err!("{} QueryId not in context", name))?
            .0;

        // Get the name of this proxy for logging/debugging
        let ctx_name = &context
            .session_config()
            .get_extension::<CtxHost>()
            .map(|ctx_host| ctx_host.0.to_string())
            .unwrap_or("unknown_context_host!".to_string());

        trace!(" trying to get clients for {:?}", stage_addrs);

        // Find all worker clients that are executing this specific stage and partition
        // The address structure is: stage_id -> partition_id -> [worker_hosts]
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
            .map(get_client)
            .collect::<Result<Vec<_>>>()?;

        trace!("got clients.  {name} num clients: {}", clients.len());

        // Create a ticket that workers will use to identify which task to execute
        // This ticket contains the query ID, stage ID, partition, and requestor info
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

        // Create an async stream that connects to all workers and combines their results
        let stream = async_stream::stream! {
            let mut error = false;

            let mut streams = vec![];
            // Connect to each worker and request their computed results
            for (i, mut client) in clients.into_iter().enumerate() {
                let name = name.clone();
                trace!("{name} - {ctx_name_capture} Getting flight stream {}/{}",i+1, num_clients);

                // Send do_get request to worker with the ticket
                // This is where the actual distributed execution happens!
                match client.do_get(ticket.clone()).await {
                    Ok(flight_stream) => {
                        trace!("{name} - {ctx_name_capture} Got flight stream. headers:{:?}", flight_stream.headers());

                        // Convert the Arrow Flight stream to RecordBatch stream
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
            // If all worker connections succeeded, combine their streams into one
            if !error {
                // CombinedRecordBatchStream merges multiple worker streams into a single stream
                // This allows the client to receive results from all workers seamlessly
                let mut combined = CombinedRecordBatchStream::new(schema.clone(),streams);

                // Stream all batches from the combined worker streams to the client
                while let Some(maybe_batch) = combined.next().await {
                    yield maybe_batch;
                }
            }

        };

        // Wrap the async stream in a RecordBatchStreamAdapter for DataFusion compatibility
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// Creates a RecordBatch stream that extracts and saves metadata from Arrow Flight messages
///
/// This function wraps an Arrow Flight stream to intercept and process trailing metadata
/// while transparently streaming data batches to the consumer. It's specifically designed
/// to handle distributed DataFusion worker responses that may include execution metadata
/// (like EXPLAIN ANALYZE results) in the final Flight message.
///
/// # Purpose
/// When workers execute stages, they may send execution analysis data or other metadata
/// as trailing information in the Flight stream. This function extracts that metadata
/// and stores it in the session context for later aggregation, while ensuring the
/// normal data flow continues uninterrupted.
///
/// # Arguments
/// * `name` - Identifier for logging and error messages (usually worker name)
/// * `config` - Session configuration containing context extensions for metadata storage
/// * `schema` - Schema of the RecordBatch data being streamed
/// * `stream` - Input Arrow Flight stream from a distributed worker
///
/// # Returns
/// * `SendableRecordBatchStream` - Stream that yields RecordBatches and captures metadata
///
/// # Metadata Processing
/// 1. **Normal Data**: Regular Flight messages are converted to RecordBatches and yielded
/// 2. **Trailing Metadata**: Messages with app_metadata are decoded and stored in context
/// 3. **Error Handling**: Decode failures are converted to stream errors
/// 4. **Stream Completion**: Metadata extraction marks the end of the stream
///
/// # Usage
/// Used by DDStageReaderExec when reading results from distributed workers to capture
/// execution analysis data for EXPLAIN ANALYZE queries.
fn make_flight_metadata_saver_stream(
    name: String,
    config: &SessionConfig,
    schema: SchemaRef,
    stream: FlightRecordBatchStream,
) -> SendableRecordBatchStream {
    // Extract the inner Flight data stream for processing
    let mut decoder = stream.into_inner();

    // Get the context extension for storing extracted metadata
    // This is where EXPLAIN ANALYZE results and other execution metadata get accumulated
    let task_outputs = config
        .get_extension::<CtxAnnotatedOutputs>()
        .unwrap_or(Arc::new(CtxAnnotatedOutputs::default()))
        .0
        .clone();

    let name_capture = name.clone();

    // Create an async stream that processes Flight messages and handles metadata
    #[allow(unused_assignments)] // clippy can't understand our assignment to done in the macro
    let new_stream = async_stream::stream! {
        let mut done = false;
        while !done {
            match (done, decoder.next().await) {
                // Received a Flight message while stream is active
                (false, Some(Ok(flight_data))) => {
                    let app_metadata_bytes = flight_data.app_metadata();

                    if !app_metadata_bytes.is_empty() {
                        // This Flight message contains metadata (usually from the last message)
                        trace!("{name} Received trailing metadata from flight stream");

                        // Decode the protobuf metadata containing execution analysis results
                        match FlightDataMetadata::decode(&app_metadata_bytes[..])
                            .map(|fdm| fdm.annotated_task_outputs.unwrap_or_default()) {
                            Ok(outputs) => {
                                trace!("{name} Decoded flight data metadata annotated outputs: {:?}", outputs);
                                // Store the metadata in the session context for later aggregation
                                task_outputs.lock().extend(outputs.outputs);
                            }
                            Err(e) => {
                                // Metadata decode failure - yield as stream error
                                yield Err(FlightError::DecodeError(format!(
                                    "{name} Failed to decode flight data metadata: {e:#?}"
                                )));
                            }
                        }

                        // Metadata messages don't contain RecordBatch data, so we're done
                        // We don't yield this message since it has no data records
                        done = true;
                    } else {
                        // Regular data message - convert to RecordBatch and yield to consumer
                        trace!("received normal data");
                        yield Ok(flight_data.inner);
                    }
                },
                // Flight stream error - propagate to consumer
                (false, Some(Err(e))) => {
                    yield Err(e);
                }
                // Stream ended without metadata message
                (false, None) => {
                    done = true;
                }
                // Already processed metadata, ignore any additional messages
                (true,_) => {
                    error!("{name} Unreachable block. flight stream already done");
                }
            }
        }
        trace!("Done with flight stream {name} - no more data to yield");
    };

    // Convert the Flight data stream back to a RecordBatch stream
    let trailing_stream =
        FlightRecordBatchStream::new_from_flight_data(new_stream).map_err(move |e| {
            internal_datafusion_err!(
                "{name_capture} Failed to consume from our flight data stream: {e:#?}"
            )
        });

    // Wrap in RecordBatchStreamAdapter to provide the expected interface
    Box::pin(RecordBatchStreamAdapter::new(schema, trailing_stream))
}
