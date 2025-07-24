// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::Schema;
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightServiceServer,
    sql::{ProstMessageExt, TicketStatementQuery},
    FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, ExecutionPlan, Partitioning,
};
use datafusion_substrait::substrait;
use futures::TryStreamExt;
use parking_lot::Mutex;
use prost::Message;
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Receiver, Sender},
};
use tonic::{async_trait, transport::Server, Request, Response, Status};

use crate::{
    customizer::Customizer,
    flight::{FlightSqlHandler, FlightSqlServ},
    logging::{debug, info, trace},
    planning::{add_ctx_extentions, get_ctx},
    protobuf::TicketStatementData,
    query_planner::{QueryPlan, QueryPlanner},
    result::Result,
    stage_reader::DDStageReaderExec,
    util::{display_plan_with_partition_counts, get_addrs, start_up},
    vocab::{Addrs, Host},
    worker_discovery::get_worker_addresses,
};

/// Core handler for distributed DataFusion proxy operations
///
/// The DDProxyHandler is the main component responsible for processing distributed
/// queries on the proxy node. It coordinates between clients, query planning, worker
/// distribution, and result retrieval. This handler implements the Arrow Flight SQL
/// protocol to provide a SQL interface for distributed DataFusion queries.
///
/// # Architecture
/// - Receives SQL queries and Substrait plans from clients
/// - Converts queries to distributed execution plans with stages
/// - Distributes execution tasks to available worker nodes
/// - Creates reader plans that fetch results from workers
/// - Streams combined results back to clients
///
/// # Query Processing Flow
/// 1. Client → get_flight_info_statement() → SQL parsing and planning
/// 2. Proxy → distribute tasks to workers → worker storage
/// 3. Client → do_get_statement() → create distributed readers
/// 4. Readers → connect to workers → fetch and combine results
pub struct DDProxyHandler {
    /// Host information for this proxy (name and address) used for logging and worker communication
    pub host: Host,

    /// Query planner responsible for SQL parsing and distributed plan generation
    pub planner: QueryPlanner,

    /// Optional customization for DataFusion context, object stores, UDFs, and serialization
    pub customizer: Option<Arc<dyn Customizer>>,
}

impl DDProxyHandler {
    /// Creates a new proxy handler with worker discovery bootstrap
    ///
    /// This constructor initializes the proxy handler and immediately performs worker
    /// discovery to ensure the proxy knows about available workers. This allows the
    /// proxy to start accepting queries and distributing work immediately.
    ///
    /// # Arguments
    /// * `name` - Human-readable name for this proxy instance (for logging/debugging)
    /// * `addr` - Network address where this proxy can be reached
    /// * `customizer` - Optional customization for DataFusion contexts and serialization
    ///
    /// # Returns
    /// * `DDProxyHandler` - Ready-to-use proxy handler instance
    ///
    /// # Panics
    /// * Panics if worker discovery fails during initialization (no workers available)
    pub fn new(name: String, addr: String, customizer: Option<Arc<dyn Customizer>>) -> Self {
        // Bootstrap worker discovery to find available workers in the cluster
        // This ensures the proxy can immediately start distributing queries
        get_worker_addresses().expect("Could not get worker addresses upon startup");

        let host = Host {
            name: name.clone(),
            addr: addr.clone(),
        };
        Self {
            host: host.clone(),
            planner: QueryPlanner::new(customizer.clone()),
            customizer,
        }
    }

    /// Creates a Flight Info response containing a ticket for the client to fetch query results
    ///
    /// This function takes a completed QueryPlan (with distributed tasks already sent to workers)
    /// and packages the metadata needed for result retrieval into an Arrow Flight ticket.
    /// The ticket contains the query ID, final stage information, worker addresses, and schema.
    ///
    /// # Arguments
    /// * `query_plan` - The prepared and distributed query plan containing worker assignments
    ///
    /// # Returns
    /// * `FlightInfo` - Arrow Flight metadata with embedded ticket for data retrieval
    /// * The client uses this ticket in subsequent `do_get` calls to fetch actual results
    pub fn create_flight_info_response(
        &self,
        query_plan: QueryPlan,
    ) -> Result<FlightInfo, Box<Status>> {
        // Create flight info structure with the query result schema
        let mut flight_info = FlightInfo::new()
            .try_with_schema(&query_plan.schema)
            .map_err(|e| {
                Box::new(Status::internal(format!(
                    "Could not create flight info {e:?}"
                )))
            })?;

        // Create ticket data containing all metadata needed to fetch results
        // This includes query ID, final stage info, worker addresses, and schema
        let ticket_data = TicketStatementData {
            query_id: query_plan.query_id,
            stage_id: query_plan.final_stage_id,
            stage_addrs: Some(query_plan.worker_addresses.into()),
            schema: Some(query_plan.schema.try_into().map_err(|e| {
                Box::new(Status::internal(format!("Could not convert schema {e:?}")))
            })?),
        };

        // Encode the ticket data and wrap it in Arrow Flight ticket format
        let ticket = Ticket::new(
            TicketStatementQuery {
                statement_handle: ticket_data.encode_to_vec().into(),
            }
            .as_any()
            .encode_to_vec(),
        );

        // Create flight endpoint with the ticket - this tells client how to fetch data
        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        flight_info = flight_info.with_endpoint(endpoint);

        Ok(flight_info)
    }

    /// Executes a distributed execution plan and returns streaming results
    ///
    /// This function takes a DDStageReaderExec plan (which connects to workers) and
    /// executes it to produce a stream of Arrow data back to the client.
    /// It handles the context setup, plan execution, and result streaming.
    ///
    /// # Arguments
    /// * `plan` - Distributed execution plan (typically DDStageReaderExec wrapped in CoalescePartitionsExec)
    /// * `query_id` - Unique identifier for this query
    /// * `stage_id` - ID of the stage being executed
    /// * `addrs` - Worker addresses for connecting to distributed workers
    ///
    /// # Returns
    /// * `DoGetStream` - Arrow Flight stream containing query results
    pub async fn execute_plan_and_build_stream(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        query_id: String,
        stage_id: u64,
        addrs: Addrs,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        // Create DataFusion execution context with default settings
        let mut ctx =
            get_ctx().map_err(|e| Status::internal(format!("Could not create context {e:?}")))?;

        // Add distributed execution context extensions (worker addresses, query metadata)
        // This allows DDStageReaderExec to find and connect to the correct workers
        add_ctx_extentions(&mut ctx, &self.host, &query_id, stage_id, addrs, vec![])
            .map_err(|e| Status::internal(format!("Could not add context extensions {e:?}")))?;

        // Apply any custom configuration (e.g., custom object stores, UDFs)
        if let Some(ref c) = self.customizer {
            c.customize(&mut ctx)
                .await
                .map_err(|e| Status::internal(format!("Could not customize context {e:?}")))?;
        }

        // TODO: revisit this to allow for consuming a partitular partition
        trace!("calling execute plan");

        // Execute the distributed plan (DDStageReaderExec will connect to workers)
        // Currently hardcoded to partition 0 since CoalescePartitionsExec combines all partitions
        let partition = 0;
        let stream = plan
            .execute(partition, ctx.task_ctx())
            .map_err(|e| {
                Status::internal(format!(
                    "Error executing plan for query_id {} partition {}: {e:?}",
                    query_id, partition
                ))
            })?
            .map_err(|e| FlightError::ExternalError(Box::new(e)));

        // Convert the RecordBatch stream to Arrow Flight format for network transmission
        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| Status::internal(format!("Unexpected error building stream {e:?}")));

        Ok(Response::new(Box::pin(out_stream)))
    }

    /// Validates that the address map contains exactly one stage with the expected ID
    ///
    /// This function ensures that the client's ticket refers to a valid final stage.
    /// In the result retrieval phase, we should only have one stage (the final result stage)
    /// since all intermediate stages have already been executed by workers.
    ///
    /// # Arguments
    /// * `addrs` - Map of stage IDs to their worker addresses
    /// * `expected_stage_id` - The stage ID from the client's ticket
    ///
    /// # Returns
    /// * `Ok(())` if validation passes
    /// * `Err(Status)` if multiple stages found or stage ID missing
    pub fn validate_single_stage_addrs(
        &self,
        addrs: &Addrs,
        expected_stage_id: u64,
    ) -> Result<(), Box<Status>> {
        // Ensure we have exactly one stage - multiple stages would indicate
        // an error in query planning or ticket creation
        if addrs.len() != 1 {
            return Err(Box::new(Status::internal(format!(
                "Expected exactly one stage in addrs, got {}",
                addrs.len()
            ))));
        }

        // Verify the stage ID matches what the client expects
        if !addrs.contains_key(&expected_stage_id) {
            return Err(Box::new(Status::internal(format!(
                "No addresses found for stage_id {} in addrs",
                expected_stage_id
            ))));
        }
        Ok(())
    }
}

#[async_trait]
impl FlightSqlHandler for DDProxyHandler {
    /// Handles Arrow Flight SQL requests for regular SQL queries
    ///
    /// This is the main entry point for SQL query processing in the distributed system.
    /// It receives a SQL string from the client, prepares it for distributed execution,
    /// and returns flight info containing a ticket for result retrieval.
    ///
    /// # Flow
    /// 1. Parse SQL and create distributed execution plan with stages
    /// 2. Distribute tasks of stages to available worker nodes  
    /// 3. Return flight info with ticket containing worker addresses and metadata
    ///
    /// # Arguments
    /// * `query` - SQL query string from the client
    /// * `_request` - Arrow Flight request metadata (unused)
    ///
    /// # Returns
    /// * `FlightInfo` - Contains ticket for subsequent data retrieval via `do_get`
    async fn get_flight_info_statement(
        &self,
        query: arrow_flight::sql::CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Parse SQL and create distributed query plan
        // This involves: SQL -> LogicalPlan -> PhysicalPlan -> DistributedPlan with stages
        let mut query_plan = self
            .planner
            .prepare(&query.query)
            .await
            .map_err(|e| Status::internal(format!("Could not prepare query {e:?}")))?;

        // Distribute the query plan to worker nodes
        // This sends execution tasks to workers and gets back their addresses
        self.planner
            .distribute_plan(&mut query_plan)
            .await
            .map_err(|e| Status::internal(format!("Could not distribute plan {e:?}")))?;

        // Create flight info response containing a ticket
        // The ticket encodes query metadata that clients use to fetch actual data
        self.create_flight_info_response(query_plan)
            .map(Response::new)
            .context("Could not create flight info response")
            .map_err(|e| Status::internal(format!("Error creating flight info: {e:?}")))
    }

    /// Handles Arrow Flight SQL requests for Substrait plans
    ///
    /// This function processes pre-compiled Substrait plans instead of raw SQL.
    /// Substrait is a cross-language serialization format for relational query plans.
    /// This allows clients to send optimized plans directly without SQL parsing.
    ///
    /// # Flow
    /// 1. Decode Substrait protobuf plan from request
    /// 2. Convert Substrait plan to DataFusion logical plan  
    /// 3. Create distributed execution plan with stages
    /// 4. Distribute tasks of stages to available worker nodes
    /// 5. Return flight info with ticket for result retrieval
    ///
    /// # Arguments
    /// * `substrait` - Substrait plan protobuf from client
    /// * `_request` - Arrow Flight request metadata (unused)
    ///
    /// # Returns
    /// * `FlightInfo` - Contains ticket for subsequent data retrieval via `do_get`
    async fn get_flight_info_substrait_plan(
        &self,
        substrait: arrow_flight::sql::CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Decode the Substrait plan from protobuf format
        let plan = match &substrait.plan {
            Some(substrait_plan) => substrait::proto::Plan::decode(substrait_plan.plan.as_ref())
                .map_err(|e| Status::invalid_argument(format!("Invalid Substrait plan: {e}")))?,
            None => return Err(Status::invalid_argument("Missing Substrait plan")),
        };

        // Convert Substrait plan to distributed query plan
        // This involves: Substrait -> LogicalPlan -> PhysicalPlan -> DistributedPlan with stages
        let mut query_plan = self
            .planner
            .prepare_substrait(plan)
            .await
            .map_err(|e| Status::internal(format!("Could not prepare query {e:?}")))?;

        // Distribute the query plan to worker nodes
        // This sends execution tasks to workers and gets back their addresses
        self.planner
            .distribute_plan(&mut query_plan)
            .await
            .map_err(|e| Status::internal(format!("Could not distribute plan {e:?}")))?;

        // Create flight info response containing a ticket
        // The ticket encodes query metadata that clients use to fetch actual data
        self.create_flight_info_response(query_plan)
            .map(Response::new)
            .context("Could not create flight info response")
            .map_err(|e| Status::internal(format!("Error creating flight info: {e:?}")))
    }

    /// Handles client requests to fetch actual query result data
    ///
    /// This function is called when a client uses the ticket (from get_flight_info_statement)
    /// to retrieve the actual query results. It creates a distributed reader plan that
    /// connects to worker nodes and streams back the computed results.
    ///
    /// # Flow
    /// 1. Parse ticket to extract query metadata and worker addresses
    /// 2. Create DDStageReaderExec plan to read from distributed workers
    /// 3. Execute the reader plan to fetch and stream results back to client
    ///
    /// # Arguments
    /// * `ticket` - Contains encoded query metadata from previous get_flight_info call
    /// * `request` - Arrow Flight request metadata
    ///
    /// # Returns
    /// * `DoGetStream` - Streaming Arrow data from distributed workers
    async fn do_get_statement(
        &self,
        ticket: arrow_flight::sql::TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        trace!("do_get_statement");

        // Extract client information for logging and debugging
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        // Decode the ticket to extract query execution metadata
        // The ticket was created in get_flight_info_statement and contains:
        // - query_id, stage_id, worker addresses, schema
        let tsd = TicketStatementData::decode(ticket.statement_handle)
            .map_err(|e| Status::internal(format!("Cannot parse statement handle {e:?}")))?;

        debug!("request for ticket: {:?} from {}", tsd, remote_addr);

        // Extract and convert the schema for the result data
        let schema: Schema = tsd
            .schema
            .as_ref()
            .ok_or_else(|| Status::internal("No schema in TicketStatementData"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Cannot convert schema {e}")))?;

        // Extract worker addresses for the final stage that contains the results
        let stage_addrs = tsd.stage_addrs.ok_or_else(|| {
            Status::internal("No stages_addrs in TicketStatementData, cannot proceed")
        })?;

        // Convert protobuf stage addresses to internal address format
        let addrs: Addrs = get_addrs(&stage_addrs).map_err(|e| {
            Status::internal(format!("Cannot get addresses from stage_addrs {e:?}"))
        })?;

        trace!("calculated addrs: {:?}", addrs);

        // Ensure we have exactly one stage (the final result stage)
        // Multiple stages would indicate an error in query planning
        self.validate_single_stage_addrs(&addrs, tsd.stage_id)
            .map_err(|e| *e)?;

        // Get the specific worker addresses for this stage's partitions
        let stage_partition_addrs = addrs.get(&tsd.stage_id).ok_or_else(|| {
            Status::internal(format!(
                "No partition addresses found for stage_id {}",
                tsd.stage_id
            ))
        })?;

        // Create a DDStageReaderExec plan that will connect to workers and read results
        // This plan knows how to fetch data from distributed workers and combine streams
        let plan = Arc::new(
            DDStageReaderExec::try_new(
                Partitioning::UnknownPartitioning(stage_partition_addrs.len()),
                Arc::new(schema),
                tsd.stage_id,
            )
            // Coalesce all partitions into a single stream for the client
            // TODO: revisit this to allow for consuming a particular partition
            .map(|stg| CoalescePartitionsExec::new(Arc::new(stg)))
            .map_err(|e| Status::internal(format!("Unexpected error {e}")))?,
        ) as Arc<dyn ExecutionPlan>;

        debug!(
            "request for query_id {} from {} reader plan:\n{}",
            tsd.query_id,
            remote_addr,
            display_plan_with_partition_counts(&plan)
        );

        // Execute the distributed reader plan and return streaming results to client
        self.execute_plan_and_build_stream(plan, tsd.query_id, tsd.stage_id, addrs)
            .await
    }
}

/// DataFusion distributed proxy service
///
/// This service provides the complete Arrow Flight SQL server for a DataFusion proxy node.
/// It acts as the entry point for distributed queries, coordinating between SQL clients
/// and distributed worker nodes. The service handles the full distributed query lifecycle:
/// - Receives SQL queries from clients via Arrow Flight SQL protocol
/// - Parses and plans queries for distributed execution
/// - Distributes execution stages to worker nodes
/// - Coordinates result retrieval from workers
/// - Streams combined results back to clients
///
/// # Architecture
/// The service wraps a DDProxyHandler and provides the network server infrastructure.
/// It supports graceful shutdown, request logging, and connection management for
/// high-performance distributed query processing.
///
/// # Protocol Support
/// - Arrow Flight SQL: Primary interface for SQL query execution
/// - Substrait: Support for pre-compiled query plans
/// - Arrow Flight: Efficient streaming of query results
pub struct DDProxyService {
    /// TCP listener for incoming Arrow Flight connections from clients
    listener: TcpListener,
    /// Core proxy handler that processes distributed queries
    handler: Arc<DDProxyHandler>,
    /// Network address where this proxy service is listening
    addr: String,
    /// Channel sender for coordinating graceful shutdown
    all_done_tx: Arc<Mutex<Sender<()>>>,
    /// Channel receiver for shutdown signaling (taken during serve())
    all_done_rx: Option<Receiver<()>>,
}

impl DDProxyService {
    /// Creates a new distributed DataFusion proxy service
    ///
    /// This constructor sets up the complete proxy service infrastructure for handling
    /// distributed SQL queries. It binds to a network port, initializes the query handler
    /// with worker discovery, and prepares for serving Arrow Flight SQL requests.
    ///
    /// # Arguments
    /// * `name` - Human-readable name for this proxy instance (used in logging)
    /// * `port` - TCP port to bind to (0 for automatic port assignment)
    /// * `ctx_customizer` - Optional customization for DataFusion contexts and serialization
    ///
    /// # Returns
    /// * `DDProxyService` - Ready-to-serve proxy service instance
    /// * `Err()` - If port binding, worker discovery, or handler creation fails
    ///
    /// # Setup Process
    /// 1. Binds to the specified TCP port for client connections
    /// 2. Performs worker discovery to find available worker nodes
    /// 3. Creates the proxy handler with distributed query capabilities
    /// 4. Sets up graceful shutdown coordination channels
    pub async fn new(
        name: String,
        port: usize,
        ctx_customizer: Option<Arc<dyn Customizer>>,
    ) -> Result<Self> {
        debug!("Creating DDProxyService!");

        // Set up channels for coordinating graceful shutdown between methods
        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        // Bind to the specified TCP port for Arrow Flight connections
        let listener = start_up(port).await?;

        let addr = format!("{}", listener.local_addr().unwrap());

        info!("DDProxyService bound to {addr}");

        // Create the core proxy handler with worker discovery and query planning
        let handler = Arc::new(DDProxyHandler::new(name, addr.clone(), ctx_customizer));

        Ok(Self {
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// Returns the network address where this proxy service is listening
    ///
    /// This method provides the complete network address (host:port) where clients
    /// can connect to this proxy service. Useful for service registration, discovery,
    /// and client configuration.
    ///
    /// # Returns
    /// * `Ok(String)` - Network address in "host:port" format
    /// * `Err()` - Currently always returns Ok, but Result maintained for future compatibility
    ///
    /// # Example
    /// The returned address might be "127.0.0.1:20200" or "0.0.0.0:8080"
    pub fn addr(&self) -> Result<String> {
        Ok(self.addr.clone())
    }

    /// Signals the proxy service to shut down gracefully
    ///
    /// This method initiates a graceful shutdown sequence that allows ongoing queries
    /// to complete before terminating the service. The shutdown signal is sent to the
    /// serve() method, which will then complete its server loop and clean up resources.
    ///
    /// # Returns
    /// * `Ok(())` - If shutdown signal was sent successfully
    /// * `Err()` - If the shutdown channel was closed (service may already be stopping)
    ///
    /// # Graceful Shutdown Process
    /// 1. Stops accepting new client connections
    /// 2. Allows ongoing queries to complete
    /// 3. Cleans up worker connections and resources
    /// 4. Terminates the server loop
    pub async fn all_done(&self) -> Result<()> {
        let sender = self.all_done_tx.lock().clone();

        sender
            .send(())
            .await
            .context("Could not send shutdown signal")?;
        Ok(())
    }

    /// Starts the proxy service and serves Arrow Flight SQL requests
    ///
    /// This method consumes the service and runs the main server loop, handling
    /// distributed SQL queries from clients. It serves both Arrow Flight SQL requests
    /// and regular Arrow Flight requests, with support for graceful shutdown.
    ///
    /// # Server Capabilities
    /// - Arrow Flight SQL: get_flight_info_statement, do_get_statement
    /// - Substrait support: get_flight_info_substrait_plan
    /// - Request logging and debugging via interceptor
    /// - Graceful shutdown on signal
    ///
    /// # Returns
    /// * `Ok(())` - When service shuts down gracefully
    /// * `Err()` - If server startup or serving fails
    ///
    /// # Shutdown Behavior
    /// The server will continue serving until all_done() is called, then gracefully
    /// complete ongoing requests before terminating.
    pub async fn serve(mut self) -> Result<()> {
        let mut all_done_rx = self.all_done_rx.take().unwrap();

        // Set up graceful shutdown signal handling
        let signal = async move {
            all_done_rx
                .recv()
                .await
                .expect("problem receiving shutdown signal");
            info!("received shutdown signal");
        };

        // Create Arrow Flight SQL service with our proxy handler
        let service = FlightSqlServ {
            handler: self.handler.clone(),
        };

        /// Request interceptor for logging and debugging Arrow Flight requests
        ///
        /// This function intercepts all incoming Arrow Flight requests before they
        /// reach the main handler. It logs request details for debugging and monitoring
        /// purposes, helping track query patterns and diagnose issues.
        ///
        /// # Arguments
        /// * `req` - Incoming Arrow Flight request
        ///
        /// # Returns
        /// * `Ok(Request)` - Unmodified request (passthrough after logging)
        /// * `Err(Status)` - If request should be rejected (currently never)
        #[allow(clippy::result_large_err)]
        fn intercept(req: Request<()>) -> Result<Request<()>, Status> {
            // Log request details to both stdout and debug logging
            println!("Intercepting request: {:?}", req);
            debug!("Intercepting request: {:?}", req);
            Ok(req)
        }

        // Create Arrow Flight service with request interceptor for debugging
        let svc = FlightServiceServer::with_interceptor(service, intercept);

        // Start the server with graceful shutdown support
        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(self.listener),
                signal,
            )
            .await
            .context("error running service")?;
        Ok(())
    }
}
