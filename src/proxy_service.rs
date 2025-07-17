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
    worker_discovery::{ WorkerDiscovery},
};

pub struct DDProxyHandler {
    /// our host info, useful for logging
    pub host: Host,

    pub planner: QueryPlanner,
    pub discovery: Arc<dyn WorkerDiscovery>,

    /// Optional customizer for our context and proto serde
    pub customizer: Option<Arc<dyn Customizer>>,
}

impl DDProxyHandler {
    pub fn new(name: String, addr: String, discovery: Arc<dyn WorkerDiscovery>, customizer: Option<Arc<dyn Customizer>>) -> Self {
        let host = Host {
            name: name.clone(),
            addr: addr.clone(),
        };
        Self {
            host: host.clone(),
            planner: QueryPlanner::new(customizer.clone(), discovery.clone()),
            discovery,
            customizer,
        }
    }

    pub fn create_flight_info_response(&self, query_plan: QueryPlan) -> Result<FlightInfo, Status> {
        let mut flight_info = FlightInfo::new()
            .try_with_schema(&query_plan.schema)
            .map_err(|e| Status::internal(format!("Could not create flight info {e:?}")))?;

        let ticket_data = TicketStatementData {
            query_id: query_plan.query_id,
            stage_id: query_plan.final_stage_id,
            stage_addrs: Some(query_plan.worker_addresses.into()),
            schema: Some(
                query_plan
                    .schema
                    .try_into()
                    .map_err(|e| Status::internal(format!("Could not convert schema {e:?}")))?,
            ),
        };

        let ticket = Ticket::new(
            TicketStatementQuery {
                statement_handle: ticket_data.encode_to_vec().into(),
            }
            .as_any()
            .encode_to_vec(),
        );

        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        flight_info = flight_info.with_endpoint(endpoint);

        Ok(flight_info)
    }

    pub async fn execute_plan_and_build_stream(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        query_id: String,
        stage_id: u64,
        addrs: Addrs,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        let mut ctx =
            get_ctx().map_err(|e| Status::internal(format!("Could not create context {e:?}")))?;

        add_ctx_extentions(&mut ctx, &self.host, &query_id, stage_id, addrs, vec![])
            .map_err(|e| Status::internal(format!("Could not add context extensions {e:?}")))?;

        if let Some(ref c) = self.customizer {
            c.customize(&mut ctx)
                .await
                .map_err(|e| Status::internal(format!("Could not customize context {e:?}")))?;
        }

        // TODO: revisit this to allow for consuming a partitular partition
        trace!("calling execute plan");
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

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| Status::internal(format!("Unexpected error building stream {e:?}")));

        Ok(Response::new(Box::pin(out_stream)))
    }

    pub fn validate_single_stage_addrs(
        &self,
        addrs: &Addrs,
        expected_stage_id: u64,
    ) -> Result<(), Status> {
        if addrs.len() != 1 {
            return Err(Status::internal(format!(
                "Expected exactly one stage in addrs, got {}",
                addrs.len()
            )));
        }
        if !addrs.contains_key(&expected_stage_id) {
            return Err(Status::internal(format!(
                "No addresses found for stage_id {} in addrs",
                expected_stage_id
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl FlightSqlHandler for DDProxyHandler {
    async fn get_flight_info_statement(
        &self,
        query: arrow_flight::sql::CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let query_plan = self
            .planner
            .prepare(&query.query)
            .await
            .map_err(|e| Status::internal(format!("Could not prepare query {e:?}")))?;

        self.create_flight_info_response(query_plan)
            .map(Response::new)
            .context("Could not create flight info response")
            .map_err(|e| Status::internal(format!("Error creating flight info: {e:?}")))
    }

    async fn get_flight_info_substrait_plan(
        &self,
        substrait: arrow_flight::sql::CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let plan = match &substrait.plan {
            Some(substrait_plan) => substrait::proto::Plan::decode(substrait_plan.plan.as_ref())
                .map_err(|e| Status::invalid_argument(format!("Invalid Substrait plan: {e}")))?,
            None => return Err(Status::invalid_argument("Missing Substrait plan")),
        };

        let query_plan = self
            .planner
            .prepare_substrait(plan)
            .await
            .map_err(|e| Status::internal(format!("Could not prepare query {e:?}")))?;

        self.create_flight_info_response(query_plan)
            .map(Response::new)
            .context("Could not create flight info response")
            .map_err(|e| Status::internal(format!("Error creating flight info: {e:?}")))
    }

    async fn do_get_statement(
        &self,
        ticket: arrow_flight::sql::TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        trace!("do_get_statement");
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let tsd = TicketStatementData::decode(ticket.statement_handle)
            .map_err(|e| Status::internal(format!("Cannot parse statement handle {e:?}")))?;

        debug!("request for ticket: {:?} from {}", tsd, remote_addr);

        let schema: Schema = tsd
            .schema
            .as_ref()
            .ok_or_else(|| Status::internal("No schema in TicketStatementData"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Cannot convert schema {e}")))?;

        // Create an Addrs from the final stage information in the tsd
        let stage_addrs = tsd.stage_addrs.ok_or_else(|| {
            Status::internal("No stages_addrs in TicketStatementData, cannot proceed")
        })?;

        let addrs: Addrs = get_addrs(&stage_addrs).map_err(|e| {
            Status::internal(format!("Cannot get addresses from stage_addrs {e:?}"))
        })?;

        trace!("calculated addrs: {:?}", addrs);

        // Validate that addrs contains exactly one stage
        self.validate_single_stage_addrs(&addrs, tsd.stage_id)?;

        let stage_partition_addrs = addrs.get(&tsd.stage_id).ok_or_else(|| {
            Status::internal(format!(
                "No partition addresses found for stage_id {}",
                tsd.stage_id
            ))
        })?;

        let plan = Arc::new(
            DDStageReaderExec::try_new(
                Partitioning::UnknownPartitioning(stage_partition_addrs.len()),
                Arc::new(schema),
                tsd.stage_id,
            )
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

        self.execute_plan_and_build_stream(plan, tsd.query_id, tsd.stage_id, addrs)
            .await
    }
}

/// An Arrow Flight SQL service
pub struct DDProxyService {
    listener: TcpListener,
    handler: Arc<DDProxyHandler>,
    addr: String,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
}

impl DDProxyService {
    pub async fn new(
        name: String,
        port: usize,
        discovery: Arc<dyn WorkerDiscovery>,
        ctx_customizer: Option<Arc<dyn Customizer>>,
    ) -> Result<Self> {
        debug!("Creating DDProxyService!");

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let listener = start_up(port).await?;

        let addr = format!("{}", listener.local_addr().unwrap());

        info!("DDProxyService bound to {addr}");

        let handler = Arc::new(DDProxyHandler::new(name, addr.clone(), discovery,  ctx_customizer));

        Ok(Self {
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> Result<String> {
        Ok(self.addr.clone())
    }

    pub async fn all_done(&self) -> Result<()> {
        let sender = self.all_done_tx.lock().clone();

        sender
            .send(())
            .await
            .context("Could not send shutdown signal")?;
        Ok(())
    }

    /// start the service
    pub async fn serve(mut self) -> Result<()> {
        let mut all_done_rx = self.all_done_rx.take().unwrap();

        let signal = async move {
            all_done_rx
                .recv()
                .await
                .expect("problem receiving shutdown signal");
            info!("received shutdown signal");
        };

        let service = FlightSqlServ {
            handler: self.handler.clone(),
        };

        fn intercept(req: Request<()>) -> Result<Request<()>, Status> {
            println!("Intercepting request: {:?}", req);
            debug!("Intercepting request: {:?}", req);
            Ok(req)
        }
        //let svc = FlightServiceServer::new(service);
        let svc = FlightServiceServer::with_interceptor(service, intercept);

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
