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

use anyhow::{Context, anyhow};
use arrow::datatypes::{Schema, SchemaRef};
use arrow_flight::{
    FlightDescriptor,
    FlightEndpoint,
    FlightInfo,
    Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightServiceServer,
    sql::{ProstMessageExt, TicketStatementQuery},
};
use datafusion::physical_plan::{
    ExecutionPlan,
    Partitioning,
    coalesce_partitions::CoalescePartitionsExec,
};
use futures::TryStreamExt;
use parking_lot::Mutex;
use prost::Message;
use tokio::{
    net::TcpListener,
    sync::mpsc::{Receiver, Sender, channel},
};
use tonic::{Request, Response, Status, async_trait, transport::Server};

use crate::{
    flight::{FlightSqlHandler, FlightSqlServ},
    k8s::get_worker_addresses,
    logging::{debug, info, trace},
    planning::{
        add_ctx_extentions,
        distribute_stages,
        execution_planning,
        get_ctx,
        logical_planning,
        physical_planning,
    },
    protobuf::TicketStatementData,
    result::Result,
    stage_reader::DFRayStageReaderExec,
    util::{display_plan_with_partition_counts, get_addrs},
    vocab::Addrs,
};

struct DfRayProxyHandler {}

impl DfRayProxyHandler {
    pub fn new() -> Self {
        // call this function but ignore the results to bootstrap the worker
        // discovery mechanism
        get_worker_addresses().expect("Could not get worker addresses upoon startup");
        Self {}
    }

    /// Prepare a query by parsing the SQL, planning it, and distributing the
    /// physical plan into stages that can be executed by workers.
    ///
    /// Returns a tuple containing:
    /// - `query_id`: A unique identifier for the query.
    /// - `addrs`: The address of the worker that will execute the final stage.
    /// - `final_stage_id`: The ID of the final stage in the execution plan.
    /// - `schema`: The schema of the final stage.
    pub async fn prepare_query(&self, sql: &str) -> Result<(String, Addrs, u64, SchemaRef)> {
        let query_id = uuid::Uuid::new_v4().to_string();

        let ctx = get_ctx()?;

        let logical_plan = logical_planning(sql, &ctx).await?;

        let physical_plan = physical_planning(&logical_plan, &ctx).await?;

        // divide the physical plan into chunks (stages) that we can distribute
        let stages = execution_planning(physical_plan, 8192, Some(2)).await?;

        if stages.is_empty() {
            return Err(anyhow!("No stages generated for query").into());
        }

        let worker_addrs = get_worker_addresses()?;
        debug!(
            "Worker addresses found:\n{}",
            worker_addrs
                .iter()
                .map(|(name, addr)| format!("{}: {}", name, addr))
                .collect::<Vec<_>>()
                .join("\n")
        );

        // gather some information we need to send back such that
        // we can send a ticket to the client
        let final_stage = &stages[stages.len() - 1];
        let schema = final_stage.plan.schema().clone();
        let final_stage_id = final_stage.stage_id;

        // distribute the stages to workers, further dividing them up
        // into chunks of partitions (partition_groups)
        let final_workers = distribute_stages(&query_id, stages, worker_addrs).await?;

        Ok((query_id, final_workers, final_stage_id, schema))
    }
}

#[async_trait]
impl FlightSqlHandler for DfRayProxyHandler {
    async fn get_flight_info_statement(
        &self,
        query: arrow_flight::sql::CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let (query_id, final_addrs, final_stage_id, schema) = self
            .prepare_query(&query.query)
            .await
            .map_err(|e| Status::internal(format!("Could not prepare query {e:?}")))?;

        debug!("get flight info: query id {query_id}");

        let mut fi = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Could not create flight info {e:?}")))?;

        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(
            TicketStatementQuery {
                statement_handle: TicketStatementData {
                    query_id: query_id.clone(),
                    stage_id: final_stage_id,
                    stage_addrs: Some(final_addrs.into()),
                    schema: Some(schema.try_into().map_err(|e| {
                        Status::internal(format!("Could not convert schema {e:?}"))
                    })?),
                }
                .encode_to_vec()
                .into(),
            }
            .as_any()
            .encode_to_vec(),
        ));
        fi = fi.with_endpoint(endpoint);

        trace!("get_flight_info_statement done");
        Ok(Response::new(fi))
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

        // create an Addrs from the final stage information in the tsd
        let stage_addrs = tsd.stage_addrs.ok_or_else(|| {
            Status::internal("No stages_addrs in TicketStatementData, cannot proceed")
        })?;

        let addrs: Addrs = get_addrs(&stage_addrs).map_err(|e| {
            Status::internal(format!("Cannot get addresses from stage_addrs {e:?}"))
        })?;

        trace!("calculated addrs: {:?}", addrs);

        // assert that the addrs looks right.  It should hold information for a single
        // stage only, the tsd.stage_id
        if addrs.len() != 1 {
            return Err(Status::internal(format!(
                "Expected exactly one stage in addrs, got {}",
                addrs.len()
            )));
        }
        if !addrs.contains_key(&tsd.stage_id) {
            return Err(Status::internal(format!(
                "No addresses found for stage_id {} in addrs",
                tsd.stage_id
            )));
        }

        let plan = Arc::new(
            DFRayStageReaderExec::try_new(
                Partitioning::UnknownPartitioning(addrs.get(&tsd.stage_id).unwrap().len()),
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

        let mut ctx =
            get_ctx().map_err(|e| Status::internal(format!("Could not create context {e:?}")))?;

        add_ctx_extentions(&mut ctx, "proxy", &tsd.query_id, addrs, None)
            .map_err(|e| Status::internal(format!("Could not add context extensions {e:?}")))?;

        // TODO: revisit this to allow for consuming a partitular partition
        trace!("calling execute plan");
        let partition = 0;
        let stream = plan
            .execute(partition, ctx.task_ctx())
            .map_err(|e| {
                Status::internal(format!(
                    "Error executing plan for query_id {} partition {}: {e:?}",
                    tsd.query_id, partition
                ))
            })?
            .map_err(|e| FlightError::ExternalError(Box::new(e)));

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| Status::internal(format!("Unexpected error building stream {e:?}")));

        Ok(Response::new(Box::pin(out_stream)))
    }
}

/// DFRayProcessorService is a Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
pub struct DFRayProxyService {
    listener: Option<TcpListener>,
    handler: Arc<DfRayProxyHandler>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
    port: usize,
}

impl DFRayProxyService {
    pub fn new(port: usize) -> Self {
        debug!("Creating DFRayProxyService!");
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let handler = Arc::new(DfRayProxyHandler::new());

        Self {
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
            port,
        }
    }

    pub async fn start_up(&mut self) -> Result<()> {
        let my_host_str = format!("0.0.0.0:{}", self.port);

        self.listener = TcpListener::bind(&my_host_str)
            .await
            .map(Some)
            .context("Could not bind socket to {my_host_str}")?;

        self.addr = Some(format!(
            "{}",
            self.listener.as_ref().unwrap().local_addr().unwrap()
        ));

        info!("DFRayProxyService bound to {}", self.addr.as_ref().unwrap());

        Ok(())
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> Result<String> {
        let addr = self.addr.clone().ok_or(anyhow!(
            "DFRayProxyService not started yet, no address available"
        ))?;
        Ok(addr)
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
    pub async fn serve(&mut self) -> Result<()> {
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

        let listener = self.listener.take().unwrap();

        Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                signal,
            )
            .await
            .context("error running service")?;
        Ok(())
    }
}
