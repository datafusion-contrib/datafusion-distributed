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

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{Context, anyhow};
use arrow::array::RecordBatch;
use arrow_flight::{
    Action,
    Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::FlightServiceServer,
};
use async_stream::stream;
use datafusion::{
    physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable},
    prelude::SessionContext,
};
use futures::{Stream, TryStreamExt};
use parking_lot::{Mutex, RwLock};
use prost::Message;
use tokio::{
    net::TcpListener,
    sync::mpsc::{Receiver, Sender, channel},
};
use tonic::{Request, Response, Status, async_trait, transport::Server};

use crate::{
    flight::{FlightHandler, FlightServ},
    logging::{debug, error, info, trace},
    planning::{add_ctx_extentions, get_ctx},
    protobuf::{FlightTicketData, StageData},
    result::{DFRayError, Result},
    util::{
        bytes_to_physical_plan,
        display_plan_with_partition_counts,
        get_addrs,
        register_object_store_for_paths_in_plan,
        reporting_stream,
    },
    vocab::{Addrs, CtxName},
};

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct PlanKey {
    query_id: String,
    stage_id: u64,
    partition: u64,
}

// For each plan key, we may have multiple plans that we might need to hold
// with the same key.
type PlanVec = Vec<(SystemTime, SessionContext, Arc<dyn ExecutionPlan>)>;

/// It only responds to the DoGet Arrow Flight method.
struct DfRayProcessorHandler {
    /// our name, useful for logging
    name: String,
    /// our map of query_id -> (session ctx, execution plan)
    #[allow(clippy::type_complexity)]
    plans: Arc<RwLock<HashMap<PlanKey, PlanVec>>>,
    done: Arc<Mutex<bool>>,
}

impl DfRayProcessorHandler {
    pub fn new(name: String) -> Self {
        let plans: Arc<RwLock<HashMap<PlanKey, PlanVec>>> = Arc::new(RwLock::new(HashMap::new()));
        let done = Arc::new(Mutex::new(false));

        // start a plan janitor ask to clean up old plans that were not collected for
        // any reason
        let c_plans = plans.clone();
        let c_done = done.clone();
        let c_name = name.clone();
        std::thread::spawn(move || {
            while !(*c_done.lock()) {
                // wait for 10 seconds
                std::thread::sleep(Duration::from_secs(10));
                if *c_done.lock() {
                    info!("{} plan janitor done", c_name);
                    break;
                }
                trace!("{} plan janitor waking up", c_name);

                let now = SystemTime::now();

                let mut to_remove = vec![];
                {
                    let _guard = c_plans.read();
                    for (key, plan_vec) in _guard.iter() {
                        if plan_vec.is_empty() {
                            error!("unexpectedly found empty plan vec.  removing");
                            to_remove.push(key.clone());
                        } else {
                            // check if any plan in this vec is older than 1 minute
                            for (insert_time, _, _) in plan_vec.iter() {
                                if now
                                    .duration_since(*insert_time)
                                    .map(|d| d.as_secs() > 60)
                                    .inspect_err(|e| {
                                        error!("CANNOT COMPUTE DURATION OR REMOVE PLANS: {e:?}");
                                    })
                                    .unwrap_or(false)
                                {
                                    to_remove.push(key.clone());
                                    break;
                                }
                            }
                        }
                    }
                }
                if !to_remove.is_empty() {
                    let mut _guard = c_plans.write();
                    for key in to_remove.iter() {
                        _guard.remove(key);
                        debug!("{} removed old plan key {:?}", c_name, key);
                    }
                }
            }
        });

        Self { name, plans, done }
    }

    #[allow(dead_code)]
    pub fn all_done(&self) {
        *self.done.lock() = true;
    }

    pub async fn add_plan(
        &self,
        query_id: String,
        stage_id: u64,
        stage_addrs: Addrs,
        partition_group: Vec<u64>,
        full_partitions: bool,
        plan_bytes: &[u8],
    ) -> Result<()> {
        let ctx = self
            .configure_ctx(
                query_id.clone(),
                stage_id,
                stage_addrs.clone(),
                partition_group.clone(),
            )
            .await?;

        let plan = bytes_to_physical_plan(&ctx, plan_bytes).context(format!(
            "{}, Could not decode plan for query_id {} stage {}",
            self.name, query_id, stage_id
        ))?;

        let partitions = if full_partitions {
            partition_group.clone()
        } else {
            (0..(plan.output_partitioning().partition_count()))
                .map(|p| p as u64)
                .collect::<Vec<u64>>()
        };

        trace!(
            "{} adding plan for stage {} partitions: {:?} stage_addrs: {:?} plan:\n{}",
            self.name,
            stage_id,
            partitions,
            stage_addrs,
            display_plan_with_partition_counts(&plan)
        );

        register_object_store_for_paths_in_plan(&ctx, plan.clone())?;

        let now = SystemTime::now();

        for partition in partitions.iter() {
            let key = PlanKey {
                query_id: query_id.clone(),
                stage_id,
                partition: *partition,
            };
            {
                let mut _guard = self.plans.write();
                if let Some(plan_vec) = _guard.get_mut(&key) {
                    plan_vec.push((now, ctx.clone(), plan.clone()));
                } else {
                    _guard.insert(key.clone(), vec![(now, ctx.clone(), plan.clone())]);
                }
                trace!("{} added plan for plan key {:?}", self.name, key);
            }
        }

        debug!("{} plans held {:?}", self.name, self.plans.read().len());
        Ok(())
    }

    async fn configure_ctx(
        &self,
        query_id: String,
        stage_id: u64,
        stage_addrs: Addrs,
        partition_group: Vec<u64>,
    ) -> Result<SessionContext> {
        let mut ctx = get_ctx()?;

        add_ctx_extentions(
            &mut ctx,
            &format!("{} stage:{} pg:{:?}", self.name, stage_id, partition_group),
            &query_id,
            stage_addrs.clone(),
            Some(partition_group),
        )?;

        Ok(ctx)
    }

    fn make_stream(
        &self,
        query_id: &str,
        stage_id: u64,
        partition: u64,
    ) -> Result<impl Stream<Item = Result<RecordBatch, FlightError>> + Send + 'static, Status> {
        let key = PlanKey {
            query_id: query_id.to_string(),
            stage_id,
            partition,
        };

        let (ctx, plan) = {
            let mut _guard = self.plans.write();
            let (plan_key, mut plan_vec) = _guard
                .remove_entry(&key)
                .ok_or_else(|| {
                    Status::internal(format!(
                        "{}, No plan found for plan key {:?}",
                        self.name, key,
                    ))
                })
                .inspect_err(|e| {
                    error!(
                        "{}, No plan found for plan key {:?},{e:?} have keys {:?}",
                        self.name,
                        key,
                        _guard.keys().map(|k| format!("{k:?}"))
                    );
                })?;
            trace!(
                "{} found {} plans for plan key {:?}",
                self.name,
                plan_vec.len(),
                plan_key
            );
            let (_insert_time, ctx, plan) = plan_vec.pop().expect("plan_vec should not be empty");
            if !plan_vec.is_empty() {
                _guard.insert(plan_key, plan_vec);
            }
            (ctx, plan)
        };
        trace!(
            "make_stream for plan {}",
            displayable(plan.as_ref()).indent(true)
        );

        let task_ctx = ctx.task_ctx();

        let ctx_name = task_ctx
            .session_config()
            .get_extension::<CtxName>()
            .ok_or_else(|| {
                Status::internal(format!("{}, CtxName not set in session config", self.name))
            })?
            .0
            .clone();

        let stream = plan
            .execute(partition as usize, task_ctx)
            .inspect_err(|e| error!("Could not get partition stream from plan {e:?}"))
            .map(|s| reporting_stream(&format!("{ctx_name} s:{stage_id} p:{partition}"), s))
            .map_err(|e| Status::internal(format!("Could not get partition stream from plan {e}")))?
            .map_err(|e| FlightError::from_external_error(Box::new(e)));

        info!("{} plans held {}", self.name, self.plans.read().len());

        Ok(stream)
    }
}

#[async_trait]
impl FlightHandler for DfRayProcessorHandler {
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<crate::flight::DoGetStream>, Status> {
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let ticket = request.into_inner();

        let ftd = FlightTicketData::decode(ticket.ticket).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error extracting ticket {e:?}",
                self.name
            ))
        })?;

        let plan_key = PlanKey {
            query_id: ftd.query_id.clone(),
            stage_id: ftd.stage_id,
            partition: ftd.partition,
        };

        debug!(
            "{}, request for plan_key:{:?} from: {},{}",
            self.name, plan_key, ftd.requestor_name, remote_addr
        );

        let name = self.name.clone();
        let stream = self
            .make_stream(&ftd.query_id, ftd.stage_id, ftd.partition)
            .map_err(|e| {
                Status::internal(format!("{name} Unexpected error making stream {e:?}"))
            })?;

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| {
                Status::internal(format!("{name} Unexpected error building stream {e:?}"))
            });

        Ok(Response::new(Box::pin(out_stream)))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<crate::flight::DoActionStream>, Status> {
        // extract a StageData protobuf from the action body
        let action = request.into_inner();
        let action_type = action.r#type.as_str();
        trace!("{} received action: {}", self.name, action_type);

        if action_type != "add_plan" {
            return Err(Status::unimplemented(format!(
                "{}, Unimplemented action: {}",
                self.name, action_type
            )));
        }

        let stage_data = StageData::decode(action.body.as_ref()).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error decoding StageData: {e:?}",
                self.name
            ))
        })?;

        let addrs = stage_data
            .stage_addrs
            .as_ref()
            .context("stage addrs not present")
            .map_err(DFRayError::from)
            .and_then(get_addrs)
            .map_err(|e| Status::internal(format!("{}, {e}", self.name)))?;

        self.add_plan(
            stage_data.query_id,
            stage_data.stage_id,
            addrs,
            stage_data.partition_group,
            stage_data.full_partitions,
            &stage_data.plan_bytes,
        )
        .await
        .map_err(|e| Status::internal(format!("{}, Could not add plan: {e:?}", self.name)))?;

        let out_stream = Box::pin(stream! {
            yield Ok::<_, tonic::Status>(arrow_flight::Result::default());
        }) as crate::flight::DoActionStream;

        Ok(Response::new(out_stream))
    }
}

/// DFRayProcessorService is a Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
pub struct DFRayProcessorService {
    #[allow(dead_code)]
    name: String,
    listener: Option<TcpListener>,
    handler: Arc<DfRayProcessorHandler>,
    addr: Option<String>,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
    port: usize,
}

impl DFRayProcessorService {
    pub fn new(name: String, port: usize) -> Self {
        let name = format!("[{}]", name);
        let listener = None;
        let addr = None;

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let handler = Arc::new(DfRayProcessorHandler::new(name.clone()));

        Self {
            name,
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

        info!(
            "DFRayProcessorService bound to {}",
            self.addr.as_ref().unwrap()
        );

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

        let flight_serv = FlightServ {
            handler: self.handler.clone(),
        };

        let svc = FlightServiceServer::new(flight_serv);

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
