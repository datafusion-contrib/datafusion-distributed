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
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
    time::{Duration, SystemTime},
};

use anyhow::{anyhow, Context};
use arrow::array::RecordBatch;
use arrow_flight::{
    encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_server::FlightServiceServer, Action, Ticket,
};
use async_stream::stream;
use datafusion::{
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
    prelude::SessionContext,
};
use futures::{StreamExt, TryStreamExt};
use parking_lot::{Mutex, RwLock};
use prost::Message;
use tokio::{
    net::TcpListener,
    sync::mpsc::{channel, Receiver, Sender},
};
use tonic::{async_trait, transport::Server, Request, Response, Status};

use crate::{
    analyze::DistributedAnalyzeExec,
    flight::{FlightHandler, FlightServ},
    logging::{debug, error, info, trace},
    planning::{add_ctx_extentions, get_ctx},
    protobuf::{
        AnnotatedTaskOutput, AnnotatedTaskOutputs, FlightDataMetadata, FlightTicketData, Host,
    },
    result::{DFRayError, Result},
    util::{
        bytes_to_physical_plan, display_plan_with_partition_counts, get_addrs,
        register_object_store_for_paths_in_plan, start_up,
    },
    vocab::{Addrs, CtxAnnotatedOutputs, CtxPartitionGroup, DDTask},
};

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct StageKey {
    query_id: String,
    stage_id: u64,
}

#[derive(Clone)]
struct Task {
    partitions: HashSet<u64>,
    ctx: SessionContext,
    plan: Arc<dyn ExecutionPlan>,
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("partitions", &self.partitions)
            .finish()
    }
}

#[derive(Debug)]
struct StageTasks {
    tasks: Vec<Task>,
    insert_time: SystemTime,
}

/// It only responds to the DoGet Arrow Flight method.
struct DfRayProcessorHandler {
    /// our name, useful for logging
    name: String,
    /// our address string, also useful for logging
    addr: String,
    /// our map of query_id -> (session ctx, execution plan)
    stages: Arc<RwLock<HashMap<StageKey, StageTasks>>>,
    done: Arc<Mutex<bool>>,
}

impl DfRayProcessorHandler {
    pub fn new(name: String, addr: String) -> Self {
        let stages: Arc<RwLock<HashMap<StageKey, StageTasks>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let done = Arc::new(Mutex::new(false));

        // start a plan janitor ask to clean up old plans that were not collected for
        // any reason
        let c_stages = stages.clone();
        let c_done = done.clone();
        let c_name = name.clone();
        std::thread::spawn(move || {
            while !(*c_done.lock()) {
                // wait for 10 seconds
                std::thread::sleep(Duration::from_secs(10));
                if *c_done.lock() {
                    info!("{} janitor done", c_name);
                    break;
                }
                trace!("{} janitor waking up", c_name);

                let now = SystemTime::now();

                let mut to_remove = vec![];
                {
                    let _guard = c_stages.read();
                    for (key, stage_tasks) in _guard.iter() {
                        if stage_tasks.tasks.is_empty() {
                            error!("unexpectedly found empty stage tasks.  removing");
                            to_remove.push(key.clone());
                        } else {
                            // check if any plan in this vec is older than 1 minute
                            if now
                                .duration_since(stage_tasks.insert_time)
                                .map(|d| d.as_secs() > 60)
                                .inspect_err(|e| {
                                    error!("CANNOT COMPUTE DURATION OR REMOVE STAGES: {e:?}");
                                    // maybe just panic here?
                                })
                                .unwrap_or(false)
                            {
                                to_remove.push(key.clone());
                                break;
                            }
                        }
                    }
                }
                if !to_remove.is_empty() {
                    let mut _guard = c_stages.write();
                    for key in to_remove.iter() {
                        _guard.remove(key);
                        debug!("{} removed old stage key {:?}", c_name, key);
                    }
                }
            }
        });

        Self {
            name,
            addr,
            stages,
            done,
        }
    }

    #[allow(dead_code)]
    /// shutdown
    pub fn all_done(&self) {
        *self.done.lock() = true;
    }

    pub async fn add_task(
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
            "{} adding task for stage {} partition group: {:?} partitions: {:?} stage_addrs: {:?} plan:\n{}",
            self.name,
            stage_id,
            partition_group,
            partitions,
            stage_addrs,
            display_plan_with_partition_counts(&plan)
        );

        register_object_store_for_paths_in_plan(&ctx, plan.clone())?;

        let now = SystemTime::now();

        let task = Task {
            ctx: ctx.clone(),
            plan: plan.clone(),
            partitions: HashSet::from_iter(partitions.clone()),
        };

        let key = StageKey {
            query_id: query_id.clone(),
            stage_id,
        };
        {
            let mut _guard = self.stages.write();
            let stage_tasks = _guard.entry(key.clone()).or_insert_with(|| StageTasks {
                tasks: vec![],
                insert_time: now,
            });
            stage_tasks.tasks.push(task.clone());
            trace!("{} added task for stage key {:?}", self.name, key);
        }

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
        let host = Host {
            addr: self.addr.clone(),
            name: self.name.clone(),
        };

        add_ctx_extentions(
            &mut ctx,
            &host,
            &query_id,
            stage_id,
            stage_addrs.clone(),
            partition_group,
        )?;

        Ok(ctx)
    }

    /// Retrieve the requested ctx and plan to execute.  Also return a bool
    /// indicating if this is the last partition for this plan
    fn get_ctx_and_plan(
        &self,
        query_id: &str,
        stage_id: u64,
        partition: u64,
    ) -> Result<(SessionContext, Arc<dyn ExecutionPlan>, bool)> {
        let stage_key = StageKey {
            query_id: query_id.to_string(),
            stage_id,
        };

        let (ctx, plan, last) = {
            let mut _guard = self.stages.write();
            let stage_tasks = _guard.get_mut(&stage_key).context(format!(
                "{}, No plan found for stage key{:?}",
                self.name, stage_key,
            ))?;
            trace!(
                "{} found {} tasks for stage key {:?}",
                self.name,
                stage_tasks.tasks.len(),
                stage_key
            );

            // of the tasks for this stage, find one that has this partition not yet consumed
            let task = stage_tasks
                .tasks
                .iter_mut()
                .find(|t| t.partitions.contains(&partition))
                .context(format!(
                    "{}, No task found for stage key {:?} partition {}",
                    self.name, stage_key, partition
                ))?;

            // remove this partition from the list of partitions yet to be consumed
            if !task.partitions.remove(&partition) {
                // it should be in there because we just filtered for it
                return Err(anyhow!("UNEXPECTED: partition {partition} not in plan parts").into());
            }
            // finally, we return the ctx and plan to execute this task, as well as a
            // bool indicating if this is the last partition for this task
            (
                task.ctx.clone(),
                task.plan.clone(),
                task.partitions.is_empty(),
            )
        };
        {
            // some house keeping, if there are no more partitions left for this task,
            // remove it from the stage tasks.
            let mut _guard = self.stages.write();

            let remove_it = _guard.get_mut(&stage_key).map(|stage_tasks| {
                stage_tasks.tasks.retain(|t| !t.partitions.is_empty());
                trace!(
                    "remaining tasks: for stage {:?}: {:?}",
                    stage_key,
                    stage_tasks
                );

                // furthermore, if there are no more tasks left for this stage,
                // remove the stage from the map
                stage_tasks.tasks.is_empty()
            });

            if remove_it.unwrap_or(false) {
                _guard.remove(&stage_key);
                debug!("{} removed stage key {:?}", self.name, stage_key);
            }
        }

        Ok((ctx, plan, last))
    }

    /// we want to send any additional FlightDataMetadata that we discover while
    /// executing the plan further downstream to consumers of our stream.
    ///
    /// We may discover a FlightDataMetadata as an additional payload incoming
    /// on streming requests.   The [`RayStageReader`] will look for these and add
    /// them to an extension on the context.
    ///
    /// Our job is to send all record batches from our stream over our flight response
    /// but when the stream is exhausted, we'll send an additional message containing the
    /// metadata.
    ///
    /// The reason we have to do it at the end is that the metadata may include an annodated
    /// plan with metrics which will only be available after the stream has been
    /// fully consumed.
    fn make_stream(
        &self,
        ctx: SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        stage_id: u64,
        partition: u64,
        last_partition: bool,
    ) -> Result<crate::flight::DoGetStream> {
        let task_ctx = ctx.task_ctx();

        // the RecordBatchStream from our plan
        let stream = plan
            .execute(partition as usize, task_ctx)
            .inspect_err(|e| error!("Could not get partition stream from plan {e:#?}"))?
            .inspect(|batch| {
                trace!("producing maybe batch {:?}", batch);
            })
            .map_err(|e| FlightError::from_external_error(Box::new(e)));

        info!("{} tasks held {}", self.name, self.stages.read().len());

        fn find_analyze(plan: &dyn ExecutionPlan) -> Option<&DistributedAnalyzeExec> {
            if let Some(target) = plan.as_any().downcast_ref::<DistributedAnalyzeExec>() {
                Some(target)
            } else {
                for child in plan.children() {
                    if let Some(target) = find_analyze(child.as_ref()) {
                        return Some(target);
                    }
                }
                None
            }
        }

        let mut flight_data_stream = FlightDataEncoderBuilder::new().build(stream);
        let name = self.name.clone();
        let host = Host {
            addr: self.addr.clone(),
            name: self.name.clone(),
        };

        #[allow(unused_assignments)] // clippy can't understand our assignment to done in the macro
        let out_stream = async_stream::stream! {
            let mut done = false;
            while !done {

                match (done, last_partition, flight_data_stream.next().await) {
                    (false, false, None) => {
                        // we finished a partition, but still have more to do, do nothing
                        done = true;
                    }
                    (false, true, None) => {
                                // no more data in the last partition, so now we yield our additional FlightDataMetadata
                                debug!("stream exhausted, yielding FlightDataMetadata");
                                let task_outputs = ctx.state().config()
                                    .get_extension::<CtxAnnotatedOutputs>()
                                    .unwrap_or(Arc::new(CtxAnnotatedOutputs::default()))
                                    .0
                                    .clone();

                                let partition_group = ctx.state().config()
                                    .get_extension::<CtxPartitionGroup>()
                                    .expect("CtxPartitionGroup to be set")
                                    .0.clone();


                                if let Some(analyze) = find_analyze(plan.as_ref()) {
                                    let annotated_plan = analyze.annotated_plan();
                                    debug!("sending annotated plan: {}", annotated_plan);

                                    let output = AnnotatedTaskOutput {
                                        plan: annotated_plan,
                                        host: Some(host.clone()),
                                        stage_id,
                                        partition_group,
                                    };
                                    task_outputs.lock().push(output);
                                }

                                let meta = FlightDataMetadata {
                                    annotated_task_outputs: Some(AnnotatedTaskOutputs {
                                        outputs: task_outputs.lock().clone(),
                                    }),
                                };


                                let fake_batch = RecordBatch::new_empty(plan.schema());


                                let mut fde = FlightDataEncoderBuilder::new()
                                    //.with_schema(plan.schema())
                                    .with_metadata(meta.encode_to_vec().into()).build(futures::stream::once(async {Ok(fake_batch)})).map_err(|e| {
                                        FlightError::from_external_error(Box::new(e))
                                    });

                                let flight_data = fde
                                    .next()
                                    .await
                                    .ok_or_else(|| {
                                        Status::internal(format!(
                                            "{}, No FlightDataMetadata from our fde",
                                            name
                                        ))
                                    })??;

                                yield Ok(flight_data);
                                done = true;

                            },
                    (false, _, Some(Err(e))) => {
                                yield Err(Status::internal(format!(
                                    "Unexpected error getting flight data stream: {e:?}",
                                )));
                                done = true;
                            },
                    (false, _, Some(Ok(flight_data))) => {
                                // we have a flight data, so we yield it
                                trace!("received normal flight data, yielding");
                                yield Ok(flight_data);
                            },
                    (true, false, _ ) => {
                                // we've finished a partition, do nothing
                                done = true;
                            },
                    (true, true, _ ) => {
                                yield Err(Status::internal(format!("{name} reached expected unreachable block")));
                            },
                }
            }
        };

        Ok(Box::pin(out_stream))
    }
    fn do_action_get_host(&self) -> Result<Response<crate::flight::DoActionStream>, Status> {
        let addr = self.addr.clone();
        let name = self.name.clone();

        let out_stream = Box::pin(stream! {
            yield Ok::<_, tonic::Status>(arrow_flight::Result {
                body: Host{
                    addr,
                    name,
                }.encode_to_vec().into()
            });
        }) as crate::flight::DoActionStream;

        Ok(Response::new(out_stream))
    }

    async fn do_action_add_plan(
        &self,
        action: Action,
    ) -> Result<Response<crate::flight::DoActionStream>, Status> {
        let task_data = DDTask::decode(action.body.as_ref()).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error decoding StageData: {e:?}",
                self.name
            ))
        })?;

        let addrs = task_data
            .stage_addrs
            .as_ref()
            .context("stage addrs not present")
            .map_err(DFRayError::from)
            .and_then(get_addrs)
            .map_err(|e| Status::internal(format!("{}, {e}", self.name)))?;

        self.add_task(
            task_data.query_id,
            task_data.stage_id,
            addrs,
            task_data.partition_group,
            task_data.full_partitions,
            &task_data.plan_bytes,
        )
        .await
        .map_err(|e| Status::internal(format!("{}, Could not add plan: {e:?}", self.name)))?;

        let out_stream = Box::pin(stream! {
            yield Ok::<_, tonic::Status>(arrow_flight::Result::default());
        }) as crate::flight::DoActionStream;

        Ok(Response::new(out_stream))
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

        let task_key = StageKey {
            query_id: ftd.query_id.clone(),
            stage_id: ftd.stage_id,
        };

        debug!(
            "{}, request for task_key:{:?} partition: {} from: {},{}",
            self.name, task_key, ftd.partition, ftd.requestor_name, remote_addr
        );

        let name = self.name.clone();
        let (ctx, plan, is_last_partition) = self
            .get_ctx_and_plan(&ftd.query_id, ftd.stage_id, ftd.partition)
            .map_err(|e| {
                Status::internal(format!(
                    "{name} Could not find task for query_id {} stage {} partition {}: {e:?}",
                    ftd.query_id, ftd.stage_id, ftd.partition
                ))
            })?;

        let do_get_stream = self
            .make_stream(ctx, plan, ftd.stage_id, ftd.partition, is_last_partition)
            .map_err(|e| {
                Status::internal(format!(
                    "{name} Could not make stream for query_id {} stage {} partition {}: {e:?}",
                    ftd.query_id, ftd.stage_id, ftd.partition
                ))
            })?;

        Ok(Response::new(do_get_stream))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<crate::flight::DoActionStream>, Status> {
        // extract a StageData protobuf from the action body
        let action = request.into_inner();
        let action_type = action.r#type.as_str();
        trace!("{} received action: {}", self.name, action_type);

        if action_type == "add_plan" {
            self.do_action_add_plan(action).await
        } else if action_type == "get_host" {
            self.do_action_get_host()
        } else {
            Err(Status::unimplemented(format!(
                "{}, Unimplemented action: {}",
                self.name, action_type
            )))
        }
    }
}

/// DFRayProcessorService is a Arrow Flight service that serves streams of
/// partitions from a hosted Physical Plan
///
/// It only responds to the DoGet Arrow Flight method
pub struct DFRayProcessorService {
    #[allow(dead_code)]
    name: String,
    listener: TcpListener,
    handler: Arc<DfRayProcessorHandler>,
    addr: String,
    all_done_tx: Arc<Mutex<Sender<()>>>,
    all_done_rx: Option<Receiver<()>>,
}

impl DFRayProcessorService {
    pub async fn new(name: String, port: usize) -> Result<Self> {
        let name = format!("[{}]", name);

        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        let listener = start_up(port).await?;

        let addr = format!("{}", listener.local_addr().unwrap());

        info!("DFRayProcessorService bound to {addr}");

        let handler = Arc::new(DfRayProcessorHandler::new(name.clone(), addr.clone()));

        Ok(Self {
            name,
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// get the address of the listing socket for this service
    pub fn addr(&self) -> String {
        self.addr.clone()
    }

    pub async fn all_done(&self) -> Result<()> {
        let sender = self.all_done_tx.lock().clone();

        sender
            .send(())
            .await
            .context("Could not send shutdown signal")?;
        Ok(())
    }

    /// start the service, consuming self
    pub async fn serve(mut self) -> Result<()> {
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
