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
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
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
    codec::DDCodec,
    customizer::Customizer,
    flight::{FlightHandler, FlightServ},
    logging::{debug, error, info, trace},
    planning::{add_ctx_extentions, get_ctx},
    protobuf::{
        AnnotatedTaskOutput, AnnotatedTaskOutputs, FlightDataMetadata, FlightTicketData, Host,
    },
    result::{DDError, Result},
    util::{
        bytes_to_physical_plan, display_plan_with_partition_counts, get_addrs,
        register_object_store_for_paths_in_plan, start_up,
    },
    vocab::{Addrs, CtxAnnotatedOutputs, CtxPartitionGroup, DDTask},
};

/// Unique identifier for a query stage, used as a key for task storage
/// Combines query ID and stage ID to uniquely identify which execution plan to run
#[derive(Eq, PartialEq, Hash, Clone, Debug)]
struct StageKey {
    /// Unique identifier for the entire distributed query
    query_id: String,
    /// Specific stage within the query (stages are executed in dependency order)
    stage_id: u64,
}

/// Represents an execution task assigned to this worker
/// Contains the execution plan and metadata needed to run a portion of a distributed query
#[derive(Clone)]
struct Task {
    /// Set of partition numbers this task is responsible for executing
    /// As partitions are consumed, they're removed from this set
    partitions: HashSet<u64>,
    /// DataFusion execution context with worker addresses and query metadata
    ctx: SessionContext,
    /// Physical execution plan that this worker will execute
    plan: Arc<dyn ExecutionPlan>,
}

impl Debug for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Task")
            .field("partitions", &self.partitions)
            .finish()
    }
}

/// Container for all tasks belonging to a specific stage
/// Tracks when tasks were added for cleanup purposes
#[derive(Debug)]
struct StageTasks {
    /// List of execution tasks for this stage (usually one per partition group)
    tasks: Vec<Task>,
    /// When these tasks were first added (for garbage collection)
    insert_time: SystemTime,
}

/// Worker node handler that executes distributed query stages
///
/// This is the core component of a DataFusion worker node. It receives execution
/// tasks from the proxy via Arrow Flight, stores them locally, and executes them
/// when the proxy requests results. Each worker can execute multiple stages from
/// different queries concurrently.
///
/// # Architecture
/// - Receives tasks via `do_action("add_plan")` from proxy during query planning
/// - Stores tasks in memory mapped by (query_id, stage_id)
/// - Executes tasks when proxy calls `do_get()` to fetch results
/// - Includes automatic cleanup of old/abandoned tasks
struct DDWorkerHandler {
    /// our name, useful for logging
    name: String,
    /// our address string, also useful for logging
    addr: String,
    /// our map of query_id -> (session ctx, execution plan)
    stages: Arc<RwLock<HashMap<StageKey, StageTasks>>>,
    done: Arc<Mutex<bool>>,

    /// Optional customizer for our context and proto serde
    #[allow(dead_code)]
    pub customizer: Option<Arc<dyn Customizer>>,

    codec: Arc<dyn PhysicalExtensionCodec>,
}

impl DDWorkerHandler {
    /// Creates a new worker handler with automatic task cleanup
    ///
    /// This constructor sets up a worker node that can receive and execute distributed
    /// query tasks. It automatically starts a background janitor thread that cleans up
    /// old or abandoned tasks to prevent memory leaks.
    ///
    /// # Arguments
    /// * `name` - Human-readable name for this worker (for logging/debugging)
    /// * `addr` - Network address where this worker can be reached
    /// * `customizer` - Optional customization for DataFusion context and serialization
    ///
    /// # Returns
    /// * `DDWorkerHandler` - Ready-to-use worker handler instance
    pub fn new(name: String, addr: String, customizer: Option<Arc<dyn Customizer>>) -> Self {
        // Initialize storage for execution tasks mapped by (query_id, stage_id)
        let stages: Arc<RwLock<HashMap<StageKey, StageTasks>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let done = Arc::new(Mutex::new(false));

        // Start a background janitor thread to clean up old or abandoned tasks
        // This prevents memory leaks from queries that never complete or are cancelled
        let c_stages = stages.clone();
        let c_done = done.clone();
        let c_name = name.clone();
        std::thread::spawn(move || {
            while !(*c_done.lock()) {
                // Check for cleanup every 10 seconds
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
                            // Remove tasks older than 1 minute (likely abandoned queries)
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

                // Remove the identified old tasks
                if !to_remove.is_empty() {
                    let mut _guard = c_stages.write();
                    for key in to_remove.iter() {
                        _guard.remove(key);
                        debug!("{} removed old stage key {:?}", c_name, key);
                    }
                }
            }
        });

        // Create codec for serializing/deserializing execution plans
        let codec = Arc::new(DDCodec::new(
            customizer
                .clone()
                .map(|c| c as Arc<dyn PhysicalExtensionCodec>)
                .unwrap_or(Arc::new(DefaultPhysicalExtensionCodec {})),
        ));

        Self {
            name,
            addr,
            stages,
            done,
            customizer,
            codec,
        }
    }

    /// Signals the worker to shut down gracefully
    ///
    /// This method stops the background janitor thread and prepares the worker
    /// for clean shutdown. Called when the worker service is being terminated.
    #[allow(dead_code)]
    pub fn all_done(&self) {
        *self.done.lock() = true;
    }

    /// Receives and stores an execution task from the proxy
    ///
    /// This method is called when the proxy distributes query stages to workers during
    /// the planning phase. It deserializes the execution plan, sets up the execution
    /// context, and stores the task for later execution when `do_get` is called.
    ///
    /// # Arguments
    /// * `query_id` - Unique identifier for the distributed query
    /// * `stage_id` - Specific stage ID within the query
    /// * `stage_addrs` - Worker addresses for all stages (for inter-stage communication)
    /// * `partition_group` - List of partitions assigned to this worker for this stage
    /// * `full_partitions` - Whether to use partition_group as-is or generate all partitions
    /// * `plan_bytes` - Serialized execution plan from the proxy
    ///
    /// # Returns
    /// * `Ok(())` if task was successfully stored
    /// * `Err()` if plan deserialization or context setup failed
    pub async fn add_task(
        &self,
        query_id: String,
        stage_id: u64,
        stage_addrs: Addrs,
        partition_group: Vec<u64>,
        full_partitions: bool,
        plan_bytes: &[u8],
    ) -> Result<()> {
        // Set up DataFusion execution context with distributed metadata
        let ctx = self
            .configure_ctx(
                query_id.clone(),
                stage_id,
                stage_addrs.clone(),
                partition_group.clone(),
            )
            .await?;

        // Deserialize the execution plan sent from the proxy
        let plan =
            bytes_to_physical_plan(&ctx, plan_bytes, self.codec.as_ref()).context(format!(
                "{}, Could not decode plan for query_id {} stage {}",
                self.name, query_id, stage_id
            ))?;

        // Determine which partitions this worker should execute
        let partitions = if full_partitions {
            // Use the specific partitions assigned by the proxy
            partition_group.clone()
        } else {
            // Generate all partition numbers based on the plan's output partitioning
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

        // Register any object stores referenced in the plan (e.g., S3, local files)
        register_object_store_for_paths_in_plan(&ctx, plan.clone())?;

        let now = SystemTime::now();

        // Create the task structure for storage
        let task = Task {
            ctx: ctx.clone(),
            plan: plan.clone(),
            partitions: HashSet::from_iter(partitions.clone()),
        };

        // Store the task mapped by (query_id, stage_id) for later execution
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

    /// Creates a configured DataFusion execution context for distributed execution
    ///
    /// This method sets up a SessionContext with all the metadata needed for this worker
    /// to execute its assigned tasks and communicate with other workers if needed.
    ///
    /// # Arguments
    /// * `query_id` - Unique identifier for the distributed query
    /// * `stage_id` - Specific stage ID being configured
    /// * `stage_addrs` - Addresses of all workers for inter-stage communication
    /// * `partition_group` - Partitions assigned to this worker
    ///
    /// # Returns
    /// * `SessionContext` - Configured context ready for plan execution
    async fn configure_ctx(
        &self,
        query_id: String,
        stage_id: u64,
        stage_addrs: Addrs,
        partition_group: Vec<u64>,
    ) -> Result<SessionContext> {
        // Start with default DataFusion session context
        let mut ctx = get_ctx()?;

        // Create host information for this worker
        let host = Host {
            addr: self.addr.clone(),
            name: self.name.clone(),
        };

        // Add distributed execution extensions to the context
        // These extensions allow execution plans to find worker addresses and metadata
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

    /// Retrieves the execution context and plan for a specific partition request
    ///
    /// This method is called when the proxy sends a `do_get` request to fetch results
    /// for a specific partition. It finds the appropriate task, marks the partition as
    /// consumed, and returns the execution context and plan needed to produce the results.
    ///
    /// # Arguments
    /// * `query_id` - Unique identifier for the distributed query
    /// * `stage_id` - Specific stage within the query
    /// * `partition` - Partition number being requested
    ///
    /// # Returns
    /// * `(SessionContext, ExecutionPlan, bool)` - Context, plan, and whether this is the last partition
    ///
    /// # Behavior
    /// - Marks the requested partition as consumed (removes it from available partitions)
    /// - Cleans up empty tasks and stages automatically
    /// - Returns whether this is the last partition for proper stream finalization
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

            // Find the stage tasks for this query and stage
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

            // Find the specific task that contains the requested partition
            let task = stage_tasks
                .tasks
                .iter_mut()
                .find(|t| t.partitions.contains(&partition))
                .context(format!(
                    "{}, No task found for stage key {:?} partition {}",
                    self.name, stage_key, partition
                ))?;

            // Mark this partition as consumed by removing it from the available set
            if !task.partitions.remove(&partition) {
                // This should never happen since we just filtered for this partition
                return Err(anyhow!("UNEXPECTED: partition {partition} not in plan parts").into());
            }

            // Return the execution context, plan, and whether this was the last partition
            (
                task.ctx.clone(),
                task.plan.clone(),
                task.partitions.is_empty(),
            )
        };

        // Cleanup: Remove empty tasks and stages to free memory
        {
            let mut _guard = self.stages.write();

            let remove_stage = _guard.get_mut(&stage_key).map(|stage_tasks| {
                // Remove any tasks that have no remaining partitions
                stage_tasks.tasks.retain(|t| !t.partitions.is_empty());
                trace!(
                    "remaining tasks: for stage {:?}: {:?}",
                    stage_key,
                    stage_tasks
                );

                // Check if this stage has no more tasks left
                stage_tasks.tasks.is_empty()
            });

            // Remove the entire stage if it has no more tasks
            if remove_stage.unwrap_or(false) {
                _guard.remove(&stage_key);
                debug!("{} removed stage key {:?}", self.name, stage_key);
            }
        }

        Ok((ctx, plan, last))
    }

    /// Creates a streaming response for query execution results
    ///
    /// This method executes the assigned plan partition and streams the results back to the proxy.
    /// It handles both regular data streaming and metadata collection for EXPLAIN ANALYZE queries.
    /// When streaming the last partition, it includes execution metrics and analysis data.
    ///
    /// # Arguments
    /// * `ctx` - DataFusion execution context with distributed metadata
    /// * `plan` - Physical execution plan to execute
    /// * `stage_id` - ID of the stage being executed
    /// * `partition` - Specific partition number to execute
    /// * `last_partition` - Whether this is the last partition (for metadata finalization)
    ///
    /// # Returns
    /// * `DoGetStream` - Arrow Flight stream containing result data and optional metadata
    ///
    /// # Stream Behavior
    /// - Streams result batches as they're produced
    /// - For the last partition, appends execution metadata and analysis results
    /// - Handles errors gracefully by converting them to Flight status messages
    fn make_stream(
        &self,
        ctx: SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        stage_id: u64,
        partition: u64,
        last_partition: bool,
    ) -> Result<crate::flight::DoGetStream> {
        let task_ctx = ctx.task_ctx();

        // Execute the physical plan to get a stream of RecordBatches
        // This is where the actual query computation happens on the worker!
        let stream = plan
            .execute(partition as usize, task_ctx)
            .inspect_err(|e| error!("Could not get partition stream from plan {e:#?}"))?
            .inspect(|batch| {
                trace!("producing maybe batch {:?}", batch);
            })
            .map_err(|e| FlightError::from_external_error(Box::new(e)));

        info!("{} tasks held {}", self.name, self.stages.read().len());

        // Helper function to find DistributedAnalyzeExec in the plan tree
        // This is used for EXPLAIN ANALYZE queries to collect execution metrics
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

        // Convert RecordBatch stream to Arrow Flight format
        let mut flight_data_stream = FlightDataEncoderBuilder::new().build(stream);
        let name = self.name.clone();
        let host = Host {
            addr: self.addr.clone(),
            name: self.name.clone(),
        };

        // Create the main streaming response
        #[allow(unused_assignments)] // clippy can't understand our assignment to done in the macro
        let out_stream = async_stream::stream! {
            let mut done = false;
            while !done {

                match (done, last_partition, flight_data_stream.next().await) {
                    // Finished a non-final partition - stop streaming
                    (false, false, None) => {
                        done = true;
                    }
                    // Finished the final partition - send metadata before stopping
                    (false, true, None) => {
                        debug!("stream exhausted, yielding FlightDataMetadata");

                        // Collect execution analysis outputs from context extensions
                        let task_outputs = ctx.state().config()
                            .get_extension::<CtxAnnotatedOutputs>()
                            .unwrap_or(Arc::new(CtxAnnotatedOutputs::default()))
                            .0
                            .clone();

                        let partition_group = ctx.state().config()
                            .get_extension::<CtxPartitionGroup>()
                            .expect("CtxPartitionGroup to be set")
                            .0.clone();

                        // If this is an EXPLAIN ANALYZE query, collect the annotated plan
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

                        // Create metadata containing execution analysis results
                        let meta = FlightDataMetadata {
                            annotated_task_outputs: Some(AnnotatedTaskOutputs {
                                outputs: task_outputs.lock().clone(),
                            }),
                        };

                        // Create a dummy batch to carry the metadata
                        let fake_batch = RecordBatch::new_empty(plan.schema());

                        // Encode the metadata as a Flight message
                        let mut fde = FlightDataEncoderBuilder::new()
                            .with_metadata(meta.encode_to_vec().into())
                            .build(futures::stream::once(async {Ok(fake_batch)}))
                            .map_err(|e| {
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
                    // Error in the data stream - propagate the error
                    (false, _, Some(Err(e))) => {
                        yield Err(Status::internal(format!(
                            "Unexpected error getting flight data stream: {e:?}",
                        )));
                        done = true;
                    },
                    // Normal data batch - stream it to the proxy
                    (false, _, Some(Ok(flight_data))) => {
                        trace!("received normal flight data, yielding");
                        yield Ok(flight_data);
                    },
                    // Already finished a non-final partition - do nothing
                    (true, false, _ ) => {
                        done = true;
                    },
                    // Unexpected state - should not happen
                    (true, true, _ ) => {
                        yield Err(Status::internal(format!("{name} reached expected unreachable block")));
                    },
                }
            }
        };

        Ok(Box::pin(out_stream))
    }
    /// Handles requests for worker host information
    ///
    /// This action is used by the proxy during worker discovery to get the host
    /// details (name and address) from this worker. It's part of the distributed
    /// system's service discovery mechanism.
    ///
    /// # Returns
    /// * `DoActionStream` - Stream containing this worker's host information
    #[allow(clippy::result_large_err)]
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

    /// Handles requests to add execution tasks to this worker
    ///
    /// This is the main method called by the proxy during query planning to distribute
    /// execution stages to workers. It receives a serialized DDTask containing the
    /// execution plan and metadata, then stores it for later execution.
    ///
    /// # Arguments
    /// * `action` - Arrow Flight action containing serialized DDTask data
    ///
    /// # Returns
    /// * `DoActionStream` - Empty success response stream
    /// * `Err(Status)` - If task deserialization or storage fails
    async fn do_action_add_plan(
        &self,
        action: Action,
    ) -> Result<Response<crate::flight::DoActionStream>, Status> {
        // Deserialize the task data sent from the proxy
        let task_data = DDTask::decode(action.body.as_ref()).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error decoding StageData: {e:?}",
                self.name
            ))
        })?;

        // Extract worker addresses for inter-stage communication
        let addrs = task_data
            .stage_addrs
            .as_ref()
            .context("stage addrs not present")
            .map_err(DDError::from)
            .and_then(get_addrs)
            .map_err(|e| Status::internal(format!("{}, {e}", self.name)))?;

        // Store the task for later execution when proxy calls do_get
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

        // Return empty success response
        let out_stream = Box::pin(stream! {
            yield Ok::<_, tonic::Status>(arrow_flight::Result::default());
        }) as crate::flight::DoActionStream;

        Ok(Response::new(out_stream))
    }
}

#[async_trait]
impl FlightHandler for DDWorkerHandler {
    /// Handles data retrieval requests from the proxy
    ///
    /// This is the main execution method called when the proxy wants to fetch results
    /// from a specific partition of a previously stored task. It's the worker-side
    /// counterpart to DDStageReaderExec.execute() on the proxy side.
    ///
    /// # Flow
    /// 1. Parse the ticket to extract query metadata
    /// 2. Look up the stored task using (query_id, stage_id)
    /// 3. Find the specific partition within that task
    /// 4. Execute the plan and stream results back to proxy
    ///
    /// # Arguments
    /// * `request` - Arrow Flight request containing ticket with execution metadata
    ///
    /// # Returns
    /// * `DoGetStream` - Streaming Arrow data containing the computed results
    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<crate::flight::DoGetStream>, Status> {
        // Extract client information for logging
        let remote_addr = request
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or("unknown".to_string());

        let ticket = request.into_inner();

        // Parse the ticket to extract execution metadata
        // The ticket was created by DDStageReaderExec on the proxy side
        let ftd = FlightTicketData::decode(ticket.ticket).map_err(|e| {
            Status::internal(format!(
                "{}, Unexpected error extracting ticket {e:?}",
                self.name
            ))
        })?;

        // Create lookup key for the stored task
        let task_key = StageKey {
            query_id: ftd.query_id.clone(),
            stage_id: ftd.stage_id,
        };

        debug!(
            "{}, request for task_key:{:?} partition: {} from: {},{}",
            self.name, task_key, ftd.partition, ftd.requestor_name, remote_addr
        );

        // Look up the previously stored task and mark the partition as consumed
        let name = self.name.clone();
        let (ctx, plan, is_last_partition) = self
            .get_ctx_and_plan(&ftd.query_id, ftd.stage_id, ftd.partition)
            .map_err(|e| {
                Status::internal(format!(
                    "{name} Could not find task for query_id {} stage {} partition {}: {e:?}",
                    ftd.query_id, ftd.stage_id, ftd.partition
                ))
            })?;

        // Execute the physical plan and return streaming results
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

    /// Routes action requests to appropriate handlers
    ///
    /// This method handles different types of administrative actions sent by the proxy:
    /// - "add_plan": Store an execution task for later execution
    /// - "get_host": Return this worker's host information for service discovery
    ///
    /// # Arguments
    /// * `request` - Arrow Flight action request with type and payload
    ///
    /// # Returns
    /// * `DoActionStream` - Response stream (content varies by action type)
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<crate::flight::DoActionStream>, Status> {
        // Extract action type and route to appropriate handler
        let action = request.into_inner();
        let action_type = action.r#type.as_str();
        trace!("{} received action: {}", self.name, action_type);

        match action_type {
            // Store execution task from proxy (during query planning phase)
            "add_plan" => self.do_action_add_plan(action).await,
            // Return host information (during worker discovery phase)
            "get_host" => self.do_action_get_host(),
            // Unknown action type
            _ => Err(Status::unimplemented(format!(
                "{}, Unimplemented action: {}",
                self.name, action_type
            ))),
        }
    }
}

/// DataFusion distributed worker service
///
/// This service provides the complete Arrow Flight server for a DataFusion worker node.
/// It handles the full lifecycle of distributed query execution on the worker side:
/// - Receives execution tasks from the proxy during query planning
/// - Stores tasks in memory until requested
/// - Executes tasks and streams results back when requested
/// - Provides service discovery information to the proxy
///
/// # Architecture
/// The service wraps a DDWorkerHandler and provides the network server infrastructure.
/// It uses Arrow Flight protocol for all communication with the proxy and supports
/// graceful shutdown signaling.
pub struct DDWorkerService {
    #[allow(dead_code)]
    /// Human-readable name for this worker service
    name: String,
    /// TCP listener for incoming Arrow Flight connections
    listener: TcpListener,
    /// Core worker handler that processes requests
    handler: Arc<DDWorkerHandler>,
    /// Network address where this service is listening
    addr: String,
    /// Channel sender for shutdown signaling
    all_done_tx: Arc<Mutex<Sender<()>>>,
    /// Channel receiver for shutdown signaling (taken during serve())
    all_done_rx: Option<Receiver<()>>,
}

impl DDWorkerService {
    /// Creates a new worker service bound to the specified port
    ///
    /// This constructor sets up the complete worker service infrastructure:
    /// - Binds to a TCP port for Arrow Flight connections
    /// - Creates the worker handler with automatic task cleanup
    /// - Sets up graceful shutdown channels
    ///
    /// # Arguments
    /// * `name` - Human-readable name for this worker (for logging)
    /// * `port` - TCP port to bind to (0 for automatic port assignment)
    /// * `customizer` - Optional customization for DataFusion context and serialization
    ///
    /// # Returns
    /// * `DDWorkerService` - Ready-to-serve worker service instance
    /// * `Err()` - If port binding or setup fails
    pub async fn new(
        name: String,
        port: usize,
        customizer: Option<Arc<dyn Customizer>>,
    ) -> Result<Self> {
        let name = format!("[{}]", name);

        // Set up shutdown signaling channels
        let (all_done_tx, all_done_rx) = channel(1);
        let all_done_tx = Arc::new(Mutex::new(all_done_tx));

        // Bind to TCP port for Arrow Flight connections
        let listener = start_up(port).await?;

        let addr = format!("{}", listener.local_addr().unwrap());

        info!("DDWorkerService bound to {addr}");

        // Create the core worker handler
        let handler = Arc::new(DDWorkerHandler::new(name.clone(), addr.clone(), customizer));

        Ok(Self {
            name,
            listener,
            handler,
            addr,
            all_done_tx,
            all_done_rx: Some(all_done_rx),
        })
    }

    /// Returns the network address where this service is listening
    ///
    /// This address can be used by the proxy to connect to this worker.
    /// Useful for service registration and discovery.
    ///
    /// # Returns
    /// * `String` - Network address in "host:port" format
    pub fn addr(&self) -> String {
        self.addr.clone()
    }

    /// Signals the worker service to shut down gracefully
    ///
    /// This method sends a shutdown signal that will cause the serve() method
    /// to complete and shut down the server gracefully. This allows ongoing
    /// requests to complete before termination.
    ///
    /// # Returns
    /// * `Ok(())` - If shutdown signal was sent successfully
    /// * `Err()` - If the shutdown channel was closed
    pub async fn all_done(&self) -> Result<()> {
        let sender = self.all_done_tx.lock().clone();

        sender
            .send(())
            .await
            .context("Could not send shutdown signal")?;
        Ok(())
    }

    /// Starts the worker service and serves Arrow Flight requests
    ///
    /// This method consumes the service and runs the main server loop.
    /// It serves Arrow Flight requests until a shutdown signal is received
    /// via the all_done() method. The service handles both do_get requests
    /// (for data retrieval) and do_action requests (for task management).
    ///
    /// # Returns
    /// * `Ok(())` - When service shuts down gracefully
    /// * `Err()` - If server startup or serving fails
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

        // Create Arrow Flight service with our worker handler
        let flight_serv = FlightServ {
            handler: self.handler.clone(),
        };

        let svc = FlightServiceServer::new(flight_serv);

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
