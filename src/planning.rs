use std::{
    collections::HashMap,
    env,
    sync::{Arc, LazyLock},
};

use anyhow::{Context, anyhow};
use arrow_flight::Action;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::{
        file_format::{FileFormat, csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    execution::{SessionState, SessionStateBuilder},
    logical_expr::LogicalPlan,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan,
        ExecutionPlanProperties,
        coalesce_batches::CoalesceBatchesExec,
        displayable,
        joins::NestedLoopJoinExec,
        repartition::RepartitionExec,
        sorts::sort::SortExec,
    },
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
// DataDog-specific table functions removed for open source version
use futures::TryStreamExt;
use itertools::Itertools;
use prost::Message;

use crate::{
    isolator::PartitionIsolatorExec,
    logging::{debug, error, info, trace},
    max_rows::MaxRowsExec,
    physical::DFRayStageOptimizerRule,
    protobuf::{Host, Hosts, PartitionAddrs, StageAddrs, StageData},
    result::{DFRayError, Result},
    stage::DFRayStageExec,
    stage_reader::{DFRayStageReaderExec, QueryId},
    util::{display_plan_with_partition_counts, get_client, physical_plan_to_bytes, wait_for},
    vocab::{Addrs, CtxName, CtxPartitionGroup, CtxStageAddrs},
};

pub struct DFRayStage {
    /// our stage id
    pub stage_id: u64,
    /// the physical plan of our stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// the partition groups for this stage.
    pub partition_groups: Vec<Vec<u64>>,
    /// Are we hosting the complete partitions?  If not
    /// then RayStageReaderExecs will be inserted to consume its desired
    /// partition from all stages with this same id, and merge the results.
    /// Using a CombinedRecordBatchStream
    pub full_partitions: bool,
}

impl DFRayStage {
    fn new(
        stage_id: u64,
        plan: Arc<dyn ExecutionPlan>,
        partition_groups: Vec<Vec<u64>>,
        full_partitions: bool,
    ) -> Self {
        Self {
            stage_id,
            plan,
            partition_groups,
            full_partitions,
        }
    }

    pub fn child_stage_ids(&self) -> Result<Vec<u64>> {
        let mut result = vec![];
        self.plan
            .clone()
            .transform_down(|node: Arc<dyn ExecutionPlan>| {
                if let Some(reader) = node.as_any().downcast_ref::<DFRayStageReaderExec>() {
                    result.push(reader.stage_id);
                }
                Ok(Transformed::no(node))
            })?;
        Ok(result)
    }
}

static STATE: LazyLock<Result<SessionState>> = LazyLock::new(|| {
    let wait_result = wait_for(make_state(), "make_state");
    match wait_result {
        Ok(Ok(state)) => Ok(state),
        Ok(Err(e)) => Err(anyhow!("Failed to initialize state: {}", e).into()),
        Err(e) => Err(anyhow!("Failed to initialize state: {}", e).into()),
    }
});

pub fn get_ctx() -> Result<SessionContext> {
    match &*STATE {
        Ok(ctx) => Ok(SessionContext::new_with_state(ctx.clone())),
        Err(e) => Err(anyhow!("Context initialization failed: {}", e).into()),
    }
}

async fn make_state() -> Result<SessionState> {
    let mut config = SessionConfig::default().with_information_schema(true);

    // these are important as we only support partitioned hash joins at the moment
    // so we must change the thresholds so the planner does not try to use
    // a collect left join for example
    let options = config.options_mut();
    options.set(
        "datafusion.optimizer.hash_join_single_partition_threshold",
        "0",
    )?;
    options.set(
        "datafusion.optimizer.hash_join_single_partition_threshold_rows",
        "0",
    )?;

    // tmp
    options.set("datafusion.execution.target_partitions", "3")?;

    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .build();

    // DataDog-specific table functions registration removed for open source version

    add_tables_from_env(&mut state)
        .await
        .context("Failed to add tables from environment")?;

    Ok(state)
}

pub fn add_ctx_extentions(
    ctx: &mut SessionContext,
    ctx_name: &str,
    query_id: &str,
    stage_addrs: Addrs,
    partition_group: Option<Vec<u64>>,
) -> Result<()> {
    let state = ctx.state_ref();
    let mut guard = state.write();
    let config = guard.config_mut();

    config.set_extension(Arc::new(CtxStageAddrs(stage_addrs)));
    config.set_extension(Arc::new(QueryId(query_id.to_owned())));
    config.set_extension(Arc::new(CtxName(ctx_name.to_owned())));

    if let Some(pg) = partition_group {
        // this only matters if the plan includes an PartitionIsolatorExec, which looks
        // for this for this extension and will be ignored otherwise

        trace!("Adding partition group: {:?}", pg);
        config.set_extension(Arc::new(CtxPartitionGroup(pg)));
    }
    Ok(())
}

pub async fn add_tables_from_env(state: &mut SessionState) -> Result<()> {
    // this string is formatted as a comman separated list of table info
    // where each table info is name:format:path
    let table_str = env::var("DFRAY_TABLES");
    if table_str.is_err() {
        info!("No DFRAY_TABLES environment variable set, skipping table registration");
        return Ok(());
    }

    for table in table_str.unwrap().split(',') {
        info!("adding table from env: {}", table);
        let parts: Vec<&str> = table.split(':').collect();
        if parts.len() != 3 {
            return Err(anyhow!("Invalid format for DFRAY_TABLES env var: {}", table).into());
        }
        let name = parts[0].to_string();
        let fmt = parts[1].to_string();
        let path = parts[2].to_string();

        let format: Arc<dyn FileFormat> = match fmt.as_str() {
            "parquet" => Arc::new(ParquetFormat::default()),
            "csv" => Arc::new(CsvFormat::default()),
            "json" => Arc::new(JsonFormat::default()),
            _ => {
                return Err(anyhow!(
                    "Unsupported format: {}. Supported formats are: parquet, csv, json",
                    fmt
                )
                .into());
            }
        };

        let options = ListingOptions::new(format);

        let table_path = ListingTableUrl::parse(path)?;
        let resolved_schema = options.infer_schema(state, &table_path).await?;
        let config = ListingTableConfig::new(table_path)
            .with_listing_options(options)
            .with_schema(resolved_schema);
        let table = Arc::new(ListingTable::try_new(config)?);

        state
            .schema_for_ref(name.clone())?
            .register_table(name, table)?;
    }

    Ok(())
}

pub async fn logical_planning(sql: &str, ctx: &SessionContext) -> Result<LogicalPlan> {
    let options = SQLOptions::new();
    let plan = ctx.state().create_logical_plan(sql).await?;

    debug!("Logical plan:\n{}", plan.display_indent());
    options.verify_plan(&plan)?;

    let plan = ctx.state().optimize(&plan)?;

    debug!("Optimized Logical plan:\n{}", plan.display_indent());
    Ok(plan)
}

pub async fn physical_planning(
    logical_plan: &LogicalPlan,
    ctx: &SessionContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let physical_plan = ctx
        .state()
        .create_physical_plan(logical_plan)
        .await
        .context("Failed to create physical plan")?;

    debug!(
        "Physical plan:\n{}",
        displayable(physical_plan.as_ref()).indent(false)
    );
    Ok(physical_plan)
}

pub async fn execution_planning(
    physical_plan: Arc<dyn ExecutionPlan>,
    batch_size: usize,
    partitions_per_worker: Option<usize>,
) -> Result<Vec<DFRayStage>> {
    let mut stages = vec![];

    let mut partition_groups = vec![];
    let mut full_partitions = false;
    // We walk up the tree from the leaves to find the stages, record ray stages,
    // and replace each ray stage with a corresponding ray reader stage.
    //
    // we also calculate the paritition groups at each node, anticipating
    // arriving at a RayStageExec that we have to replace with a stage reader
    // which will have differing requirements for partition groups depending on
    // its children.
    let up = |plan: Arc<dyn ExecutionPlan>| {
        trace!(
            "Examining plan up: {}",
            displayable(plan.as_ref()).one_line()
        );

        if let Some(stage_exec) = plan.as_any().downcast_ref::<DFRayStageExec>() {
            trace!("ray stage exec. partition_groups: {:?}", partition_groups);
            let input = plan.children();
            assert!(input.len() == 1, "RayStageExec must have exactly one child");
            let input = input[0];

            let replacement = Arc::new(DFRayStageReaderExec::try_new(
                plan.output_partitioning().clone(),
                input.schema(),
                stage_exec.stage_id,
            )?) as Arc<dyn ExecutionPlan>;

            let stage = DFRayStage::new(
                stage_exec.stage_id,
                input.clone(),
                partition_groups.clone(),
                full_partitions,
            );
            full_partitions = false;

            stages.push(stage);
            Ok(Transformed::yes(replacement))
        } else if plan.as_any().downcast_ref::<RepartitionExec>().is_some() {
            trace!("repartition exec partition_groups: {:?}", partition_groups);
            let (calculated_partition_groups, replacement) =
                build_replacement(plan, partitions_per_worker, true, batch_size, batch_size)?;
            partition_groups = calculated_partition_groups;
            full_partitions = false;

            Ok(Transformed::yes(replacement))
        } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
            trace!("sort exec partition_groups: {:?}", partition_groups);
            let (calculated_partition_groups, replacement) =
                build_replacement(plan, partitions_per_worker, false, batch_size, batch_size)?;
            partition_groups = calculated_partition_groups;
            full_partitions = true;

            Ok(Transformed::yes(replacement))
        } else if plan.as_any().downcast_ref::<NestedLoopJoinExec>().is_some() {
            trace!(
                "nested loop join exec partition_groups: {:?}",
                partition_groups
            );
            // NestedLoopJoinExec must be on a stage by itself as it materializes the entire
            // left side of the join and is not suitable to be executed in a
            // partitioned manner.
            let mut replacement = plan.clone();
            let partition_count = plan.output_partitioning().partition_count() as u64;
            trace!("nested join output partitioning {}", partition_count);

            replacement = Arc::new(MaxRowsExec::new(
                Arc::new(CoalesceBatchesExec::new(replacement, batch_size))
                    as Arc<dyn ExecutionPlan>,
                batch_size,
            )) as Arc<dyn ExecutionPlan>;

            partition_groups = vec![(0..partition_count).collect()];
            full_partitions = true;
            Ok(Transformed::yes(replacement))
        } else {
            trace!("not special case partition_groups: {:?}", partition_groups);

            let partition_count = plan.output_partitioning().partition_count() as u64;
            // set this back to default
            partition_groups = vec![(0..partition_count).collect()];

            Ok(Transformed::no(plan))
        }
    };

    // walk up the plan adding DFRayStageExec marker nodes.
    // FIXME: we can do this in one step in the future i think but this is
    // a carry over from when it was done in a physical optimizer seperate
    // step
    let optimizer = DFRayStageOptimizerRule::new();
    let physical_plan = optimizer.optimize(physical_plan, &ConfigOptions::default())?;

    physical_plan.transform_up(up)?;

    // add coalesce and max rows to last stage
    let mut last_stage = stages.pop().ok_or(anyhow!("No stages found"))?;

    last_stage = DFRayStage::new(
        last_stage.stage_id,
        Arc::new(MaxRowsExec::new(
            Arc::new(CoalesceBatchesExec::new(last_stage.plan, batch_size))
                as Arc<dyn ExecutionPlan>,
            batch_size,
        )) as Arc<dyn ExecutionPlan>,
        partition_groups,
        full_partitions,
    );

    // done fixing last stage, put it back
    stages.push(last_stage);

    let txt = stages
        .iter()
        .map(|stage| format!("{}", display_plan_with_partition_counts(&stage.plan)))
        .join(",\n");
    trace!("stages:{}", txt);

    Ok(stages)
}

/// Distribute the stages to the workers, assigning each stage to a worker
/// Returns an Addrs containing the addresses of the workers that will execute
/// final stage only as that's all we care about from the call site
pub async fn distribute_stages(
    query_id: &str,
    stages: Vec<DFRayStage>,
    worker_addrs: Vec<(String, String)>,
) -> Result<Addrs> {
    // map of worker name to address
    // FIXME: use types over tuples of strings, as we can accidently swap them and
    // not know

    let mut workers: HashMap<String, String> = worker_addrs.iter().cloned().collect();

    for attempt in 0..3 {
        // all stages to workers
        let (stage_datas, final_addrs) =
            assign_to_workers(query_id, &stages, workers.iter().collect())?;

        // we retry this a few times to ensure that the workers are ready
        // and can accept the stages
        match try_distribute_stages(&stage_datas).await {
            Ok(_) => return Ok(final_addrs),
            Err(DFRayError::WorkerCommunicationError(bad_worker)) => {
                error!(
                    "distribute stages for query {query_id} attempt {attempt} failed removing \
                     worker {bad_worker}. Retrying..."
                );
                // if we cannot communicate with a worker, we remove it from the list of workers
                workers.remove(&bad_worker);
            }
            Err(e) => return Err(e),
        }
        if attempt == 2 {
            return Err(
                anyhow!("Failed to distribute query {query_id} stages after 3 attempts").into(),
            );
        }
    }
    unreachable!()
}

/// try to distribute the stages to the workers, if we cannot communicate with a
/// worker return it as the element in the Err
async fn try_distribute_stages(stage_datas: &[StageData]) -> Result<()> {
    // we can use the stage data to distribute the stages to workers
    for stage_data in stage_datas {
        trace!(
            "Distributing stage_id {}, pg: {:?} to worker: {:?}",
            stage_data.stage_id, stage_data.partition_group, stage_data.assigned_addr
        );

        // populate its child stages
        let mut stage_data = stage_data.clone();
        stage_data.stage_addrs = Some(get_stage_addrs_from_stages(
            &stage_data.child_stage_ids,
            stage_datas,
        )?);

        let mut client = match get_client(
            &stage_data
                .assigned_addr
                .as_ref()
                .context("Assigned stage address is missing")?
                .name,
            &stage_data
                .assigned_addr
                .as_ref()
                .context("Assigned stage address is missing")?
                .addr,
        ) {
            Ok(client) => client,
            Err(e) => {
                error!("Couldn't not communicate with worker {e:#?}");
                return Err(DFRayError::WorkerCommunicationError(
                    stage_data
                        .assigned_addr
                        .as_ref()
                        .cloned()
                        .unwrap() // we know we can unwrap it as we checked a few lines up
                        .name,
                ));
            }
        };

        let mut buf = vec![];
        stage_data
            .encode(&mut buf)
            .context("Failed to encode stage data to buf")?;

        let action = Action {
            r#type: "add_plan".to_string(),
            body: buf.into(),
        };

        let mut response = client
            .do_action(action)
            .await
            .context("Failed to send action to worker")?;

        // consume this empty response to ensure the action was successful
        while let Some(_res) = response
            .try_next()
            .await
            .context("error consuming do_action response")?
        {
            // we don't care about the response, just that it was successful
        }
        trace!("do action success for stage_id: {}", stage_data.stage_id);
    }
    Ok(())
}

// go through our stages, and further divide them into their partition
// groups and produce a StageData for each partition group and assign it
// to a worker
fn assign_to_workers(
    query_id: &str,
    stages: &[DFRayStage],
    worker_addrs: Vec<(&String, &String)>,
) -> Result<(Vec<StageData>, Addrs)> {
    let mut stage_datas = vec![];
    let mut worker_idx = 0;

    // keep track of which worker has the root of the plan tree (highest stage
    // number)
    let mut max_stage_id = -1;
    let mut final_addrs = Addrs::default();

    for stage in stages {
        for partition_group in stage.partition_groups.iter() {
            let plan_bytes = physical_plan_to_bytes(stage.plan.clone())?;

            let addr = Host {
                name: worker_addrs[worker_idx].0.to_string(),
                addr: worker_addrs[worker_idx].1.to_string(),
            };
            worker_idx = (worker_idx + 1) % worker_addrs.len();

            if stage.stage_id as isize > max_stage_id {
                // this wasn't the last stage
                max_stage_id = stage.stage_id as isize;
                final_addrs.clear();
            }
            if stage.stage_id as isize == max_stage_id {
                for part in partition_group.iter() {
                    // we are the final stage, so we will be the one to serve this partition
                    final_addrs
                        .entry(stage.stage_id)
                        .or_default()
                        .entry(*part)
                        .or_default()
                        .push((addr.name.clone(), addr.addr.clone()));
                }
            }

            let stage_data = StageData {
                query_id: query_id.to_string(),
                stage_id: stage.stage_id,
                plan_bytes,
                partition_group: partition_group.to_vec(),
                child_stage_ids: stage.child_stage_ids().unwrap_or_default().to_vec(),
                stage_addrs: None, // will be calculated and filled in later
                num_output_partitions: stage.plan.output_partitioning().partition_count() as u64,
                full_partitions: stage.full_partitions,
                assigned_addr: Some(addr),
            };
            stage_datas.push(stage_data);
        }
    }

    Ok((stage_datas, final_addrs))
}

fn get_stage_addrs_from_stages(
    target_stage_ids: &[u64],
    stages: &[StageData],
) -> Result<StageAddrs> {
    let mut stage_addrs = StageAddrs::default();

    // this can be more efficient

    trace!(
        "getting stage addresses for target stages: {:?}",
        target_stage_ids
    );

    for &stage_id in target_stage_ids {
        let mut partition_addrs = PartitionAddrs::default();
        for stage in stages {
            if stage.stage_id == stage_id {
                for part in stage.partition_group.iter() {
                    if !stage.full_partitions {
                        // we have to fan out to read this partition
                        let hosts = stages
                            .iter()
                            .filter(|s| s.stage_id == stage_id)
                            .map(|s| {
                                Ok(Host {
                                    name: s
                                        .assigned_addr
                                        .clone()
                                        .context("assigned address missing")?
                                        .name,
                                    addr: s
                                        .assigned_addr
                                        .clone()
                                        .context("assigned address missing")?
                                        .addr,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        partition_addrs
                            .partition_addrs
                            .insert(*part, Hosts { hosts });
                    } else {
                        // we have the full partition, so this is the only address required
                        let host = Host {
                            name: stage
                                .assigned_addr
                                .clone()
                                .context("Assigned address is missing")?
                                .name,
                            addr: stage
                                .assigned_addr
                                .clone()
                                .context("Assigned address is missing")?
                                .addr,
                        };
                        partition_addrs
                            .partition_addrs
                            .insert(*part, Hosts { hosts: vec![host] });
                    }
                }
            }
        }
        stage_addrs.stage_addrs.insert(stage_id, partition_addrs);
    }
    trace!("returned stage_addrs: {:?}", stage_addrs);

    Ok(stage_addrs)
}

#[allow(clippy::type_complexity)]
fn build_replacement(
    plan: Arc<dyn ExecutionPlan>,
    partitions_per_worker: Option<usize>,
    isolate: bool,
    max_rows: usize,
    inner_batch_size: usize,
) -> Result<(Vec<Vec<u64>>, Arc<dyn ExecutionPlan>), DataFusionError> {
    let mut replacement = plan.clone();
    let children = plan.children();
    assert!(children.len() == 1, "Unexpected plan structure");

    let child = children[0];
    let partition_count = plan.output_partitioning().partition_count() as u64;
    trace!(
        "build_replacement for {}, partition_count: {}",
        displayable(plan.as_ref()).one_line(),
        partition_count
    );

    let partition_groups = match partitions_per_worker {
        Some(p) => (0..partition_count)
            .chunks(p)
            .into_iter()
            .map(|chunk| chunk.collect())
            .collect(),
        None => vec![(0..partition_count).collect()],
    };

    if isolate && partition_groups.len() > 1 {
        let new_child = Arc::new(PartitionIsolatorExec::new(
            child.clone(),
            partitions_per_worker.unwrap(), // we know it is a Some, here.
        ));
        replacement = replacement.clone().with_new_children(vec![new_child])?;
    }
    // insert a max rows & coalescing batches here too so that we aren't sending
    // too small (or too big) of batches over the network
    replacement = Arc::new(MaxRowsExec::new(
        Arc::new(CoalesceBatchesExec::new(replacement, inner_batch_size)) as Arc<dyn ExecutionPlan>,
        max_rows,
    )) as Arc<dyn ExecutionPlan>;

    Ok((partition_groups, replacement))
}
