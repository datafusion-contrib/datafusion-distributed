use std::{
    collections::HashMap, env, sync::{Arc, LazyLock}
};

use anyhow::{anyhow, Context};
use arrow_flight::Action;
use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::{
        file_format::{csv::CsvFormat, json::JsonFormat, parquet::ParquetFormat, FileFormat},
        listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    },
    error::DataFusionError,
    execution::{SessionState, SessionStateBuilder},
    logical_expr::LogicalPlan,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        analyze::AnalyzeExec, coalesce_batches::CoalesceBatchesExec, displayable,
        joins::NestedLoopJoinExec, repartition::RepartitionExec, sorts::sort::SortExec,
        ExecutionPlan, ExecutionPlanProperties,
    },
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::TryStreamExt;
use itertools::Itertools;
use prost::Message;

use crate::{
    analyze::{DistributedAnalyzeExec, DistributedAnalyzeRootExec}, isolator::PartitionIsolatorExec, logging::{debug, info, trace, error}, max_rows::MaxRowsExec, physical::DDStageOptimizerRule, result::{DDError, Result}, stage::DDStageExec, stage_reader::{DDStageReaderExec, QueryId}, transport::WorkerTransport, util::{display_plan_with_partition_counts, physical_plan_to_bytes, wait_for}, vocab::{
        Addrs, CtxAnnotatedOutputs, CtxHost, CtxPartitionGroup, CtxStageAddrs, CtxStageId, DDTask,
        Host, Hosts, PartitionAddrs, StageAddrs,
    }
};

#[derive(Debug)]
pub struct DDStage {
    /// our stage id
    pub stage_id: u64,
    /// the physical plan of our stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// the partition groups for this stage.
    pub partition_groups: Vec<Vec<u64>>,
    /// Are we hosting the complete partitions?  If not
    /// then DDStageReaderExecs will be inserted to consume its desired
    /// partition from all stages with this same id, and merge the results.
    /// Using a CombinedRecordBatchStream
    pub full_partitions: bool,
}

impl DDStage {
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
                if let Some(reader) = node.as_any().downcast_ref::<DDStageReaderExec>() {
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
        Ok(state) => Ok(SessionContext::new_with_state(state.clone())),
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

    add_tables_from_env(&mut state)
        .await
        .context("Failed to add tables from environment")?;

    Ok(state)
}

pub fn add_ctx_extentions(
    ctx: &mut SessionContext,
    host: &Host,
    query_id: &str,
    stage_id: u64,
    stage_addrs: Addrs,
    partition_group: Vec<u64>,
) -> Result<()> {
    let state = ctx.state_ref();
    let mut guard = state.write();
    let config = guard.config_mut();

    config.set_extension(Arc::new(CtxStageAddrs(stage_addrs)));
    config.set_extension(Arc::new(QueryId(query_id.to_owned())));
    config.set_extension(Arc::new(CtxHost(host.clone())));
    config.set_extension(Arc::new(CtxStageId(stage_id)));
    config.set_extension(Arc::new(CtxAnnotatedOutputs::default()));

    trace!("Adding partition group: {:?}", partition_group);
    config.set_extension(Arc::new(CtxPartitionGroup(partition_group)));
    Ok(())
}

pub async fn add_tables_from_env(state: &mut SessionState) -> Result<()> {
    // this string is formatted as a comman separated list of table info
    // where each table info is name:format:path
    let table_str = env::var("DD_TABLES");
    if table_str.is_err() {
        info!("No DD_TABLES environment variable set, skipping table registration");
        return Ok(());
    }

    for table in table_str.unwrap().split(',') {
        info!("adding table from env: {}", table);
        let parts: Vec<&str> = table.split(':').collect();
        if parts.len() != 3 {
            return Err(anyhow!("Invalid format for DD_TABLES env var: {}", table).into());
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

/// Returns distributed plan and execution stages for both query execution and EXPLAIN display
pub async fn execution_planning(
    physical_plan: Arc<dyn ExecutionPlan>,
    batch_size: usize,
    partitions_per_worker: Option<usize>,
) -> Result<(Arc<dyn ExecutionPlan>, Vec<DDStage>)> {
    let mut stages = vec![];

    let mut partition_groups = vec![];
    let mut full_partitions = false;
    // We walk up the tree from the leaves to find the stages
    // and replace each stage with a corresponding reader stage.
    //
    // we also calculate the paritition groups at each node, anticipating
    // arriving at a DDStageExec that we have to replace with a stage reader
    // which will have differing requirements for partition groups depending on
    // its children.
    let up = |plan: Arc<dyn ExecutionPlan>| {
        trace!(
            "Examining plan up: {}",
            displayable(plan.as_ref()).one_line()
        );

        if let Some(stage_exec) = plan.as_any().downcast_ref::<DDStageExec>() {
            trace!("stage exec. partition_groups: {:?}", partition_groups);
            let input = plan.children();
            assert!(input.len() == 1, "DDStageExec must have exactly one child");
            let input = input[0];

            let replacement = Arc::new(DDStageReaderExec::try_new(
                plan.output_partitioning().clone(),
                input.schema(),
                stage_exec.stage_id,
            )?) as Arc<dyn ExecutionPlan>;

            let stage = DDStage::new(
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

    // walk up the plan adding DDStageExec marker nodes.
    // FIXME: we can do this in one step in the future i think but this is
    // a carry over from when it was done in a physical optimizer seperate
    // step
    let optimizer = DDStageOptimizerRule::new();
    let distributed_plan = optimizer.optimize(physical_plan, &ConfigOptions::default())?;

    // Clone the distributed plan before transformation since we need to return it
    let distributed_plan_clone = Arc::clone(&distributed_plan);
    distributed_plan.transform_up(up)?;

    // add coalesce and max rows to last stage
    let mut last_stage = stages.pop().ok_or(anyhow!("No stages found"))?;

    last_stage = DDStage::new(
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

    if contains_analyze(stages[stages.len() - 1].plan.as_ref()) {
        // if the plan contains an analyze, we need to add the distributed analyze
        // stages to the plan
        add_distributed_analyze(&mut stages, false, false)?;
    }

    let txt = stages
        .iter()
        .map(|stage| format!("{}", display_plan_with_partition_counts(&stage.plan)))
        .join(",\n");
    trace!("stages:\n{}", txt);

    Ok((distributed_plan_clone, stages))
}

fn contains_analyze(plan: &dyn ExecutionPlan) -> bool {
    trace!(
        "checking stage for analyze: {}",
        displayable(plan).indent(false)
    );
    if plan.as_any().downcast_ref::<AnalyzeExec>().is_some() {
        true
    } else {
        for child in plan.children() {
            if contains_analyze(child.as_ref()) {
                return true;
            }
        }
        false
    }
}

pub fn add_distributed_analyze(
    stages: &mut [DDStage],
    verbose: bool,
    show_statistics: bool,
) -> Result<()> {
    trace!("Adding distributed analyze to stages");
    let len = stages.len();
    for (i, stage) in stages.iter_mut().enumerate() {
        if i == len - 1 {
            let plan_without_analyze = stage
                .plan
                .clone()
                .transform_down(|plan: Arc<dyn ExecutionPlan>| {
                    if let Some(analyze) = plan.as_any().downcast_ref::<AnalyzeExec>() {
                        Ok(Transformed::yes(analyze.input().clone()))
                    } else {
                        Ok(Transformed::no(plan))
                    }
                })?
                .data;

            trace!(
                "plan without analyze: {}",
                displayable(plan_without_analyze.as_ref()).indent(false)
            );
            stage.plan = Arc::new(DistributedAnalyzeRootExec::new(
                plan_without_analyze,
                verbose,
                show_statistics,
            )) as Arc<dyn ExecutionPlan>;
            stage.partition_groups = vec![vec![0]]; // accounting for coalesce
        } else {
            stage.plan = Arc::new(DistributedAnalyzeExec::new(
                stage.plan.clone(),
                verbose,
                show_statistics,
            )) as Arc<dyn ExecutionPlan>;
        }
    }
    Ok(())
}

/// Distribute the stages to the workers, assigning each stage to a worker
/// Returns an Addrs containing the addresses of the workers that will execute
/// final stage only as that's all we care about from the call site
pub async fn distribute_stages(
    query_id: &str,
    stages: Vec<DDStage>,
    workers: Vec<(Host, Arc<dyn WorkerTransport>)>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(Addrs, Vec<DDTask>)> {
    // materialise a name-keyed map so we can remove “bad” workers on each retry
    let mut valid_workers: HashMap<_, _> = workers
        .into_iter()
        .map(|(h, tx)| (h.name.clone(), (h, tx)))
        .collect();

    for attempt in 0..3 {
        if valid_workers.is_empty() {
            return Err(anyhow!("No workers available to distribute stages").into());
        }

        let current: Vec<_> = valid_workers.values().cloned().collect();
        let (tasks, final_addrs, tx_host_pairs) =
            assign_to_workers(query_id, &stages, current, codec)?;

        match try_distribute_tasks(&tasks, &tx_host_pairs).await {
            Ok(_) => return Ok((final_addrs, tasks)),

            // remove the poisoned worker and retry on the non poisoned workers
            Err(DDError::WorkerCommunicationError(bad_host)) => {
                error!(
                    "distribute_stages: attempt {attempt} – \
                     worker {} failed; will retry without it",
                    bad_host.name
                );
                valid_workers.remove(&bad_host.name);
            }

            // any other error is terminal
            Err(e) => return Err(e),
        }
    }

    unreachable!("retry loop exits on success or early return on error");
}

/// try to distribute the stages to the workers, if we cannot communicate with a
/// worker return it as the element in the Err
async fn try_distribute_tasks(
    tasks: &[DDTask],
    tx_host_pairs: &[(Arc<dyn WorkerTransport>, Host)],
) -> Result<()> {
    for ((task, (tx, host))) in tasks.iter().zip(tx_host_pairs) {
        trace!(
            "Sending stage {} / pg {:?} to {}",
            task.stage_id,
            task.partition_group,
            host
        );

        // embed the StageAddrs of all children before shipping
        let mut stage = task.clone();
        stage.stage_addrs = Some(get_stage_addrs_from_tasks(
            &stage.child_stage_ids,
            tasks,
        )?);

        let mut buf = Vec::new();
        stage.encode(&mut buf).map_err(anyhow::Error::from)?;

        let action = Action {
            r#type: "add_plan".into(),
            body: buf.into(),
        };

        // gRPC call, if it fails, transport poisons itself on failure and removes the address from the registry
        let mut stream = tx
            .do_action(action)
            .await
            .map_err(|_| DDError::WorkerCommunicationError(host.clone()))?;

        // drain the (empty) response – ensures the worker actually accepted it
        while stream.try_next().await? != None {}

        trace!("stage {} delivered to {}", stage.stage_id, host);
    }
    Ok(())
}

// go through our stages, and further divide them into their partition
// groups and produce a DDTask for each partition group and assign it
// to a worker
fn assign_to_workers(
    query_id: &str,
    stages: &[DDStage],
    workers: Vec<(Host, Arc<dyn WorkerTransport>)>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(Vec<DDTask>, Addrs, Vec<(Arc<dyn WorkerTransport>, Host)>)> {
    let mut task_datas   = Vec::new();
    let mut tx_host_pairs = Vec::new();

    // round-robin scheduler
    let mut idx = 0;
    let n_workers = workers.len();

    // keep track of where the root of the plan will live (highest stage id)
    let mut max_stage_id: i64 = -1;
    let mut final_addrs = Addrs::default();

    for stage in stages {
        for pg in &stage.partition_groups {
            let plan_bytes = physical_plan_to_bytes(stage.plan.clone(), codec)?;

            // pick next worker
            let (host, tx) = workers[idx].clone();
            idx = (idx + 1) % n_workers;

            // remember which host serves the final stage
            if stage.stage_id as i64 > max_stage_id {
                max_stage_id = stage.stage_id as i64;
                final_addrs.clear();
            }
            if stage.stage_id as i64 == max_stage_id {
                for part in pg {
                    final_addrs
                        .entry(stage.stage_id)
                        .or_default()
                        .entry(*part)
                        .or_default()
                        .push(host.clone());
                }
            }

            task_datas.push(DDTask {
                query_id: query_id.to_owned(),
                stage_id: stage.stage_id,
                plan_bytes,
                partition_group: pg.clone(),
                child_stage_ids: stage.child_stage_ids().unwrap_or_default(),
                stage_addrs: None, // filled in later
                num_output_partitions: stage.plan.output_partitioning().partition_count() as u64,
                full_partitions: stage.full_partitions,
                assigned_host: Some(host.clone()),
            });

            // keep the order **exactly** aligned with task_datas
            tx_host_pairs.push((tx, host));
        }
    }

    Ok((task_datas, final_addrs, tx_host_pairs))
}

fn get_stage_addrs_from_tasks(target_stage_ids: &[u64], stages: &[DDTask]) -> Result<StageAddrs> {
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
                                s.assigned_host.clone().ok_or_else(|| {
                                    anyhow!(
                                        "Assigned address is missing for stage_id: {}",
                                        stage_id
                                    )
                                    .into()
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;

                        partition_addrs
                            .partition_addrs
                            .insert(*part, Hosts { hosts });
                    } else {
                        // we have the full partition, so this is the only address required
                        let host = stage
                            .assigned_host
                            .clone()
                            .context("Assigned address is missing")?;
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
