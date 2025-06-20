use std::{
    collections::HashMap,
    env,
    sync::{Arc, LazyLock},
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

use crate::distribution_strategy::{Grouper, PartitionGroup, PartitionGrouper};
use crate::{
    analyze::{DistributedAnalyzeExec, DistributedAnalyzeRootExec},
    isolator::PartitionIsolatorExec,
    logging::{debug, error, info, trace},
    max_rows::MaxRowsExec,
    physical::DDStageOptimizerRule,
    result::{DDError, Result},
    stage::DDStageExec,
    stage_reader::{DDStageReaderExec, QueryId},
    util::{display_plan_with_partition_counts, get_client, physical_plan_to_bytes, wait_for},
    vocab::{
        Addrs, CtxAnnotatedOutputs, CtxHost, CtxPartitionGroup, CtxStageAddrs, CtxStageId, DDTask,
        Host, Hosts, PartitionAddrs, StageAddrs,
    },
};

#[derive(Debug, Clone)]
pub struct DDStage {
    /// our stage id
    pub stage_id: u64,
    /// the physical plan of our stage
    pub plan: Arc<dyn ExecutionPlan>,
    /// the partition groups for this stage.
    pub partition_groups: Vec<PartitionGroup>,
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
        partition_groups: Vec<PartitionGroup>,
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

/// Builds the physical plan from the logical plan, using the default Physical Planner from DataFusion
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
pub async fn distributed_physical_planning<G: Grouper + Copy>(
    physical_plan: Arc<dyn ExecutionPlan>,
    partiton_grouper: G,
    batch_size: usize,
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
                build_replacement(plan, partiton_grouper, true, batch_size, batch_size)?;
            partition_groups = calculated_partition_groups;
            full_partitions = false;

            Ok(Transformed::yes(replacement))
        } else if plan.as_any().downcast_ref::<SortExec>().is_some() {
            trace!("sort exec partition_groups: {:?}", partition_groups);
            let (calculated_partition_groups, replacement) =
                build_replacement(plan, partiton_grouper, false, batch_size, batch_size)?;
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
            let partition_count = plan.output_partitioning().partition_count();
            trace!("nested join output partitioning {}", partition_count);

            replacement = Arc::new(MaxRowsExec::new(
                Arc::new(CoalesceBatchesExec::new(replacement, batch_size))
                    as Arc<dyn ExecutionPlan>,
                batch_size,
            )) as Arc<dyn ExecutionPlan>;

            // NestedLoopJoinExec must be on a stage by itself
            partition_groups = vec![PartitionGroup::new(0, partition_count)];
            full_partitions = true;
            Ok(Transformed::yes(replacement))
        } else {
            trace!("not special case partition_groups: {:?}", partition_groups);

            let partition_count = plan.output_partitioning().partition_count();

            // Default to one partition group containing all the partitions.
            partition_groups = vec![PartitionGroup::new(0, partition_count)];
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
            stage.partition_groups = vec![PartitionGroup::new(0, 1)]; // accounting for coalesce
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
    stages: &[DDStage],
    worker_addrs: Vec<Host>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(Addrs, Vec<DDTask>)> {
    // map of worker name to address
    // FIXME: use types over tuples of strings, as we can accidently swap them and
    // not know

    // a map of worker name to host
    let mut workers: HashMap<String, Host> = worker_addrs
        .iter()
        .map(|host| (host.name.clone(), host.clone()))
        .collect();

    for attempt in 0..3 {
        if workers.is_empty() {
            return Err(anyhow!("No workers available to distribute stages").into());
        }

        // all stages to workers
        let (task_datas, final_addrs) =
            assign_to_workers(query_id, stages, workers.values().collect(), codec)?;

        // we retry this a few times to ensure that the workers are ready
        // and can accept the stages
        match try_distribute_tasks(&task_datas).await {
            Ok(_) => return Ok((final_addrs, task_datas)),
            Err(DDError::WorkerCommunicationError(bad_worker)) => {
                error!(
                    "distribute stages for query {query_id} attempt {attempt} failed removing \
                     worker {bad_worker}. Retrying..."
                );
                // if we cannot communicate with a worker, we remove it from the list of workers
                workers.remove(&bad_worker.name);
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
async fn try_distribute_tasks(task_datas: &[DDTask]) -> Result<()> {
    // we can use the stage data to distribute the stages to workers
    for task_data in task_datas {
        trace!(
            "Distributing Task: stage_id {}, pg: {:?} to worker: {:?}",
            task_data.stage_id,
            task_data.partition_group,
            task_data.assigned_host
        );

        // populate its child stages
        let mut stage_data = task_data.clone();
        stage_data.stage_addrs = Some(get_stage_addrs_from_tasks(
            &stage_data.child_stage_ids,
            task_datas,
        )?);

        let host = stage_data
            .assigned_host
            .clone()
            .context("Assigned host is missing for task data")?;

        let mut client = match get_client(&host) {
            Ok(client) => client,
            Err(e) => {
                error!("Couldn't not communicate with worker {e:#?}");
                return Err(DDError::WorkerCommunicationError(
                    host.clone(), // here
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
// groups and produce a DDTask for each partition group and assign it
// to a worker
fn assign_to_workers(
    query_id: &str,
    stages: &[DDStage],
    worker_addrs: Vec<&Host>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<(Vec<DDTask>, Addrs)> {
    let mut task_datas = vec![];
    let mut worker_idx = 0;

    trace!(
        "assigning stages: {:?}",
        stages
            .iter()
            .map(|s| format!("stage_id: {}, pgs:{:?}", s.stage_id, s.partition_groups))
            .join(",\n")
    );

    // keep track of which worker has the root of the plan tree (highest stage
    // number)
    let mut max_stage_id = -1;
    let mut final_addrs = Addrs::default();

    for stage in stages {
        for partition_group in stage.partition_groups.iter() {
            let plan_bytes = physical_plan_to_bytes(stage.plan.clone(), codec)?;

            let host = worker_addrs[worker_idx].clone();
            worker_idx = (worker_idx + 1) % worker_addrs.len();

            if stage.stage_id as isize > max_stage_id {
                // this wasn't the last stage
                max_stage_id = stage.stage_id as isize;
                final_addrs.clear();
            }
            if stage.stage_id as isize == max_stage_id {
                for part in partition_group.start()..partition_group.end() {
                    // we are the final stage, so we will be the one to serve this partition
                    final_addrs
                        .entry(stage.stage_id)
                        .or_default()
                        .entry(part as u64)
                        .or_default()
                        .push(host.clone());
                }
            }

            let task_data = DDTask {
                query_id: query_id.to_string(),
                stage_id: stage.stage_id,
                plan_bytes,
                partition_group: (partition_group.start() as u64..partition_group.end() as u64)
                    .collect(),
                child_stage_ids: stage.child_stage_ids().unwrap_or_default().to_vec(),
                stage_addrs: None, // will be calculated and filled in later
                num_output_partitions: stage.plan.output_partitioning().partition_count() as u64,
                full_partitions: stage.full_partitions,
                assigned_host: Some(host),
            };
            task_datas.push(task_data);
        }
    }

    Ok((task_datas, final_addrs))
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
fn build_replacement<G: Grouper>(
    plan: Arc<dyn ExecutionPlan>,
    partition_grouper: G,
    isolate: bool,
    max_rows: usize,
    inner_batch_size: usize,
) -> Result<(Vec<PartitionGroup>, Arc<dyn ExecutionPlan>), DataFusionError> {
    let mut replacement = plan.clone();
    let children = plan.children();
    assert_eq!(children.len(), 1, "Unexpected plan structure");

    let child = children[0];
    let partition_count = plan.output_partitioning().partition_count();
    trace!(
        "build_replacement for {}, partition_count: {}",
        displayable(plan.as_ref()).one_line(),
        partition_count
    );

    let partition_groups = partition_grouper.group(partition_count);

    if isolate && partition_groups.len() > 1 {
        let new_child = Arc::new(PartitionIsolatorExec::new(child.clone(), partition_count));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record_batch_exec::RecordBatchExec;
    use arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::{repartition::RepartitionExec, Partitioning};
    use std::sync::Arc;

    fn create_test_plan() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let record_batch_exec = Arc::new(RecordBatchExec::new(batch));
        Arc::new(
            RepartitionExec::try_new(record_batch_exec, Partitioning::RoundRobinBatch(4)).unwrap(),
        )
    }

    #[test]
    fn test_build_replacement_basic() {
        let plan = create_test_plan();
        let grouper = PartitionGrouper::new(2);
        let (partition_groups, replacement) =
            build_replacement(plan.clone(), grouper, false, 1000, 8192).unwrap();
        assert_eq!(
            grouper.group(plan.properties().partitioning.partition_count()),
            partition_groups
        );
        let max_rows_exec = replacement.as_any().downcast_ref::<MaxRowsExec>().unwrap();
        assert_eq!(max_rows_exec.max_rows, 1000);
        let coalesce_batches_exec = max_rows_exec.children()[0]
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .unwrap();
        assert_eq!(coalesce_batches_exec.target_batch_size(), 8192);
    }

    #[test]
    fn test_build_replacement_with_isolation() {
        let plan = create_test_plan();
        let grouper = PartitionGrouper::new(1);
        let (partition_groups, replacement) =
            build_replacement(plan.clone(), grouper, true, 1000, 8192).unwrap();
        assert_eq!(
            grouper.group(plan.properties().partitioning.partition_count()),
            partition_groups
        );
        let max_rows = replacement.as_any().downcast_ref::<MaxRowsExec>().unwrap();
        let coalesce = max_rows.children()[0]
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .unwrap();
        let repartition = coalesce.children()[0];
        let isolator = repartition.children()[0]
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .unwrap();
        assert_eq!(
            isolator.partition_count,
            plan.properties().partitioning.partition_count()
        )
    }

    // Edge case tests with small batch sizes and controlled partition counts
    fn create_test_plan_with_partitions(num_partitions: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]))],
        )
        .unwrap();
        let record_batch_exec = Arc::new(RecordBatchExec::new(batch));
        Arc::new(
            RepartitionExec::try_new(
                record_batch_exec,
                Partitioning::RoundRobinBatch(num_partitions),
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_build_replacement_edge_case_uneven_partitions() {
        // Test: 7 partitions with group size 3 -> groups [0..3), [3..6), [6..7)
        let plan = create_test_plan_with_partitions(7);
        let grouper = PartitionGrouper::new(3);
        let (partition_groups, replacement) =
            build_replacement(plan.clone(), grouper, true, 10, 50).unwrap();

        let expected_groups = vec![
            PartitionGroup::new(0, 3),
            PartitionGroup::new(3, 6),
            PartitionGroup::new(6, 7), // Uneven last group
        ];
        assert_eq!(partition_groups, expected_groups);

        // Verify small batch sizes are applied
        let max_rows = replacement.as_any().downcast_ref::<MaxRowsExec>().unwrap();
        assert_eq!(max_rows.max_rows, 10);
        let coalesce = max_rows.children()[0]
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .unwrap();
        assert_eq!(coalesce.target_batch_size(), 50);

        // Verify isolator is inserted since isolate=true and partition_groups.len() > 1
        let repartition = coalesce.children()[0];
        let isolator = repartition.children()[0]
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .unwrap();
        assert_eq!(isolator.partition_count, 7);
    }

    #[test]
    fn test_build_replacement_edge_case_single_partition() {
        // Test: 1 partition with group size 5 -> groups [0..1)
        let plan = create_test_plan_with_partitions(1);
        let grouper = PartitionGrouper::new(5);
        let (partition_groups, replacement) =
            build_replacement(plan.clone(), grouper, true, 5, 20).unwrap();

        let expected_groups = vec![PartitionGroup::new(0, 1)];
        assert_eq!(partition_groups, expected_groups);

        // With only 1 partition group, isolator should NOT be inserted
        let max_rows = replacement.as_any().downcast_ref::<MaxRowsExec>().unwrap();
        let coalesce = max_rows.children()[0]
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .unwrap();
        let repartition = coalesce.children()[0];
        // Should be the original child, not an isolator
        assert!(repartition.children()[0]
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .is_none());
    }

    #[test]
    fn test_build_replacement_edge_case_group_size_larger_than_partitions() {
        // Test: 3 partitions with group size 10 -> groups [0..3)
        let plan = create_test_plan_with_partitions(3);
        let grouper = PartitionGrouper::new(10);
        let (partition_groups, replacement) =
            build_replacement(plan.clone(), grouper, false, 2, 15).unwrap();

        let expected_groups = vec![PartitionGroup::new(0, 3)];
        assert_eq!(partition_groups, expected_groups);

        // With isolate=false, no isolator should be inserted
        let max_rows = replacement.as_any().downcast_ref::<MaxRowsExec>().unwrap();
        let coalesce = max_rows.children()[0]
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .unwrap();
        let repartition = coalesce.children()[0];
        assert!(repartition.children()[0]
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .is_none());
    }

    #[test]
    fn test_build_replacement_edge_case_many_small_groups() {
        // Test: 8 partitions with group size 1 -> 8 groups of size 1
        let plan = create_test_plan_with_partitions(8);
        let grouper = PartitionGrouper::new(1);
        let (partition_groups, replacement) =
            build_replacement(plan.clone(), grouper, true, 3, 12).unwrap();

        let expected_groups = vec![
            PartitionGroup::new(0, 1),
            PartitionGroup::new(1, 2),
            PartitionGroup::new(2, 3),
            PartitionGroup::new(3, 4),
            PartitionGroup::new(4, 5),
            PartitionGroup::new(5, 6),
            PartitionGroup::new(6, 7),
            PartitionGroup::new(7, 8),
        ];
        assert_eq!(partition_groups, expected_groups);

        // With 8 groups (> 1), isolator should be inserted
        let max_rows = replacement.as_any().downcast_ref::<MaxRowsExec>().unwrap();
        assert_eq!(max_rows.max_rows, 3);
        let coalesce = max_rows.children()[0]
            .as_any()
            .downcast_ref::<CoalesceBatchesExec>()
            .unwrap();
        assert_eq!(coalesce.target_batch_size(), 12);
        let repartition = coalesce.children()[0];
        let isolator = repartition.children()[0]
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .unwrap();
        assert_eq!(isolator.partition_count, 8);
    }
}
