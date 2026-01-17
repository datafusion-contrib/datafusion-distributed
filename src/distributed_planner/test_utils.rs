#[cfg(test)]
use crate::distributed_planner::insert_broadcast::insert_broadcast_execs;
#[cfg(test)]
use crate::distributed_planner::plan_annotator::annotate_plan;
#[cfg(test)]
use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
#[cfg(test)]
use crate::test_utils::parquet::register_parquet_tables;
#[cfg(test)]
use crate::{DistributedConfig, DistributedExt, DistributedPhysicalOptimizerRule};
#[cfg(test)]
use crate::{TaskEstimation, TaskEstimator, display_plan_ascii};
#[cfg(test)]
use datafusion::config::ConfigOptions;
#[cfg(test)]
use datafusion::execution::{SessionStateBuilder, context::SessionContext};
#[cfg(test)]
use datafusion::physical_plan::{ExecutionPlan, displayable};
#[cfg(test)]
use datafusion::prelude::SessionConfig;
#[cfg(test)]
use itertools::Itertools;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
pub(crate) fn base_session_builder(
    target_partitions: usize,
    num_workers: usize,
    broadcast_enabled: bool,
) -> SessionStateBuilder {
    let mut config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_information_schema(true);

    let d_cfg = DistributedConfig {
        broadcast_joins: broadcast_enabled,
        ..Default::default()
    };
    config.set_distributed_option_extension(d_cfg).unwrap();

    SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(num_workers))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TestPlanOptions {
    pub(crate) target_partitions: usize,
    pub(crate) num_workers: usize,
    pub(crate) broadcast_enabled: bool,
}

#[cfg(test)]
impl Default for TestPlanOptions {
    fn default() -> Self {
        Self {
            target_partitions: 4,
            num_workers: 4,
            broadcast_enabled: false,
        }
    }
}

#[cfg(test)]
pub(crate) async fn context_with_query(
    builder: SessionStateBuilder,
    query: &str,
) -> (SessionContext, String) {
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    let mut queries = query.split(';').collect_vec();
    let last_query = queries.pop().unwrap();

    for query in queries {
        ctx.sql(query).await.unwrap();
    }

    register_parquet_tables(&ctx).await.unwrap();
    (ctx, last_query.to_string())
}

#[cfg(test)]
pub(crate) async fn annotate_test_plan(
    query: &str,
    options: TestPlanOptions,
    configure: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
) -> String {
    let builder = base_session_builder(
        options.target_partitions,
        options.num_workers,
        options.broadcast_enabled,
    );
    let builder = configure(builder);
    let (ctx, query) = context_with_query(builder, query).await;
    let df = ctx.sql(&query).await.unwrap();
    let mut plan = df.create_physical_plan().await.unwrap();

    plan = insert_broadcast_execs(plan, ctx.state_ref().read().config_options().as_ref())
        .expect("failed to insert broadcasts");

    let annotated = annotate_plan(plan, ctx.state_ref().read().config_options().as_ref())
        .expect("failed to annotate plan");
    format!("{annotated:?}")
}

#[cfg(test)]
pub(crate) async fn explain_test_plan(
    query: &str,
    options: TestPlanOptions,
    use_optimizer: bool,
    configure: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder,
) -> String {
    let mut builder = base_session_builder(
        options.target_partitions,
        options.num_workers,
        options.broadcast_enabled,
    );
    if use_optimizer {
        builder = builder.with_physical_optimizer_rule(Arc::new(DistributedPhysicalOptimizerRule));
    }
    let builder = configure(builder);
    let (ctx, query) = context_with_query(builder, query).await;
    let df = ctx.sql(&query).await.unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();

    if use_optimizer {
        display_plan_ascii(physical_plan.as_ref(), false)
    } else {
        format!("{}", displayable(physical_plan.as_ref()).indent(true))
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct BuildSideOneTaskEstimator;

#[cfg(test)]
impl TaskEstimator for BuildSideOneTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        if !plan.children().is_empty() {
            return None;
        }
        let schema = plan.schema();
        let has_min_temp = schema.fields().iter().any(|f| f.name() == "MinTemp");
        let has_max_temp = schema.fields().iter().any(|f| f.name() == "MaxTemp");
        if has_min_temp && !has_max_temp {
            Some(TaskEstimation::maximum(1))
        } else {
            None
        }
    }

    fn scale_up_leaf_node(
        &self,
        _: &Arc<dyn ExecutionPlan>,
        _: usize,
        _: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}
