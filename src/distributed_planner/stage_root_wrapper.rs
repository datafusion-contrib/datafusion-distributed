use crate::common::{TreeNodeExt, require_one_child};
use crate::{NetworkBoundaryExt, ProducerHead, Stage};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Result, internal_err};
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use std::fmt::Debug;
use std::sync::Arc;

/// Creates and recognizes explicit unary wrappers at producer stage roots.
///
/// A wrapper can cache stage results, establish execution context, observe progress, or
/// enforce cancellation, deadlines and resource checks. Register the same implementation on
/// coordinator and worker sessions. The coordinator assembles the producer, then ensures its
/// root is wrapped before sending it. Workers preserve the wrapper when replacing the producer.
/// Its node uses the session's physical extension codec. Existing wrappers inside the
/// producer input remain in place. When no producer is needed, an already wrapped root is reused.
///
/// The wrapper has exactly one child and preserves successful query results, schema,
/// partitioning, ordering, boundedness and emission type. Its `replace_children` operation
/// must retain the supplied child subtree and any wrapper state needed across producer
/// replacement. Stateful wrappers must implement [`ExecutionPlan::reset_state`] to create
/// fresh task-local state while preserving their settings and child. Workers call it once per
/// task, including in-process execution. Producer replacement then retains that task's state
/// and the input subtree, including any initialized sampler.
///
/// The adapter is synchronous and runs during stage assembly, before the root executes.
/// Configure local behavior through the session while keeping wrappers present on both sides,
/// since distributed metrics require matching plan layouts.
pub trait StageRootWrapper: Debug + Send + Sync + 'static {
    /// Identifies your unary wrapper node, typically with
    /// `plan.is::<MyRootExec>()`.
    ///
    /// DFD uses this to avoid wrapping twice, reset task-local wrapper state, and rebuild
    /// the producer beneath the wrapper while keeping the wrapper on top.
    fn matches(&self, plan: &dyn ExecutionPlan) -> bool;

    /// Creates a unary plan node around the supplied stage root.
    fn wrap(&self, input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>;
}

#[derive(Debug)]
pub(crate) struct StageRootWrapperConfig(pub Arc<dyn StageRootWrapper>);

pub(crate) fn wrap_stage_root(
    plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(wrapper) = config.get_extension::<StageRootWrapperConfig>() else {
        return Ok(plan);
    };
    if wrapper.0.matches(plan.as_ref()) {
        return Ok(plan);
    }
    let wrapped = wrapper.0.wrap(Arc::clone(&plan))?;
    validate_stage_root(wrapped, &plan, wrapper.0.as_ref())
}

/// Separates task-local state even when in-process transport shares a decoded plan.
pub(crate) fn reset_stage_root(
    plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(wrapper) = config.get_extension::<StageRootWrapperConfig>() else {
        return Ok(plan);
    };
    if !wrapper.0.matches(plan.as_ref()) {
        return Ok(plan);
    }
    let input = require_one_child(plan.children())?;
    validate_stage_root(plan.reset_state()?, &input, wrapper.0.as_ref())
}

fn validate_stage_root(
    plan: Arc<dyn ExecutionPlan>,
    input: &Arc<dyn ExecutionPlan>,
    wrapper: &dyn StageRootWrapper,
) -> Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if !wrapper.matches(plan.as_ref()) || children.len() != 1 || !Arc::ptr_eq(children[0], input) {
        return internal_err!(
            "Stage-root wrapper must return a recognized unary wrapper around the supplied input"
        );
    }
    Ok(plan)
}

/// Applies producer replacement or sampler insertion below the registered root wrapper.
/// The input subtree is reused, since sampling may already have started on workers.
pub(crate) fn map_stage_root(
    plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
    rewrite: impl FnOnce(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let Some(wrapper) = config.get_extension::<StageRootWrapperConfig>() else {
        return rewrite(plan);
    };
    if !wrapper.0.matches(plan.as_ref()) {
        return rewrite(plan);
    }
    let children = plan.children();
    let [input] = children.as_slice() else {
        return internal_err!("Stage-root wrapper must have exactly one child");
    };
    let input = Arc::clone(input);
    plan.replace_children(
        vec![rewrite(input)?],
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}

/// Finalizes a producer beneath its wrapper. A no-producer stage is already assembled.
pub(crate) fn replace_stage_producer(
    plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
    producer: ProducerHead,
) -> Result<Arc<dyn ExecutionPlan>> {
    if matches!(producer, ProducerHead::None)
        && config
            .get_extension::<StageRootWrapperConfig>()
            .is_some_and(|wrapper| wrapper.0.matches(plan.as_ref()))
    {
        return Ok(plan);
    }
    map_stage_root(plan, config, |head| producer.insert(head))
}

/// Assembles static producers before ensuring one wrapper around each stage root.
pub(crate) fn wrap_stage_roots(
    plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    if config.get_extension::<StageRootWrapperConfig>().is_none() {
        return Ok(plan);
    }
    plan.transform_up_with_task_count(1, |plan, task_count| {
        let Some(boundary) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };
        let Stage::Local(stage) = boundary.input_stage() else {
            return Ok(Transformed::no(plan));
        };
        let mut stage = stage.clone();
        let producer = boundary.producer_head(task_count)?.insert(stage.plan)?;
        stage.plan = wrap_stage_root(producer, config)?;
        Ok(Transformed::yes(
            boundary.with_input_stage(Stage::Local(stage))?,
        ))
    })
    .map(|result| result.data)
}
