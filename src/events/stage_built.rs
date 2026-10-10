use crate::TaskCountAnnotation;
use crate::events::common::EventHandlerChain;
use datafusion::common::{Result, stats::Precision};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// Estimated CPU, memory, and network costs of a stage.
#[derive(Default, Debug, Clone, Copy)]
pub struct Cost {
    /// Estimated CPU work, scaled by data size.
    pub cpu: Precision<usize>,
    /// Estimated memory cost in bytes.
    pub memory: Precision<usize>,
    /// Estimated bytes transferred over the network.
    pub network: Precision<usize>,
}

/// Information supplied when a stage is built during dynamic planning.
#[derive(Clone)]
pub struct StageBuiltEvent<'a> {
    /// Estimated cost of the original stage plan, up to network boundaries.
    pub cost: Cost,
    /// Stage plan before task-count propagation and runtime sampling.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Session configuration used to build the stage.
    pub session_config: &'a SessionConfig,
}

/// Plan, task count, and metrics returned by a stage handler.
pub struct StageBuiltEventResponse {
    /// Stage plan, possibly rewritten by the handler.
    pub plan: Arc<dyn ExecutionPlan>,
    /// Task count merged with other handlers' annotations and the stage's existing count.
    /// `TaskCountAnnotation::soft(0.0)` adds no constraint.
    pub task_count: TaskCountAnnotation,
    /// Metrics displayed at the stage boundary.
    pub metrics: MetricsSet,
}

impl StageBuiltEventResponse {
    /// Returns an unchanged plan with no task-count constraint or metrics.
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            plan,
            metrics: MetricsSet::new(),
            task_count: TaskCountAnnotation::soft(0.0),
        }
    }

    /// Sets the stage's task-count annotation.
    pub fn with_task_count(mut self, task_count: TaskCountAnnotation) -> Self {
        self.task_count = task_count;
        self
    }

    /// Adds metrics to display at the stage boundary.
    pub fn with_metrics(mut self, metrics_set: MetricsSet) -> Self {
        self.metrics = metrics_set;
        self
    }
}

/// Handles a stage built during dynamic planning.
///
/// Custom handlers run in registration order, followed by built-in handlers. Each receives the
/// previous handler's plan; task-count annotations are merged and metrics are accumulated.
pub trait StageBuiltHandler: Send + Sync + 'static {
    /// Returns a plan, task-count annotation, and metrics.
    /// An error aborts planning.
    fn handle(&self, ev: StageBuiltEvent) -> Result<StageBuiltEventResponse>;
}

impl<F> StageBuiltHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(StageBuiltEvent<'a>) -> Result<StageBuiltEventResponse>,
{
    fn handle(&self, ev: StageBuiltEvent) -> Result<StageBuiltEventResponse> {
        self(ev)
    }
}

pub(crate) type StageBuiltHandlers = EventHandlerChain<dyn StageBuiltHandler>;

impl StageBuiltHandlers {
    pub(crate) fn handle(ev: StageBuiltEvent) -> Result<StageBuiltEventResponse> {
        let mut task_count = TaskCountAnnotation::soft(0.0);
        let mut plan = ev.plan;
        let mut metrics = MetricsSet::new();
        if let Some(handlers) = ev.session_config.get_extension::<StageBuiltHandlers>() {
            for handler in handlers.iter() {
                let ev = StageBuiltEvent {
                    cost: ev.cost,
                    session_config: ev.session_config,
                    plan,
                };
                let event_response = handler.handle(ev)?;
                plan = event_response.plan;
                task_count = task_count.merge(event_response.task_count)?;
                metrics.extend(event_response.metrics);
            }
        }
        Ok(StageBuiltEventResponse {
            plan,
            task_count,
            metrics,
        })
    }
}
