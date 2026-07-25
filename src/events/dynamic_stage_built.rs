use crate::events::common::EventHandlerChain;
use datafusion::common::{Result, stats::Precision};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

/// Estimated cost associated to the fragment of the plan. Calculated based on the amount of data
/// that will flow through each node and the static cpu, memory and network complexity of the
/// different nodes based on the algorithms that will run inside them.
#[derive(Default, Debug, Clone, Copy)]
pub struct Cost {
    /// Estimated cost of CPU measured in bytes.
    pub cpu: Precision<usize>,
    /// Estimated amount of memory consumed measured in bytes.
    pub memory: Precision<usize>,
    /// Estimated amount of data that will need to be transferred over the wire in bytes.
    pub network: Precision<usize>,
}

/// Event definition fired for each individual stage during dynamic planning.
#[derive(Copy, Clone)]
pub struct DynamicStageBuiltEvent<'a> {
    /// Cost associated to the plan in the field below and all its children recursively until
    /// network boundaries.
    pub cost: Cost,
    /// Plan that is about to be sent to a remote worker for runtime sampling.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// SessionConfig in scope at the moment of building the stage.
    pub session_config: &'a SessionConfig,
}

pub struct DynamicStageBuiltEventResponse {
    /// A potentially modified plan (e.g., optimizations applied).
    pub plan: Arc<dyn ExecutionPlan>,
}

impl DynamicStageBuiltEventResponse {
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self { plan }
    }
}

pub trait DynamicStageBuiltHandler: Send + Sync + 'static {
    fn handle(&self, ev: DynamicStageBuiltEvent) -> Option<Result<DynamicStageBuiltEventResponse>>;
}

impl<F> DynamicStageBuiltHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(DynamicStageBuiltEvent<'a>) -> Option<Result<DynamicStageBuiltEventResponse>>,
{
    fn handle(&self, ev: DynamicStageBuiltEvent) -> Option<Result<DynamicStageBuiltEventResponse>> {
        self(ev)
    }
}

pub(crate) type DynamicStageBuiltHandlers = EventHandlerChain<dyn DynamicStageBuiltHandler>;

impl DynamicStageBuiltHandlers {
    pub(crate) fn handle(
        ev: DynamicStageBuiltEvent,
    ) -> Option<Result<DynamicStageBuiltEventResponse>> {
        ev.session_config
            .get_extension::<DynamicStageBuiltHandlers>()?
            .find_map(|handler| handler.handle(ev))
    }
}
