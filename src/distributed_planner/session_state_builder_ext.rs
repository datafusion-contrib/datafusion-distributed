use crate::{
    AddCoalesceOnTop, AnnotatePlan, ApplyNetworkBoundaries, BatchCoalesceBelowBoundaries,
    EndDistributedContext, InsertBroadcast, StartDistributedContext,
};
use datafusion::execution::SessionStateBuilder;
use std::sync::Arc;

pub trait SessionStateBuilderExt {
    /// Adds all the distributed physical optimizer rules to the [SessionStateBuilder] in the
    /// correct order and placement.
    fn with_distributed_physical_optimizer_rules(self) -> Self;

    /// Same as [SessionStateBuilderExt::with_distributed_physical_optimizer_rules] but with an in-place mutation.
    fn set_distributed_physical_optimizer_rules(&mut self);
}

impl SessionStateBuilderExt for SessionStateBuilder {
    fn with_distributed_physical_optimizer_rules(mut self) -> Self {
        self.set_distributed_physical_optimizer_rules();
        self
    }

    fn set_distributed_physical_optimizer_rules(&mut self) {
        let rules = self.physical_optimizer_rules().get_or_insert_default();
        rules.extend_from_slice(&[
            Arc::new(StartDistributedContext),
            Arc::new(AddCoalesceOnTop),
            Arc::new(InsertBroadcast),
            Arc::new(AnnotatePlan),
            Arc::new(ApplyNetworkBoundaries),
            Arc::new(BatchCoalesceBelowBoundaries),
            Arc::new(EndDistributedContext),
        ])
    }
}
