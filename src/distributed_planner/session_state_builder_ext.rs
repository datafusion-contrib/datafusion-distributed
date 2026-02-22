use crate::{
    AddCoalesceOnTop, ApplyNetworkBoundaries, BatchCoalesceBelowBoundaries, EndDistributedContext,
    InjectNetworkBoundaryPlaceholders, InsertBroadcast, StartDistributedContext,
};
use datafusion::execution::SessionStateBuilder;
use std::sync::Arc;

pub trait SessionStateBuilderExt {
    /// Adds all the distributed physical optimizer rules to the [SessionStateBuilder] in the
    /// correct order and placement.
    fn with_distributed_physical_optimizer_rules(self) -> Self;

    /// Same as [SessionStateBuilderExt::with_distributed_physical_optimizer_rules] but with an
    /// in-place mutation.
    fn set_distributed_physical_optimizer_rules(&mut self);
}

impl SessionStateBuilderExt for SessionStateBuilder {
    fn with_distributed_physical_optimizer_rules(mut self) -> Self {
        self.set_distributed_physical_optimizer_rules();
        self
    }

    fn set_distributed_physical_optimizer_rules(&mut self) {
        let rules = self.physical_optimizer_rules().get_or_insert_default();
        // Any rule related to Distributed DataFusion needs to be placed between the
        // StartDistributedContext and the EndDistributedContext rules, as those are
        // designed to propagate contextual information only relevant for distributed
        // planning
        rules.extend_from_slice(&[
            Arc::new(StartDistributedContext),
            Arc::new(AddCoalesceOnTop),
            Arc::new(InsertBroadcast),
            Arc::new(InjectNetworkBoundaryPlaceholders),
            Arc::new(ApplyNetworkBoundaries),
            Arc::new(BatchCoalesceBelowBoundaries),
            Arc::new(EndDistributedContext),
        ])
    }
}
