use datafusion::common::JoinType;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{
    CrossJoinExec, HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec,
};
use std::sync::Arc;

pub(crate) trait AnyJoin: ExecutionPlan {
    fn exprs(&self) -> Vec<(PhysicalExprRef, PhysicalExprRef)>;
    fn projection(&self) -> Option<&[usize]>;
    fn join_type(&self) -> JoinType;
    fn left(&self) -> &Arc<dyn ExecutionPlan>;
    fn right(&self) -> &Arc<dyn ExecutionPlan>;
}

pub(crate) fn get_join(plan: &Arc<dyn ExecutionPlan>) -> Option<&dyn AnyJoin> {
    if let Some(plan) = plan.downcast_ref::<HashJoinExec>() {
        Some(plan)
    } else if let Some(plan) = plan.downcast_ref::<NestedLoopJoinExec>() {
        Some(plan)
    } else if let Some(plan) = plan.downcast_ref::<CrossJoinExec>() {
        Some(plan)
    } else if let Some(plan) = plan.downcast_ref::<SortMergeJoinExec>() {
        Some(plan)
    } else {
        None
    }
}

impl AnyJoin for HashJoinExec {
    fn exprs(&self) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
        // TODO: avoid this clone
        self.on.clone()
    }

    fn projection(&self) -> Option<&[usize]> {
        self.projection.as_ref().map(|v| v.as_ref())
    }

    fn join_type(&self) -> JoinType {
        *HashJoinExec::join_type(self)
    }

    fn left(&self) -> &Arc<dyn ExecutionPlan> {
        HashJoinExec::left(self)
    }

    fn right(&self) -> &Arc<dyn ExecutionPlan> {
        HashJoinExec::right(self)
    }
}

impl AnyJoin for NestedLoopJoinExec {
    fn exprs(&self) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
        match self.filter() {
            // TODO: the idea is that the two-position tuples this function returns belong respectively
            //  to the left child and to the right child. However, the filter() in a NLJ refers to a
            //  weird intermediate schema. Figure this out.
            Some(v) => vec![],
            None => vec![],
        }
    }

    fn projection(&self) -> Option<&[usize]> {
        NestedLoopJoinExec::projection(self)
            .as_ref()
            .map(|v| v.as_ref())
    }

    fn join_type(&self) -> JoinType {
        *NestedLoopJoinExec::join_type(self)
    }

    fn left(&self) -> &Arc<dyn ExecutionPlan> {
        NestedLoopJoinExec::right(self)
    }

    fn right(&self) -> &Arc<dyn ExecutionPlan> {
        NestedLoopJoinExec::right(self)
    }
}

impl AnyJoin for CrossJoinExec {
    fn exprs(&self) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
        vec![]
    }

    fn projection(&self) -> Option<&[usize]> {
        None
    }

    fn join_type(&self) -> JoinType {
        // TODO: is this right?
        JoinType::Full
    }

    fn left(&self) -> &Arc<dyn ExecutionPlan> {
        CrossJoinExec::right(self)
    }

    fn right(&self) -> &Arc<dyn ExecutionPlan> {
        CrossJoinExec::right(self)
    }
}

impl AnyJoin for SortMergeJoinExec {
    fn exprs(&self) -> Vec<(PhysicalExprRef, PhysicalExprRef)> {
        // TODO: avoid this clone
        self.on.clone()
    }

    fn projection(&self) -> Option<&[usize]> {
        self.projection.as_ref().map(|v| v.as_slice())
    }

    fn join_type(&self) -> JoinType {
        SortMergeJoinExec::join_type(self)
    }

    fn left(&self) -> &Arc<dyn ExecutionPlan> {
        SortMergeJoinExec::right(self)
    }

    fn right(&self) -> &Arc<dyn ExecutionPlan> {
        SortMergeJoinExec::right(self)
    }
}
