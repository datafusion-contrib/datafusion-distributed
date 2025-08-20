use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};

use std::fmt::Write;
use std::sync::Arc;

pub fn display_plan_with_partition_in_out(plan: &dyn ExecutionPlan) -> Result<String> {
    let mut f = String::new();

    fn visit(plan: &dyn ExecutionPlan, indent: usize, f: &mut String) -> Result<()> {
        let output_partitions = plan.output_partitioning().partition_count();
        let input_partitions = plan
            .children()
            .first()
            .map(|child| child.output_partitioning().partition_count());

        write!(
            f,
            "partitions [out:{:<3}{}]{} {}",
            output_partitions,
            input_partitions
                .map(|p| format!("<-- in:{:<3}", p))
                .unwrap_or("          ".to_string()),
            " ".repeat(indent),
            displayable(plan).one_line()
        )?;

        plan.children()
            .iter()
            .try_for_each(|input| visit(input.as_ref(), indent + 2, f))?;

        Ok(())
    }

    visit(plan, 0, &mut f)?;
    Ok(f)
}

/// Returns a boolean indicating if this stage can be divided into more than one task.
///
/// Some Plan nodes need to materialize all partitions inorder to execute such as
/// NestedLoopJoinExec.   Rewriting the plan to accommodate dividing it into tasks
/// would result in redundant work.
///
/// The plans we cannot split are:
/// - NestedLoopJoinExec
pub fn can_be_divided(plan: &Arc<dyn ExecutionPlan>) -> Result<bool> {
    // recursively check to see if this stages plan contains a NestedLoopJoinExec
    let mut has_unsplittable_plan = false;
    let search = |f: &Arc<dyn ExecutionPlan>| {
        if f.as_any()
            .downcast_ref::<datafusion::physical_plan::joins::NestedLoopJoinExec>()
            .is_some()
        {
            has_unsplittable_plan = true;
            return Ok(TreeNodeRecursion::Stop);
        }

        Ok(TreeNodeRecursion::Continue)
    };
    plan.apply(search)?;
    Ok(!has_unsplittable_plan)
}
