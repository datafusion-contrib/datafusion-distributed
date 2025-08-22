/// Be able to display a nice tree for stages.
///
/// The challenge to doing this at the moment is that `TreeRenderVistor`
/// in [`datafusion::physical_plan::display`] is not public, and that it also
/// is specific to a `ExecutionPlan` trait object, which we don't have.
///
/// TODO: try to upstream a change to make rendering of Trees (logical, physical, stages) against
/// a generic trait rather than a specific trait object. This would allow us to
/// use the same rendering code for all trees, including stages.
///
/// In the meantime, we can make a dummy ExecutionPlan that will let us render
/// the Stage tree.
use std::{fmt::Write, sync::Arc};

use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    error::Result,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties},
};

use crate::{
    common::util::display_plan_with_partition_in_out,
    plan::PartitionIsolatorExec,
    task::{format_pg, ExecutionTask},
    ArrowFlightReadExec,
};

use super::ExecutionStage;

// Unicode box-drawing characters for creating borders and connections.
const LTCORNER: &str = "┌"; // Left top corner
const LDCORNER: &str = "└"; // Left bottom corner
const VERTICAL: &str = "│"; // Vertical line
const HORIZONTAL: &str = "─"; // Horizontal line

impl DisplayAs for ExecutionStage {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        #[allow(clippy::format_in_format_args)]
        match t {
            DisplayFormatType::Default => {
                write!(f, "{}", self.name)
            }
            DisplayFormatType::Verbose => {
                writeln!(
                    f,
                    "{}{}{}{}",
                    LTCORNER,
                    HORIZONTAL.repeat(5),
                    format!(" {} ", self.name),
                    format_tasks(&self.tasks),
                )?;
                let plan_str = display_plan_with_partition_in_out(self.plan.as_ref())
                    .map_err(|_| std::fmt::Error {})?;
                let plan_str = plan_str
                    .split('\n')
                    .filter(|v| !v.is_empty())
                    .collect::<Vec<_>>()
                    .join(&format!("\n{}{}", "  ".repeat(self.depth), VERTICAL));
                writeln!(f, "{}{}{}", "  ".repeat(self.depth), VERTICAL, plan_str)?;
                write!(
                    f,
                    "{}{}{}",
                    "  ".repeat(self.depth),
                    LDCORNER,
                    HORIZONTAL.repeat(50)
                )?;

                Ok(())
            }
            DisplayFormatType::TreeRender => write!(
                f,
                "{}",
                self.tasks
                    .iter()
                    .map(|task| format!("{task}"))
                    .collect::<Vec<_>>()
                    .join("\n")
            ),
        }
    }
}

pub fn display_stage_graphviz(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
    let mut f = String::new();

    writeln!(
        f,
        "digraph G {{
  rankdir=BT
"
    )?;

    // draw all tasks first
    plan.apply(|node| {
        let stage = node
            .as_any()
            .downcast_ref::<ExecutionStage>()
            .expect("Expected ExecutionStage");
        for task in stage.tasks.iter() {
            let partition_group = &task.partition_group;
            let p = display_single_task(stage, partition_group)?;
            writeln!(f, "{}", p)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    // now draw edges between the tasks

    plan.apply(|node| {
        let stage = node
            .as_any()
            .downcast_ref::<ExecutionStage>()
            .expect("Expected ExecutionStage");

        for child_stage in stage.child_stages_iter() {
            for task in stage.tasks.iter() {
                for child_task in child_stage.tasks.iter() {
                    let edges = display_inter_task_edges(stage, task, child_stage, child_task)?;
                    writeln!(f, "{}", edges)?;
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    writeln!(f, "}}")?;

    Ok(f)
}

pub fn display_single_task(stage: &ExecutionStage, partition_group: &[u64]) -> Result<String> {
    let mut f = String::new();
    writeln!(
        f,
        "
  subgraph \"cluster_stage_{}_task_{}\" {{
    color=blue
    style=dotted
    label = \"Stage {} Task Partitions {}\"
    labeljust=r
    labelloc=b

    node[shape=none]
",
        stage.num,
        format_pg(partition_group),
        stage.num,
        format_pg(partition_group)
    )?;

    // draw all plans
    // we need to label the nodes including depth to uniquely identify them within this task
    let mut depth = 0;
    stage.plan.apply(|plan| {
        let p = display_single_plan(plan.as_ref(), stage.num, partition_group, depth)?;
        writeln!(f, "{}", p)?;
        depth += 1;
        Ok(TreeNodeRecursion::Continue)
    })?;

    // draw edges between the plans
    depth = 0;
    stage.plan.apply(|plan| {
        if let Some(child) = plan.children().first() {
            let partitions = child.output_partitioning().partition_count();
            for i in 0..partitions {
                let mut style = "";
                if plan
                    .as_any()
                    .downcast_ref::<PartitionIsolatorExec>()
                    .is_some()
                    && !partition_group.contains(&(i as u64))
                {
                    style = "[style=invis]";
                }

                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{} -> {}_{}_{}_{}:b{} {}",
                    child.name(),
                    stage.num,
                    node_format_pg(partition_group),
                    depth + 1,
                    i,
                    plan.name(),
                    stage.num,
                    node_format_pg(partition_group),
                    depth,
                    i,
                    style
                )?;
            }
        }
        depth += 1;
        Ok(TreeNodeRecursion::Continue)
    })?;

    writeln!(f, "  }}")?;

    Ok(f)
}

/// We want to display a single plan as a three row table with the top and bottom being
/// graphvis ports.
///
/// We accept an index `i` to make the node name unique in the graphviz output.
///
/// An example of such a node would be:
///
/// ```text
///       ArrowFlightReadExec [label=<
///     <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="0">
///         <TR>
///             <TD CELLBORDER="0">
///                 <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
///                     <TR>
///                         <TD PORT="t1"></TD>
///                         <TD PORT="t2"></TD>
///                     </TR>
///                 </TABLE>
///             </TD>
///         </TR>
///         <TR>
///             <TD BORDER="0" CELLPADDING="0" CELLSPACING="0">
///                 <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
///                     <TR>
///                         <TD>ArrowFlightReadExec</TD>
///                     </TR>
///                 </TABLE>
///             </TD>
///         </TR>
///         <TR>
///             <TD CELLBORDER="0">
///                 <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
///                     <TR>
///                         <TD PORT="b1"></TD>
///                         <TD PORT="b2"></TD>
///                     </TR>
///                 </TABLE>
///             </TD>
///         </TR>
///     </TABLE>
/// >];
/// ```
pub fn display_single_plan(
    plan: &dyn ExecutionPlan,
    stage_num: usize,
    partition_group: &[u64],
    depth: usize,
) -> Result<String> {
    let mut f = String::new();
    let output_partitions = plan.output_partitioning().partition_count();
    let input_partitions = if let Some(child) = plan.children().first() {
        child.output_partitioning().partition_count()
    } else if plan
        .as_any()
        .downcast_ref::<ArrowFlightReadExec>()
        .is_some()
    {
        output_partitions
    } else {
        1
    };

    writeln!(
        f,
        "
    {}_{}_{}_{} [label=<
    <TABLE BORDER='0' CELLBORDER='0' CELLSPACING='0' CELLPADDING='0'>
        <TR>
            <TD CELLBORDER='0'>
                <TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0'>
                    <TR>",
        plan.name(),
        stage_num,
        node_format_pg(partition_group),
        depth
    )?;

    for i in 0..output_partitions {
        writeln!(f, "                        <TD PORT='t{}'></TD>", i)?;
    }

    writeln!(
        f,
        "                   </TR>
                </TABLE>
            </TD>
        </TR>
        <TR>
            <TD BORDER='0' CELLPADDING='0' CELLSPACING='0'>
                <TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0'>
                    <TR>
                        <TD>{}</TD>
                    </TR>
                </TABLE>
            </TD>
        </TR>
        <TR>
            <TD CELLBORDER='0'>
                <TABLE BORDER='0' CELLBORDER='1' CELLSPACING='0'>
                    <TR>",
        plan.name()
    )?;

    for i in 0..input_partitions {
        writeln!(f, "                        <TD PORT='b{}'></TD>", i)?;
    }

    writeln!(
        f,
        "                   </TR>
                </TABLE>
            </TD>
        </TR>
    </TABLE>
  >];
"
    )?;
    Ok(f)
}

fn display_inter_task_edges(
    stage: &ExecutionStage,
    task: &ExecutionTask,
    child_stage: &ExecutionStage,
    child_task: &ExecutionTask,
) -> Result<String> {
    let mut f = String::new();

    let mut found_isolator = false;
    let mut depth = 0;
    stage.plan.apply(|plan| {
        if plan
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .is_some()
        {
            found_isolator = true;
        }
        if plan
            .as_any()
            .downcast_ref::<ArrowFlightReadExec>()
            .is_some()
        {
            // draw the edges from this node pulling from its child
            for p in 0..plan.output_partitioning().partition_count() {
                let mut style = "";
                if found_isolator && !task.partition_group.contains(&(p as u64)) {
                    style = "[style=invis]";
                }

                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{} -> {}_{}_{}_{}:b{} {}",
                    child_stage.plan.name(),
                    child_stage.num,
                    node_format_pg(&child_task.partition_group),
                    0,
                    p,
                    plan.name(),
                    stage.num,
                    node_format_pg(&task.partition_group),
                    depth,
                    p,
                    style,
                )?;
            }
        }
        depth += 1;
        Ok(TreeNodeRecursion::Continue)
    })?;

    Ok(f)
}

fn format_tasks(tasks: &[ExecutionTask]) -> String {
    tasks
        .iter()
        .map(|task| format!("{task}"))
        .collect::<Vec<String>>()
        .join(",")
}

fn node_format_pg(partition_group: &[u64]) -> String {
    partition_group
        .iter()
        .map(|pg| format!("{pg}"))
        .collect::<Vec<_>>()
        .join("_")
}
