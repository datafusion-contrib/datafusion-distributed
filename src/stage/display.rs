use super::ExecutionStage;
use crate::plan::PartitionIsolatorExec;
use crate::{
    task::{format_pg, ExecutionTask},
    ArrowFlightReadExec,
};
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};
use datafusion::{
    error::Result,
    physical_plan::{DisplayAs, DisplayFormatType},
};
use itertools::Itertools;
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
use std::fmt::Write;

// Unicode box-drawing characters for creating borders and connections.
const LTCORNER: &str = "┌"; // Left top corner
const LDCORNER: &str = "└"; // Left bottom corner
const VERTICAL: &str = "│"; // Vertical line
const HORIZONTAL: &str = "─"; // Horizontal line

impl ExecutionStage {
    fn format(&self, plan: &dyn ExecutionPlan, indent: usize, f: &mut String) -> std::fmt::Result {
        let mut node_str = displayable(plan).one_line().to_string();
        node_str.pop();
        write!(f, "{} {node_str}", " ".repeat(indent))?;

        if let Some(ArrowFlightReadExec::Ready(ready)) =
            plan.as_any().downcast_ref::<ArrowFlightReadExec>()
        {
            let Some(input_stage) = &self.child_stages_iter().find(|v| v.num == ready.stage_num)
            else {
                writeln!(f, "Wrong partition number {}", ready.stage_num)?;
                return Ok(());
            };
            let tasks = input_stage.tasks.len();
            let partitions = plan.output_partitioning().partition_count();
            let stage = ready.stage_num;
            write!(
                f,
                " input_stage={stage}, input_partitions={partitions}, input_tasks={tasks}",
            )?;
        }

        if plan.as_any().is::<PartitionIsolatorExec>() {
            write!(f, " {}", format_tasks_for_partition_isolator(&self.tasks))?;
        }
        writeln!(f)?;

        for child in plan.children() {
            self.format(child.as_ref(), indent + 2, f)?;
        }
        Ok(())
    }
}

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
                    format_tasks_for_stage(&self.tasks),
                )?;

                let mut plan_str = String::new();
                self.format(self.plan.as_ref(), 0, &mut plan_str)?;
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

pub fn display_stage_graphviz(stage: &ExecutionStage) -> Result<String> {
    let mut f = String::new();

    let num_colors = 5; // this should aggree with the colorscheme chosen from
                        // https://graphviz.org/doc/info/colors.html
    let colorscheme = "spectral5";

    writeln!(f, "digraph G {{")?;
    writeln!(f, "  node[shape=rect];")?;
    writeln!(f, "  rankdir=BT;")?;
    writeln!(f, "  ranksep=2;")?;
    writeln!(f, "  edge[colorscheme={},penwidth=2.0];", colorscheme)?;

    // we'll keep a stack of stage ref, parrent stage ref
    let mut stack: Vec<(&ExecutionStage, Option<&ExecutionStage>)> = vec![(stage, None)];

    while let Some((stage, parent)) = stack.pop() {
        writeln!(f, "  subgraph cluster_{} {{", stage.num)?;
        writeln!(f, "    node[shape=record];")?;
        writeln!(f, "    label=\"{}\";", stage.name())?;
        writeln!(f, "    labeljust=r;")?;
        writeln!(f, "    labelloc=b;")?; // this will put the label at the top as our
                                         // rankdir=BT

        stage.tasks.iter().try_for_each(|task| {
            let lab = task
                .partition_group
                .iter()
                .map(|p| format!("<p{}>{}", p, p))
                .collect::<Vec<_>>()
                .join("|");
            writeln!(
                f,
                "    \"{}_{}\"[label = \"{}\"]",
                stage.num,
                format_pg(&task.partition_group),
                lab,
            )?;

            if let Some(our_parent) = parent {
                our_parent.tasks.iter().try_for_each(|ptask| {
                    task.partition_group.iter().try_for_each(|partition| {
                        ptask.partition_group.iter().try_for_each(|ppartition| {
                            writeln!(
                                f,
                                "    \"{}_{}\":p{}:n -> \"{}_{}\":p{}:s[color={}]",
                                stage.num,
                                format_pg(&task.partition_group),
                                partition,
                                our_parent.num,
                                format_pg(&ptask.partition_group),
                                ppartition,
                                (partition) % num_colors + 1
                            )
                        })
                    })
                })?;
            }

            Ok::<(), std::fmt::Error>(())
        })?;

        // now we try to force the left right nature of tasks to be honored
        writeln!(f, "    {{")?;
        writeln!(f, "         rank = same;")?;
        stage.tasks.iter().try_for_each(|task| {
            writeln!(
                f,
                "         \"{}_{}\"",
                stage.num,
                format_pg(&task.partition_group)
            )?;

            Ok::<(), std::fmt::Error>(())
        })?;
        writeln!(f, "    }}")?;
        // combined with rank = same, the invisible edges will force the tasks to be
        // laid out in a single row within the stage
        for i in 0..stage.tasks.len() - 1 {
            writeln!(
                f,
                "    \"{}_{}\":w -> \"{}_{}\":e[style=invis]",
                stage.num,
                format_pg(&stage.tasks[i].partition_group),
                stage.num,
                format_pg(&stage.tasks[i + 1].partition_group),
            )?;
        }

        // add a node for the plan, its way too big!   Alternatives to add it?
        /*writeln!(
            f,
            "    \"{}_plan\"[label = \"{}\", shape=box];",
            stage.num,
            displayable(stage.plan.as_ref()).indent(false)
        )?;
        */

        writeln!(f, "  }}")?;

        for child in stage.child_stages_iter() {
            stack.push((child, Some(stage)));
        }
    }

    writeln!(f, "}}")?;
    Ok(f)
}

fn format_tasks_for_stage(tasks: &[ExecutionTask]) -> String {
    let mut result = "Tasks: ".to_string();
    for (i, t) in tasks.iter().enumerate() {
        result += &format!("t{i}:[");
        result += &t.partition_group.iter().map(|v| format!("p{v}")).join(",");
        result += "] "
    }
    result
}

fn format_tasks_for_partition_isolator(tasks: &[ExecutionTask]) -> String {
    let mut result = "Tasks: ".to_string();
    let mut partitions = vec![];
    for t in tasks.iter() {
        partitions.extend(vec!["__".to_string(); t.partition_group.len()])
    }
    for (i, t) in tasks.iter().enumerate() {
        let mut partitions = partitions.clone();
        for (i, p) in t.partition_group.iter().enumerate() {
            partitions[*p as usize] = format!("p{i}")
        }
        result += &format!("t{i}:[{}] ", partitions.join(","));
    }
    result
}
