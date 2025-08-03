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
use std::{
    fmt::{Display, Formatter, Write},
    sync::Arc,
};

use datafusion::{
    common::{
        internal_datafusion_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor},
    },
    error::Result,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        displayable,
        execution_plan::{Boundedness, EmissionType},
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    },
};

use crate::common::util::display_plan_with_partition_in_out;

use super::ExecutionStage;

// Unicode box-drawing characters for creating borders and connections.
const LTCORNER: &str = "┌"; // Left top corner
const RTCORNER: &str = "┐"; // Right top corner
const LDCORNER: &str = "└"; // Left bottom corner
const RDCORNER: &str = "┘"; // Right bottom corner

const TMIDDLE: &str = "┬"; // Top T-junction (connects down)
const LMIDDLE: &str = "├"; // Left T-junction (connects right)
const DMIDDLE: &str = "┴"; // Bottom T-junction (connects up)

const VERTICAL: &str = "│"; // Vertical line
const HORIZONTAL: &str = "─"; // Horizontal line

impl DisplayAs for ExecutionStage {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
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
                    HORIZONTAL.repeat(50 - 7 - self.name.len())
                )?;
                let plan_str = display_plan_with_partition_in_out(self.plan.as_ref())
                    .map_err(|_| std::fmt::Error {})?;
                let plan_str = plan_str.replace(
                    '\n',
                    &format!("\n{}{}", "  ".repeat(self.depth()), VERTICAL),
                );
                writeln!(f, "{}{}{}", "  ".repeat(self.depth()), VERTICAL, plan_str)?;
                write!(
                    f,
                    "{}{}{}",
                    "  ".repeat(self.depth()),
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

    writeln!(f, "digraph G {{")?;
    writeln!(f, "  node[shape=rect];")?;

    let mut stack = vec![stage];

    while !stack.is_empty() {
        writeln!(f, "  subgraph cluster_{} {{];", stage.num)?;
        writeln!(f, "    label=\"{}\";", stage.name())?;
        writeln!(f, "    labeljust=l")?;
        writeln!(f, "  }}")?;
    }

    writeln!(f, "}}")?;
    Ok(f)
}
