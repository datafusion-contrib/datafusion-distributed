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

impl DisplayAs for ExecutionStage {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "{}", self.name)
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
