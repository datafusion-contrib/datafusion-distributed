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

pub fn display_stage_tree(stage: &ExecutionStage) -> Result<impl Display> {
    // walk up the stage tree and produce a StageDummyExecutionPlan
    // and then return a displayable representation of it using the tree renderer

    let mut visitor = StageDisplayVisitor::new();

    stage.visit(&mut visitor)?;
    let stage_plan = visitor.finish()?;

    let out = displayable(stage_plan.as_ref()).tree_render().to_string();
    Ok(out)
}

pub fn display_stage(stage: &ExecutionStage) -> Result<String> {
    const LTCORNER: &str = "┌"; // Left top corner
    const LDCORNER: &str = "└"; // Left bottom corner
    const VERTICAL: &str = "│"; // Vertical line
    const HORIZONTAL: &str = "─"; // Horizontal line

    let mut f = String::new();
    let mut indent = 0;

    let write_stage = |s: &ExecutionStage| -> Result<TreeNodeRecursion> {
        writeln!(f, "{}{} {}", LTCORNER, HORIZONTAL, s.name_with_children())?;

        let mut content = display_plan_with_partition_in_out(s.plan.as_ref())?
            .replace("\n", &format!("\n{}{}", VERTICAL, " ".repeat(indent)));

        content.insert_str(0, &" ".repeat(indent));

        writeln!(f, "{}{}", VERTICAL, content)?;
        writeln!(f, "{}{}", LDCORNER, HORIZONTAL)?;

        // count the number of leading spaces in the last line of content output
        let last_line = content.lines().last().unwrap_or("");
        indent = last_line.chars().take_while(|c| c.is_whitespace()).count();

        Ok(TreeNodeRecursion::Continue)
    };

    stage.apply(write_stage).map(|_| f)
}

#[derive(Debug, Clone)]
struct StageDummyExecutionPlan {
    name: String,
    content: String,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    properties: PlanProperties,
}

impl StageDummyExecutionPlan {
    fn new(name: String, content: String, inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::new(datafusion::arrow::datatypes::Schema::empty())),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        StageDummyExecutionPlan {
            name,
            content,
            inputs,
            properties,
        }
    }
}

// we will follow the pattern of how to walk up a tree and construct another
// as demonstrated in [`create::planner::StagePlanner`]
struct StageDisplayVisitor {
    stage_head: Option<ExecutionStage>,
    depth: usize,
    input_plans: Vec<Arc<dyn ExecutionPlan>>,
    input_plans_depth: usize,
}

impl StageDisplayVisitor {
    fn new() -> Self {
        StageDisplayVisitor {
            stage_head: None,
            depth: 0,
            input_plans: vec![],
            input_plans_depth: 0,
        }
    }

    fn finish(mut self) -> Result<Arc<dyn ExecutionPlan>> {
        let stage = self.stage_head.ok_or(internal_datafusion_err!(
            "No stage head found. Did you call rewrite() yes?"
        ))?;
        println!(
            "StageDisplayVisitor::finish: depth={} input_plans_depth={}",
            self.depth, self.input_plans_depth
        );

        if self.depth < self.input_plans_depth {
            let content = make_content(&stage);
            Ok(Arc::new(StageDummyExecutionPlan::new(
                stage.plan.name().to_owned(),
                content,
                self.input_plans,
            )) as Arc<dyn ExecutionPlan>)
        } else {
            Ok(self.input_plans.remove(0))
        }
    }
}

impl<'n> TreeNodeVisitor<'n> for StageDisplayVisitor {
    type Node = ExecutionStage;

    fn f_down(&mut self, _node: &'n Self::Node) -> Result<TreeNodeRecursion> {
        self.depth += 1;
        Ok(TreeNodeRecursion::Continue)
    }

    fn f_up(&mut self, stage: &'n Self::Node) -> Result<TreeNodeRecursion> {
        self.depth -= 1;
        self.stage_head = Some(stage.clone());

        let content = make_content(stage);

        if self.depth < self.input_plans_depth {
            let plan =
                StageDummyExecutionPlan::new(stage.name(), content, self.input_plans.clone());
            self.input_plans = vec![Arc::new(plan)];
        } else {
            let plan = StageDummyExecutionPlan::new(stage.name(), content, vec![]);
            self.input_plans.push(Arc::new(plan));
        }
        self.input_plans_depth = self.depth;

        Ok(TreeNodeRecursion::Continue)
    }
}

/// Format the content of a TreeRenderNode for display.   There is some
/// ugly formatting in here to ensure that we can get an abbreviated version
/// of the Stage plan inside the box that won't rely on newlines for delimiting.
fn make_content(stage: &ExecutionStage) -> String {
    let content = displayable(stage.plan.as_ref()).indent(false).to_string();

    // from [`datafusion::common::tree_node::TreeNodeVisitor`]
    const NODE_RENDER_WIDTH: usize = 29; // Width of each node's box
    const CONTENT_WIDTH: usize = NODE_RENDER_WIDTH - 4; // Width of the content inside the box

    let out = content
        .lines()
        .map(|line| {
            let content = line[..line.find(':').unwrap_or(line.len())].to_owned();
            format!(
                "{}{}",
                content,
                " ".repeat((CONTENT_WIDTH as i32 - content.len() as i32).max(0) as usize)
            )
        })
        .map(|line| line[..(CONTENT_WIDTH.min(line.len()))].to_owned())
        .collect::<Vec<_>>()
        .join("");
    out[..out.len() - 1].to_owned()
}

impl DisplayAs for StageDummyExecutionPlan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => Ok(()),
            DisplayFormatType::TreeRender => {
                write!(f, "{}", self.content)
            }
        }
    }
}

impl ExecutionPlan for StageDummyExecutionPlan {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StageDummyExecutionPlan::new(
            self.name.clone(),
            self.content.clone(),
            children,
        )) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        unimplemented!("StageDummyExecutionPlan does not support execution");
    }
}
