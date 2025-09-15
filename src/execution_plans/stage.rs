use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::{ArrowFlightReadExec, ChannelResolver, PartitionIsolatorExec};
use datafusion::common::{
    internal_err,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    displayable, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
};
use datafusion::prelude::SessionContext;
use itertools::Itertools;
use rand::Rng;
use std::collections::VecDeque;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

/// A unit of isolation for a portion of a physical execution plan
/// that can be executed independently and across a network boundary.  
/// It implements [`ExecutionPlan`] and can be executed to produce a
/// stream of record batches.
///
/// An ExecutionTask is a finer grained unit of work compared to an StageExec.
/// One StageExec will create one or more ExecutionTasks
///
/// When an [`StageExec`] is execute()'d if will execute its plan and return a stream
/// of record batches.
///
/// If the stage has input stages, then it those input stages will be executed on remote resources
/// and will be provided the remainder of the stage tree.
///
/// For example if our stage tree looks like this:
///
/// ```text
///                       ┌─────────┐
///                       │ stage 1 │
///                       └───┬─────┘
///                           │
///                    ┌──────┴────────┐
///               ┌────┴────┐     ┌────┴────┐
///               │ stage 2 │     │ stage 3 │
///               └────┬────┘     └─────────┘
///                    │
///             ┌──────┴────────┐
///        ┌────┴────┐     ┌────┴────┐
///        │ stage 4 │     │ Stage 5 │
///        └─────────┘     └─────────┘                    
///
/// ```
///  
/// Then executing Stage 1 will run its plan locally.  Stage 1 has two inputs, Stage 2 and Stage 3.  We
/// know these will execute on remote resources.   As such the plan for Stage 1 must contain an
/// [`ArrowFlightReadExec`] node that will read the results of Stage 2 and Stage 3 and coalese the
/// results.
///
/// When Stage 1's [`ArrowFlightReadExec`] node is executed, it makes an ArrowFlightRequest to the
/// host assigned in the Stage.  It provides the following Stage tree serialilzed in the body of the
/// Arrow Flight Ticket:
///
/// ```text
///               ┌─────────┐     
///               │ Stage 2 │    
///               └────┬────┘   
///                    │
///             ┌──────┴────────┐
///        ┌────┴────┐     ┌────┴────┐
///        │ Stage 4 │     │ Stage 5 │
///        └─────────┘     └─────────┘                    
///
/// ```
///
/// The receiving ArrowFlightEndpoint will then execute Stage 2 and will repeat this process.
///
/// When Stage 4 is executed, it has no input tasks, so it is assumed that the plan included in that
/// Stage can complete on its own; its likely holding a leaf node in the overall phyysical plan and
/// producing data from a [`DataSourceExec`].
#[derive(Debug, Clone)]
pub struct StageExec {
    /// Our query_id
    pub query_id: Uuid,
    /// Our stage number
    pub num: usize,
    /// Our stage name
    pub name: String,
    /// The physical execution plan that this stage will execute.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The input stages to this stage
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    pub tasks: Vec<ExecutionTask>,
    /// tree depth of our location in the stage tree, used for display only
    pub depth: usize,
}

#[derive(Debug, Clone)]
pub struct ExecutionTask {
    /// The url of the worker that will execute this task.  A None value is interpreted as
    /// unassigned.
    pub url: Option<Url>,
    /// The partitions that we can execute from this plan
    pub partition_group: Vec<usize>,
}

impl StageExec {
    /// Creates a new `StageExec` with the given plan and inputs.  One task will be created
    /// responsible for partitions in the plan.
    pub fn new(
        query_id: Uuid,
        num: usize,
        plan: Arc<dyn ExecutionPlan>,
        inputs: Vec<Arc<StageExec>>,
    ) -> Self {
        let name = format!("Stage {:<3}", num);
        let partition_group = (0..plan.properties().partitioning.partition_count()).collect();
        StageExec {
            query_id,
            num,
            name,
            plan,
            inputs: inputs
                .into_iter()
                .map(|s| s as Arc<dyn ExecutionPlan>)
                .collect(),
            tasks: vec![ExecutionTask {
                partition_group,
                url: None,
            }],
            depth: 0,
        }
    }

    /// Recalculate the tasks for this stage based on the number of partitions in the plan
    /// and the maximum number of partitions per task.
    ///
    /// This will unset any worker assignments
    pub fn with_maximum_partitions_per_task(mut self, max_partitions_per_task: usize) -> Self {
        let partitions = self.plan.properties().partitioning.partition_count();

        self.tasks = (0..partitions)
            .chunks(max_partitions_per_task)
            .into_iter()
            .map(|partition_group| ExecutionTask {
                partition_group: partition_group.collect(),
                url: None,
            })
            .collect();
        self
    }

    /// Returns the name of this stage
    pub fn name(&self) -> String {
        format!("Stage {:<3}", self.num)
    }

    /// Returns an iterator over the child stages of this stage cast as &StageExec
    /// which can be useful
    pub fn child_stages_iter(&self) -> impl Iterator<Item = &StageExec> {
        self.inputs
            .iter()
            .filter_map(|s| s.as_any().downcast_ref::<StageExec>())
    }

    /// Returns the name of this stage including child stage numbers if any.
    pub fn name_with_children(&self) -> String {
        let child_str = if self.inputs.is_empty() {
            "".to_string()
        } else {
            format!(
                " Child Stages:[{}] ",
                self.child_stages_iter()
                    .map(|s| format!("{}", s.num))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        format!("Stage {:<3}{}", self.num, child_str)
    }

    pub fn try_assign(self, channel_resolver: &impl ChannelResolver) -> Result<Self> {
        let urls: Vec<Url> = channel_resolver.get_urls()?;
        if urls.is_empty() {
            return internal_err!("No URLs found in ChannelManager");
        }

        Ok(self)
    }

    fn try_assign_urls(&self, urls: &[Url]) -> Result<Self> {
        let assigned_children = self
            .child_stages_iter()
            .map(|child| {
                child
                    .clone() // TODO: avoid cloning if possible
                    .try_assign_urls(urls)
                    .map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
            })
            .collect::<Result<Vec<_>>>()?;

        // pick a random starting position
        let mut rng = rand::thread_rng();
        let start_idx = rng.gen_range(0..urls.len());

        let assigned_tasks = self
            .tasks
            .iter()
            .enumerate()
            .map(|(i, task)| ExecutionTask {
                partition_group: task.partition_group.clone(),
                url: Some(urls[(start_idx + i) % urls.len()].clone()),
            })
            .collect::<Vec<_>>();

        let assigned_stage = StageExec {
            query_id: self.query_id,
            num: self.num,
            name: self.name.clone(),
            plan: self.plan.clone(),
            inputs: assigned_children,
            tasks: assigned_tasks,
            depth: self.depth,
        };

        Ok(assigned_stage)
    }
}

impl ExecutionPlan for StageExec {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(StageExec {
            query_id: self.query_id,
            num: self.num,
            name: self.name.clone(),
            plan: self.plan.clone(),
            inputs: children,
            tasks: self.tasks.clone(),
            depth: self.depth,
        }))
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.plan.properties()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        let channel_resolver = get_distributed_channel_resolver(context.session_config())?;

        let assigned_stage = self
            .try_assign_urls(&channel_resolver.get_urls()?)
            .map(Arc::new)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // insert the stage into the context so that ExecutionPlan nodes
        // that care about the stage can access it
        let config = context
            .session_config()
            .clone()
            .with_extension(assigned_stage.clone());

        let new_ctx =
            SessionContext::new_with_config_rt(config, context.runtime_env().clone()).task_ctx();

        assigned_stage.plan.execute(partition, new_ctx)
    }
}

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

impl StageExec {
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

impl DisplayAs for StageExec {
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
            DisplayFormatType::TreeRender => write!(f, "{}", format_tasks_for_stage(&self.tasks),),
        }
    }
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
            partitions[*p] = format!("p{i}")
        }
        result += &format!("t{i}:[{}] ", partitions.join(","));
    }
    result
}

// num_colors must agree with the colorscheme selected from
// https://graphviz.org/doc/info/colors.html
const NUM_COLORS: usize = 6;
const COLOR_SCHEME: &str = "spectral6";

pub fn display_stage_graphviz(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
    let mut f = String::new();

    writeln!(
        f,
        "digraph G {{
  rankdir=BT
  edge[colorscheme={}, penwidth=2.0]
  splines=false
",
        COLOR_SCHEME
    )?;

    // draw all tasks first
    plan.apply(|node| {
        let stage = node
            .as_any()
            .downcast_ref::<StageExec>()
            .expect("Expected StageExec");
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
            .downcast_ref::<StageExec>()
            .expect("Expected StageExec");

        for child_stage in stage.child_stages_iter() {
            for task in stage.tasks.iter() {
                for child_task in child_stage.tasks.iter() {
                    let edges = display_inter_task_edges(stage, task, child_stage, child_task)?;
                    writeln!(
                        f,
                        "// edges from child stage {} task {} to stage {} task {}\n {}",
                        child_stage.num,
                        format_pg(&child_task.partition_group),
                        stage.num,
                        format_pg(&task.partition_group),
                        edges
                    )?;
                }
            }
        }

        Ok(TreeNodeRecursion::Continue)
    })?;

    writeln!(f, "}}")?;

    Ok(f)
}

pub fn display_single_task(stage: &StageExec, partition_group: &[usize]) -> Result<String> {
    let mut f = String::new();
    writeln!(
        f,
        "
  subgraph \"cluster_stage_{}_task_{}_margin\" {{
    style=invis
    margin=20.0
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
        format_pg(partition_group),
        stage.num,
        format_pg(partition_group)
    )?;

    // draw all plans
    // we need to label the nodes including depth to uniquely identify them within this task
    // the tree node API provides depth first traversal, but we need breadth to align with
    // how we will draw edges below, so we'll do that.
    let mut queue = VecDeque::from([&stage.plan]);
    let mut index = 0;
    while let Some(plan) = queue.pop_front() {
        index += 1;
        let p = display_single_plan(plan.as_ref(), stage.num, partition_group, index)?;
        writeln!(f, "{}", p)?;
        for child in plan.children().iter() {
            queue.push_back(child);
        }
    }

    // draw edges between the plan nodes
    type PlanWithParent<'a> = (
        &'a Arc<dyn ExecutionPlan>,
        Option<&'a Arc<dyn ExecutionPlan>>,
        usize,
    );
    let mut queue: VecDeque<PlanWithParent> = VecDeque::from([(&stage.plan, None, 0usize)]);
    let mut found_isolator = false;
    index = 0;
    while let Some((plan, maybe_parent, parent_idx)) = queue.pop_front() {
        index += 1;
        if plan
            .as_any()
            .downcast_ref::<PartitionIsolatorExec>()
            .is_some()
        {
            found_isolator = true;
        }
        if let Some(parent) = maybe_parent {
            let partitions = plan.output_partitioning().partition_count();
            for i in 0..partitions {
                let mut style = "";
                if plan
                    .as_any()
                    .downcast_ref::<PartitionIsolatorExec>()
                    .is_some()
                    && i >= partition_group.len()
                {
                    style = "[style=dotted, label=empty]";
                } else if found_isolator && !partition_group.contains(&i) {
                    style = "[style=invis]";
                }

                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{}:n -> {}_{}_{}_{}:b{}:s {}[color={}]",
                    plan.name(),
                    stage.num,
                    node_format_pg(partition_group),
                    index,
                    i,
                    parent.name(),
                    stage.num,
                    node_format_pg(partition_group),
                    parent_idx,
                    i,
                    style,
                    i % NUM_COLORS + 1
                )?;
            }
        }

        for child in plan.children().iter() {
            queue.push_back((child, Some(plan), index));
        }
    }
    writeln!(f, "  }}")?;
    writeln!(f, "  }}")?;

    Ok(f)
}

/// We want to display a single plan as a three row table with the top and bottom being
/// graphvis ports.
///
/// We accept an index to make the node name unique in the graphviz output within
/// a plan at the same depth
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
    partition_group: &[usize],
    index: usize,
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
        index
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
    stage: &StageExec,
    task: &ExecutionTask,
    child_stage: &StageExec,
    child_task: &ExecutionTask,
) -> Result<String> {
    let mut f = String::new();

    let mut found_isolator = false;
    let mut queue = VecDeque::from([&stage.plan]);
    let mut index = 0;
    while let Some(plan) = queue.pop_front() {
        index += 1;
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
            // draw the edges to this node pulling data up from its child
            for p in 0..plan.output_partitioning().partition_count() {
                let mut style = "";
                if found_isolator && !task.partition_group.contains(&p) {
                    style = "[style=invis]";
                }

                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{}:n -> {}_{}_{}_{}:b{}:s {} [color={}]",
                    child_stage.plan.name(),
                    child_stage.num,
                    node_format_pg(&child_task.partition_group),
                    1, // the repartition exec is always the first node in the plan
                    p,
                    plan.name(),
                    stage.num,
                    node_format_pg(&task.partition_group),
                    index,
                    p,
                    style,
                    p % NUM_COLORS + 1
                )?;
            }
        }
        for child in plan.children().iter() {
            queue.push_back(child);
        }
    }

    Ok(f)
}

fn format_pg(partition_group: &[usize]) -> String {
    partition_group
        .iter()
        .map(|pg| format!("{pg}"))
        .collect::<Vec<_>>()
        .join("_")
}

fn node_format_pg(partition_group: &[usize]) -> String {
    partition_group
        .iter()
        .map(|pg| format!("{pg}"))
        .collect::<Vec<_>>()
        .join("_")
}
