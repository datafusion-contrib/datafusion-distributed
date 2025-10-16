use crate::execution_plans::{DistributedExec, NetworkCoalesceExec};
use crate::{NetworkShuffleExec, PartitionIsolatorExec};
use datafusion::common::plan_err;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use itertools::{Either, Itertools};
use std::collections::VecDeque;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

/// A unit of isolation for a portion of a physical execution plan
/// that can be executed independently and across a network boundary.  
/// It implements [`ExecutionPlan`] and can be executed to produce a
/// stream of record batches.
///
/// An ExecutionTask is a finer grained unit of work compared to an ExecutionStage.
/// One ExecutionStage will create one or more ExecutionTasks
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
/// [`NetworkShuffleExec`] node that will read the results of Stage 2 and Stage 3 and coalese the
/// results.
///
/// When Stage 1's [`NetworkShuffleExec`] node is executed, it makes an ArrowFlightRequest to the
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
pub struct Stage {
    /// Our query_id
    pub(crate) query_id: Uuid,
    /// Our stage number
    pub(crate) num: usize,
    /// The physical execution plan that this stage will execute.
    pub(crate) plan: MaybeEncodedPlan,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    pub tasks: Vec<ExecutionTask>,
}

#[derive(Debug, Clone)]
pub struct ExecutionTask {
    /// The url of the worker that will execute this task.  A None value is interpreted as
    /// unassigned.
    pub(crate) url: Option<Url>,
}

/// An [ExecutionPlan] that can be either:
/// - Decoded: the inner [ExecutionPlan] is stored as-is.
/// - Encoded: the inner [ExecutionPlan] is stored as protobuf [Bytes]. Storing it this way allow us
///   to thread it through the project and eventually send it through gRPC in a zero copy manner.
#[derive(Debug, Clone)]
pub(crate) enum MaybeEncodedPlan {
    /// The decoded [ExecutionPlan].
    Decoded(Arc<dyn ExecutionPlan>),
    /// A protobuf encoded version of the [ExecutionPlan]. The inner [Bytes] represent the full
    /// input [ExecutionPlan] encoded in protobuf format.
    ///
    /// By keeping it encoded, we avoid encoding/decoding it unnecessarily in parts of the project
    /// that only need it to be decoded.
    Encoded(Bytes),
}

impl MaybeEncodedPlan {
    pub(crate) fn to_encoded(&self, codec: &dyn PhysicalExtensionCodec) -> Result<Self> {
        Ok(match self {
            Self::Decoded(plan) => Self::Encoded(
                PhysicalPlanNode::try_from_physical_plan(Arc::clone(plan), codec)?
                    .encode_to_vec()
                    .into(),
            ),
            Self::Encoded(plan) => Self::Encoded(plan.clone()),
        })
    }

    pub(crate) fn decoded(&self) -> Result<&Arc<dyn ExecutionPlan>> {
        match self {
            MaybeEncodedPlan::Decoded(v) => Ok(v),
            MaybeEncodedPlan::Encoded(_) => plan_err!("Expected plan to be in a decoded state"),
        }
    }

    pub(crate) fn encoded(&self) -> Result<&Bytes> {
        match self {
            MaybeEncodedPlan::Decoded(_) => plan_err!("Expected plan to be in a encoded state"),
            MaybeEncodedPlan::Encoded(v) => Ok(v),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DistributedTaskContext {
    pub task_index: usize,
    pub task_count: usize,
}

impl DistributedTaskContext {
    pub fn from_ctx(ctx: &Arc<TaskContext>) -> Arc<Self> {
        ctx.session_config()
            .get_extension::<Self>()
            .unwrap_or(Arc::new(DistributedTaskContext {
                task_index: 0,
                task_count: 1,
            }))
    }
}

impl Stage {
    /// Creates a new `ExecutionStage` with the given plan and inputs.  One task will be created
    /// responsible for partitions in the plan.
    pub(crate) fn new(
        query_id: Uuid,
        num: usize,
        plan: Arc<dyn ExecutionPlan>,
        n_tasks: usize,
    ) -> Self {
        Self {
            query_id,
            num,
            plan: MaybeEncodedPlan::Decoded(plan),
            tasks: vec![ExecutionTask { url: None }; n_tasks],
        }
    }
}

use crate::distributed_physical_optimizer_rule::{NetworkBoundary, NetworkBoundaryExt};
use crate::rewrite_distributed_plan_with_metrics;
use bytes::Bytes;
use datafusion::common::DataFusionError;
use datafusion::physical_expr::Partitioning;
use datafusion_proto::physical_plan::{AsExecutionPlan, PhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
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

/// Context used to display a [Stage], tasks, and plans.
#[derive(Debug, Clone, Default)]
pub struct DisplayCtx {
    show_metrics: bool,
}

impl DisplayCtx {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn with_metrics(mut self) -> Self {
        self.show_metrics = true;
        self
    }
}

/// explain_analyze renders an [ExecutionPlan] with metrics.
pub fn explain_analyze(executed: Arc<dyn ExecutionPlan>) -> Result<String, DataFusionError> {
    match executed.as_any().downcast_ref::<DistributedExec>() {
        None => Ok(DisplayableExecutionPlan::with_metrics(executed.as_ref())
            .indent(true)
            .to_string()),
        Some(_) => {
            let executed = rewrite_distributed_plan_with_metrics(executed.clone())?;
            Ok(display_plan_ascii(executed.as_ref()))
        }
    }
}

// Unicode box-drawing characters for creating borders and connections.
const LTCORNER: &str = "┌"; // Left top corner
const LDCORNER: &str = "└"; // Left bottom corner
const VERTICAL: &str = "│"; // Vertical line
const HORIZONTAL: &str = "─"; // Horizontal line
pub fn display_plan_ascii(plan: &dyn ExecutionPlan) -> String {
    if let Some(plan) = plan.as_any().downcast_ref::<DistributedExec>() {
        let mut f = String::new();
        let display_ctx = plan.display_ctx.clone().unwrap_or(DisplayCtx::default());
        display_ascii(Either::Left(plan), 0, display_ctx, &mut f).unwrap();
        f
    } else {
        displayable(plan).indent(true).to_string()
    }
}

fn display_ascii(
    stage: Either<&DistributedExec, &Stage>,
    depth: usize,
    display_ctx: DisplayCtx,
    f: &mut String,
) -> std::fmt::Result {
    let plan = match stage {
        Either::Left(distributed_exec) => distributed_exec.children().first().unwrap(),
        Either::Right(stage) => {
            let MaybeEncodedPlan::Decoded(plan) = &stage.plan else {
                return write!(f, "StageExec: encoded input plan");
            };
            plan
        }
    };
    match stage {
        Either::Left(_) => {
            writeln!(
                f,
                "{}{}{} DistributedExec {} {}",
                "  ".repeat(depth),
                LTCORNER,
                HORIZONTAL.repeat(5),
                HORIZONTAL.repeat(2),
                format_tasks_for_stage(1, plan)
            )?;
        }
        Either::Right(stage) => {
            writeln!(
                f,
                "{}{}{} Stage {} {} {}",
                "  ".repeat(depth),
                LTCORNER,
                HORIZONTAL.repeat(5),
                stage.num,
                HORIZONTAL.repeat(2),
                format_tasks_for_stage(stage.tasks.len(), plan)
            )?;
        }
    }

    let mut plan_str = String::new();
    display_inner_ascii(plan, 0, display_ctx.clone(), &mut plan_str)?;
    let plan_str = plan_str
        .split('\n')
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>()
        .join(&format!("\n{}{}", "  ".repeat(depth), VERTICAL));
    writeln!(f, "{}{}{}", "  ".repeat(depth), VERTICAL, plan_str)?;
    writeln!(
        f,
        "{}{}{}",
        "  ".repeat(depth),
        LDCORNER,
        HORIZONTAL.repeat(50)
    )?;
    for input_stage in find_input_stages(plan.as_ref()) {
        display_ascii(
            Either::Right(input_stage),
            depth + 1,
            display_ctx.clone(),
            f,
        )?;
    }
    Ok(())
}

fn display_inner_ascii(
    plan: &Arc<dyn ExecutionPlan>,
    indent: usize,
    display_ctx: DisplayCtx,
    f: &mut String,
) -> std::fmt::Result {
    let node_str = match display_ctx.show_metrics {
        true => DisplayableExecutionPlan::with_metrics(plan.as_ref())
            .one_line()
            .to_string(),
        false => displayable(plan.as_ref()).one_line().to_string(),
    };
    writeln!(f, "{} {node_str}", " ".repeat(indent))?;

    if plan.is_network_boundary() {
        return Ok(());
    }

    for child in plan.children() {
        display_inner_ascii(child, indent + 2, display_ctx.clone(), f)?;
    }
    Ok(())
}

fn format_tasks_for_stage(n_tasks: usize, head: &Arc<dyn ExecutionPlan>) -> String {
    let partitioning = head.properties().output_partitioning();
    let input_partitions = partitioning.partition_count();
    let hash_shuffle = matches!(partitioning, Partitioning::Hash(_, _));
    let mut result = "Tasks: ".to_string();
    let mut off = 0;
    for i in 0..n_tasks {
        result += &format!("t{i}:[");
        result += &(off..(off + input_partitions))
            .map(|v| format!("p{v}"))
            .join(",");
        result += "] ";
        off += if hash_shuffle { 0 } else { input_partitions }
    }
    result
}

// num_colors must agree with the colorscheme selected from
// https://graphviz.org/doc/info/colors.html
const NUM_COLORS: usize = 6;
const COLOR_SCHEME: &str = "spectral6";

/// This will render a regular or distributed datafusion plan as
/// Graphviz dot format.
/// You can view them on https://vis-js.com
///
/// Or it is often useful to expertiment with plan output using
/// https://datafusion-fiddle.vercel.app/
pub fn display_plan_graphviz(plan: Arc<dyn ExecutionPlan>) -> Result<String> {
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

    if plan.as_any().is::<DistributedExec>() {
        let mut max_num = 0;
        let mut all_stages = find_all_stages(&plan)
            .into_iter()
            .inspect(|v| max_num = max_num.max(v.num))
            .collect::<Vec<_>>();
        let head_stage = Stage {
            query_id: Default::default(),
            num: max_num + 1,
            plan: MaybeEncodedPlan::Decoded(plan.clone()),
            tasks: vec![ExecutionTask { url: None }],
        };
        all_stages.insert(0, &head_stage);

        // draw all tasks first
        for stage in &all_stages {
            for i in 0..stage.tasks.iter().len() {
                let p = display_single_task(stage, i)?;
                writeln!(f, "{}", p)?;
            }
        }
        // now draw edges between the tasks
        for stage in &all_stages {
            for input_stage in find_input_stages(stage.plan.decoded()?.as_ref()) {
                for task_i in 0..stage.tasks.len() {
                    for input_task_i in 0..input_stage.tasks.len() {
                        let edges =
                            display_inter_task_edges(stage, task_i, input_stage, input_task_i)?;
                        writeln!(
                            f,
                            "// edges from child stage {} task {} to stage {} task {}\n {}",
                            input_stage.num, input_task_i, stage.num, task_i, edges
                        )?;
                    }
                }
            }
        }
    } else {
        // single plan, not a stage tree
        writeln!(f, "node[shape=none]")?;
        let p = display_plan(&plan, 0, 1, 0)?;
        writeln!(f, "{}", p)?;
    }

    writeln!(f, "}}")?;

    Ok(f)
}

fn display_single_task(stage: &Stage, task_i: usize) -> Result<String> {
    let plan = stage.plan.decoded()?;
    let partition_group =
        build_partition_group(task_i, plan.output_partitioning().partition_count());

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
    label = \"Stage {} Task {} Partitions {}\"
    labeljust=r
    labelloc=b

    node[shape=none]

",
        stage.num,
        task_i,
        stage.num,
        task_i,
        stage.num,
        task_i,
        format_pg(&partition_group)
    )?;

    writeln!(
        f,
        "{}",
        display_plan(plan, task_i, stage.tasks.len(), stage.num)?
    )?;
    writeln!(f, "  }}")?;
    writeln!(f, "  }}")?;

    Ok(f)
}

fn display_plan(
    plan: &Arc<dyn ExecutionPlan>,
    task_i: usize,
    n_tasks: usize,
    stage_num: usize,
) -> Result<String> {
    // draw all plans
    // we need to label the nodes including depth to uniquely identify them within this task
    // the tree node API provides depth first traversal, but we need breadth to align with
    // how we will draw edges below, so we'll do that.
    let mut queue = VecDeque::from([plan]);
    let mut node_index = 0;

    let mut f = String::new();
    while let Some(plan) = queue.pop_front() {
        node_index += 1;
        let p = display_single_plan(plan.as_ref(), stage_num, task_i, node_index)?;
        writeln!(f, "{}", p)?;

        if plan.is_network_boundary() {
            continue;
        }
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
    let mut queue: VecDeque<PlanWithParent> = VecDeque::from([(plan, None, 0usize)]);
    let mut isolator_partition_group = None;
    node_index = 0;
    while let Some((plan, maybe_parent, parent_idx)) = queue.pop_front() {
        node_index += 1;
        if let Some(node) = plan.as_any().downcast_ref::<PartitionIsolatorExec>() {
            isolator_partition_group = Some(PartitionIsolatorExec::partition_group(
                node.input().output_partitioning().partition_count(),
                task_i,
                n_tasks,
            ));
        }
        if let Some(parent) = maybe_parent {
            let output_partitions = plan.output_partitioning().partition_count();

            for i in 0..output_partitions {
                let mut style = "";
                if plan.as_any().is::<PartitionIsolatorExec>() {
                    if i >= isolator_partition_group.as_ref().map_or(0, |v| v.len()) {
                        style = "[style=dotted, label=empty]";
                    }
                } else if let Some(partition_group) = &isolator_partition_group {
                    if !partition_group.contains(&i) {
                        style = "[style=invis]";
                    }
                }

                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{}:n -> {}_{}_{}_{}:b{}:s {}[color={}]",
                    plan.name(),
                    stage_num,
                    task_i,
                    node_index,
                    i,
                    parent.name(),
                    stage_num,
                    task_i,
                    parent_idx,
                    i,
                    style,
                    i % NUM_COLORS + 1
                )?;
            }
        }

        if plan.as_ref().is_network_boundary() {
            continue;
        }

        for child in plan.children() {
            queue.push_back((child, Some(plan), node_index));
        }
    }
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
///       NetworkShuffleExec [label=<
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
///                         <TD>NetworkShuffleExec</TD>
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
    plan: &(dyn ExecutionPlan + 'static),
    stage_num: usize,
    task_i: usize,
    node_index: usize,
) -> Result<String> {
    let mut f = String::new();
    let output_partitions = plan.output_partitioning().partition_count();
    let input_partitions = if plan.is_network_boundary() {
        output_partitions
    } else if let Some(child) = plan.children().first() {
        child.output_partitioning().partition_count()
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
        task_i,
        node_index
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
    stage: &Stage,
    task_i: usize,
    input_stage: &Stage,
    input_task_i: usize,
) -> Result<String> {
    let MaybeEncodedPlan::Decoded(plan) = &stage.plan else {
        return plan_err!("The inner plan of a stage was encoded.");
    };
    let MaybeEncodedPlan::Decoded(input_plan) = &input_stage.plan else {
        return plan_err!("The inner plan of a stage was encoded.");
    };
    let mut f = String::new();

    let mut queue = VecDeque::from([plan]);
    let mut index = 0;
    while let Some(plan) = queue.pop_front() {
        index += 1;
        if let Some(node) = plan.as_any().downcast_ref::<NetworkShuffleExec>() {
            if node.input_stage().is_none_or(|v| v.num != input_stage.num) {
                continue;
            }
            // draw the edges to this node pulling data up from its child
            let output_partitions = plan.output_partitioning().partition_count();
            for p in 0..output_partitions {
                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{}:n -> {}_{}_{}_{}:b{}:s [color={}]",
                    input_plan.name(),
                    input_stage.num,
                    input_task_i,
                    1, // the repartition exec is always the first node in the plan
                    p + (task_i * output_partitions),
                    plan.name(),
                    stage.num,
                    task_i,
                    index,
                    p,
                    p % NUM_COLORS + 1
                )?;
            }
            continue;
        } else if let Some(node) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() {
            if node.input_stage().is_none_or(|v| v.num != input_stage.num) {
                continue;
            }
            // draw the edges to this node pulling data up from its child
            let output_partitions = plan.output_partitioning().partition_count();
            let input_partitions_per_task = output_partitions / input_stage.tasks.len();
            for p in 0..input_partitions_per_task {
                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{}:n -> {}_{}_{}_{}:b{}:s [color={}]",
                    input_plan.name(),
                    input_stage.num,
                    input_task_i,
                    1, // the repartition exec is always the first node in the plan
                    p,
                    plan.name(),
                    stage.num,
                    task_i,
                    index,
                    p + (input_task_i * input_partitions_per_task),
                    p % NUM_COLORS + 1
                )?;
            }
            continue;
        }

        for child in plan.children() {
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

fn build_partition_group(task_i: usize, partitions: usize) -> Vec<usize> {
    ((task_i * partitions)..((task_i + 1) * partitions)).collect::<Vec<_>>()
}

fn find_input_stages(plan: &dyn ExecutionPlan) -> Vec<&Stage> {
    let mut result = vec![];
    for child in plan.children() {
        if let Some(plan) = child.as_network_boundary() {
            if let Some(stage) = plan.input_stage() {
                result.push(stage);
            }
        } else {
            result.extend(find_input_stages(child.as_ref()));
        }
    }
    result
}

pub(crate) fn find_all_stages(plan: &Arc<dyn ExecutionPlan>) -> Vec<&Stage> {
    let mut result = vec![];
    if let Some(plan) = plan.as_network_boundary() {
        if let Some(stage) = plan.input_stage() {
            result.push(stage);
        }
    }
    for child in plan.children() {
        result.extend(find_all_stages(child));
    }
    result
}
