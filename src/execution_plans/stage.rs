use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::execution_plans::NetworkCoalesceExec;
use crate::{ChannelResolver, NetworkShuffleExec, PartitionIsolatorExec};
use datafusion::common::{exec_err, internal_datafusion_err, internal_err, plan_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, displayable,
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
    pub inputs: Vec<InputStage>,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    pub tasks: Vec<ExecutionTask>,
    /// tree depth of our location in the stage tree, used for display only
    pub depth: usize,
}

/// A [StageExec] that is the input of another [StageExec].
///
/// It can be either:
/// - Decoded: the inner [StageExec] is stored as-is.
/// - Encoded: the inner [StageExec] is stored as protobuf [Bytes]. Storing it this way allow us
///   to thread it through the project and eventually send it through gRPC in a zero copy manner.
#[derive(Debug, Clone)]
pub enum InputStage {
    /// The decoded [StageExec]. Unfortunately, this cannot be an `Arc<StageExec>`, because at
    /// some point we need to upcast `&Arc<StageExec>` to `&Arc<dyn ExecutionPlan>`, and Rust
    /// compiler does not allow it.
    ///
    /// This is very annoying because it forces us to store it like an `Arc<dyn ExecutionPlan>`
    /// here even though we know this can only be `Arc<StageExec>`. For this reason
    /// [StageExec::from_dyn] was introduced for casting it back to [StageExec].
    Decoded(Arc<dyn ExecutionPlan>),
    /// A protobuf encoded version of the [InputStage]. The inner [Bytes] represent the full
    /// input [StageExec] encoded in protobuf format.
    ///
    /// By keeping it encoded, we avoid encoding/decoding it unnecessarily in parts of the project
    /// that do not need it. Only the Stage num and the [ExecutionTask]s are left decoded,
    /// as typically those are the only things needed by the network boundaries. The [Bytes] can be
    /// just passed through in a zero copy manner.
    Encoded {
        num: usize,
        tasks: Vec<ExecutionTask>,
        proto: Bytes,
    },
}

impl InputStage {
    pub fn num(&self) -> usize {
        match self {
            InputStage::Decoded(v) => StageExec::from_dyn(v).num,
            InputStage::Encoded { num, .. } => *num,
        }
    }

    pub fn tasks(&self) -> &[ExecutionTask] {
        match self {
            InputStage::Decoded(v) => &StageExec::from_dyn(v).tasks,
            InputStage::Encoded { tasks, .. } => tasks,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionTask {
    /// The url of the worker that will execute this task.  A None value is interpreted as
    /// unassigned.
    pub url: Option<Url>,
}

#[derive(Debug, Clone, Default)]
pub struct DistributedTaskContext {
    pub task_index: usize,
}

impl DistributedTaskContext {
    pub fn new(task_index: usize) -> Self {
        Self { task_index }
    }

    pub fn from_ctx(ctx: &Arc<TaskContext>) -> Arc<Self> {
        ctx.session_config()
            .get_extension::<Self>()
            .unwrap_or_default()
    }
}

impl StageExec {
    /// Dangerous way of accessing a [StageExec] out of an `&Arc<dyn ExecutionPlan>`.
    /// See [InputStage::Decoded] docs for more details about why panicking here is preferred.
    pub(crate) fn from_dyn(plan: &Arc<dyn ExecutionPlan>) -> &Self {
        plan.as_any()
            .downcast_ref()
            .expect("Programming Error: expected Arc<dyn ExecutionPlan> to be of type StageExec")
    }

    /// Creates a new `ExecutionStage` with the given plan and inputs.  One task will be created
    /// responsible for partitions in the plan.
    pub(crate) fn new(
        query_id: Uuid,
        num: usize,
        plan: Arc<dyn ExecutionPlan>,
        inputs: Vec<StageExec>,
        n_tasks: usize,
    ) -> Self {
        StageExec {
            name: format!("Stage {:<3}", num),
            query_id,
            num,
            plan,
            inputs: inputs
                .into_iter()
                .map(|s| InputStage::Decoded(Arc::new(s)))
                .collect(),
            tasks: vec![ExecutionTask { url: None }; n_tasks],
            depth: 0,
        }
    }

    /// Returns the name of this stage
    pub fn name(&self) -> String {
        format!("Stage {:<3}", self.num)
    }

    /// Returns an iterator over the input stages of this stage cast as &ExecutionStage
    /// which can be useful
    pub fn input_stages_iter(&self) -> impl Iterator<Item = &InputStage> {
        self.inputs.iter()
    }

    fn try_assign_urls(&self, urls: &[Url]) -> Result<Self> {
        let assigned_input_stages = self
            .input_stages_iter()
            .map(|input_stage| {
                let InputStage::Decoded(input_stage) = input_stage else {
                    return exec_err!("Cannot assign URLs to the tasks in an encoded stage");
                };
                StageExec::from_dyn(input_stage).try_assign_urls(urls)
            })
            .map_ok(|v| InputStage::Decoded(Arc::new(v)))
            .collect::<Result<Vec<_>>>()?;

        // pick a random starting position
        let mut rng = rand::thread_rng();
        let start_idx = rng.gen_range(0..urls.len());

        let assigned_tasks = self
            .tasks
            .iter()
            .enumerate()
            .map(|(i, _)| ExecutionTask {
                url: Some(urls[(start_idx + i) % urls.len()].clone()),
            })
            .collect::<Vec<_>>();

        let assigned_stage = StageExec {
            query_id: self.query_id,
            num: self.num,
            name: self.name.clone(),
            plan: self.plan.clone(),
            inputs: assigned_input_stages,
            tasks: assigned_tasks,
            depth: self.depth,
        };

        Ok(assigned_stage)
    }

    pub fn from_ctx(ctx: &Arc<TaskContext>) -> Result<Arc<StageExec>, DataFusionError> {
        ctx.session_config()
            .get_extension::<StageExec>()
            .ok_or(internal_datafusion_err!(
                "missing ExecutionStage in session config"
            ))
    }

    pub fn input_stage(&self, stage_num: usize) -> Result<&InputStage, DataFusionError> {
        for input_stage in self.input_stages_iter() {
            match input_stage {
                InputStage::Decoded(v) => {
                    if StageExec::from_dyn(v).num == stage_num {
                        return Ok(input_stage);
                    };
                }
                InputStage::Encoded { num, .. } => {
                    if *num == stage_num {
                        return Ok(input_stage);
                    }
                }
            }
        }
        internal_err!("no child stage with num {stage_num}")
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
        self.inputs
            .iter()
            .filter_map(|v| match v {
                InputStage::Decoded(v) => Some(v),
                InputStage::Encoded { .. } => None,
            })
            .collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan_err!("with_new_children() not supported for StageExec")
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.plan.properties()
    }

    /// Executes a query in a distributed manner. This method will lazily perform URL assignation
    /// to all the tasks, therefore, it must only be called once.
    ///
    /// [StageExec::execute] is only used for starting the distributed query in the same machine
    /// that planned it, but it's not used for task execution in `ArrowFlightEndpoint`, there,
    /// the inner `stage.plan` is executed directly.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream> {
        if partition > 0 {
            // The StageExec node calls try_assign_urls() lazily upon calling .execute(). This means
            // that .execute() must only be called once, as we cannot afford to perform several
            // random URL assignation while calling multiple partitions, as they will differ,
            // producing an invalid plan
            return exec_err!(
                "an executable StageExec must only have 1 partition, but it was called with partition index {partition}"
            );
        }

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
            .with_extension(assigned_stage.clone())
            .with_extension(Arc::new(DistributedTaskContext { task_index: 0 }));

        let new_ctx =
            SessionContext::new_with_config_rt(config, context.runtime_env().clone()).task_ctx();

        assigned_stage.plan.execute(partition, new_ctx)
    }
}

use bytes::Bytes;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_expr::Partitioning;
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

        if let Some(NetworkShuffleExec::Ready(ready)) =
            plan.as_any().downcast_ref::<NetworkShuffleExec>()
        {
            let Ok(input_stage) = &self.input_stage(ready.stage_num) else {
                writeln!(f, "Wrong partition number {}", ready.stage_num)?;
                return Ok(());
            };
            let n_tasks = self.tasks.len();
            let input_tasks = input_stage.tasks().len();
            let partitions = plan.output_partitioning().partition_count();
            let stage = ready.stage_num;
            write!(
                f,
                " read_from=Stage {stage}, output_partitions={partitions}, n_tasks={n_tasks}, input_tasks={input_tasks}",
            )?;
        }

        if let Some(NetworkCoalesceExec::Ready(ready)) =
            plan.as_any().downcast_ref::<NetworkCoalesceExec>()
        {
            let Ok(input_stage) = &self.input_stage(ready.stage_num) else {
                writeln!(f, "Wrong partition number {}", ready.stage_num)?;
                return Ok(());
            };
            let tasks = input_stage.tasks().len();
            let partitions = plan.output_partitioning().partition_count();
            let stage = ready.stage_num;
            write!(
                f,
                " read_from=Stage {stage}, output_partitions={partitions}, input_tasks={tasks}",
            )?;
        }

        if let Some(isolator) = plan.as_any().downcast_ref::<PartitionIsolatorExec>() {
            write!(
                f,
                " {}",
                format_tasks_for_partition_isolator(isolator, &self.tasks)
            )?;
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
                    "{}{} {} {}",
                    LTCORNER,
                    HORIZONTAL.repeat(5),
                    self.name,
                    format_tasks_for_stage(self.tasks.len(), &self.plan)
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
                format_tasks_for_stage(self.tasks.len(), &self.plan)
            ),
        }
    }
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

fn format_tasks_for_partition_isolator(
    isolator: &PartitionIsolatorExec,
    tasks: &[ExecutionTask],
) -> String {
    let input_partitions = isolator.input().output_partitioning().partition_count();
    let partition_groups = PartitionIsolatorExec::partition_groups(input_partitions, tasks.len());

    let n: usize = partition_groups.iter().map(|v| v.len()).sum();
    let mut partitions = vec![];
    for _ in 0..tasks.len() {
        partitions.push(vec!["__".to_string(); n]);
    }

    let mut result = "Tasks: ".to_string();
    for (i, partition_group) in partition_groups.iter().enumerate() {
        for (j, p) in partition_group.iter().enumerate() {
            partitions[i][*p] = format!("p{j}")
        }
        result += &format!("t{i}:[{}] ", partitions[i].join(","));
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

    if plan.as_any().downcast_ref::<StageExec>().is_some() {
        // draw all tasks first
        plan.apply(|node| {
            let stage = node
                .as_any()
                .downcast_ref::<StageExec>()
                .expect("Expected StageExec");

            for i in 0..stage.tasks.iter().len() {
                let p = display_single_task(stage, i)?;
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

            for input_stage in stage.input_stages_iter() {
                let InputStage::Decoded(input_stage) = input_stage else {
                    continue;
                };
                let input_stage = StageExec::from_dyn(input_stage);
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

            Ok(TreeNodeRecursion::Continue)
        })?;
    } else {
        // single plan, not a stage tree
        writeln!(f, "node[shape=none]")?;
        let p = display_plan(&plan, 0, 1, 0)?;
        writeln!(f, "{}", p)?;
    }

    writeln!(f, "}}")?;

    Ok(f)
}

pub fn display_single_task(stage: &StageExec, task_i: usize) -> Result<String> {
    let partition_group =
        build_partition_group(task_i, stage.plan.output_partitioning().partition_count());

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
        display_plan(&stage.plan, task_i, stage.tasks.len(), stage.num)?
    )?;
    writeln!(f, "  }}")?;
    writeln!(f, "  }}")?;

    Ok(f)
}

pub fn display_plan(
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

        for child in plan.children().iter() {
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
    plan: &dyn ExecutionPlan,
    stage_num: usize,
    task_i: usize,
    node_index: usize,
) -> Result<String> {
    let mut f = String::new();
    let output_partitions = plan.output_partitioning().partition_count();
    let input_partitions = if let Some(child) = plan.children().first() {
        child.output_partitioning().partition_count()
    } else if plan.as_any().is::<NetworkShuffleExec>() || plan.as_any().is::<NetworkCoalesceExec>()
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
    stage: &StageExec,
    task_i: usize,
    input_stage: &StageExec,
    input_task_i: usize,
) -> Result<String> {
    let mut f = String::new();
    let partition_group =
        build_partition_group(task_i, stage.plan.output_partitioning().partition_count());

    let mut found_isolator = false;
    let mut queue = VecDeque::from([&stage.plan]);
    let mut index = 0;
    while let Some(plan) = queue.pop_front() {
        index += 1;
        if plan.as_any().is::<PartitionIsolatorExec>() {
            found_isolator = true;
        } else if let Some(node) = plan.as_any().downcast_ref::<NetworkShuffleExec>() {
            let NetworkShuffleExec::Ready(node) = node else {
                continue;
            };
            if node.stage_num != input_stage.num {
                continue;
            }
            // draw the edges to this node pulling data up from its child
            let output_partitions = plan.output_partitioning().partition_count();
            for p in 0..output_partitions {
                let mut style = "";
                if found_isolator && !partition_group.contains(&p) {
                    style = "[style=invis]";
                }

                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{}:n -> {}_{}_{}_{}:b{}:s {} [color={}]",
                    input_stage.plan.name(),
                    input_stage.num,
                    input_task_i,
                    1, // the repartition exec is always the first node in the plan
                    p + (task_i * output_partitions),
                    plan.name(),
                    stage.num,
                    task_i,
                    index,
                    p,
                    style,
                    p % NUM_COLORS + 1
                )?;
            }
        } else if let Some(node) = plan.as_any().downcast_ref::<NetworkCoalesceExec>() {
            let NetworkCoalesceExec::Ready(node) = node else {
                continue;
            };
            if node.stage_num != input_stage.num {
                continue;
            }
            // draw the edges to this node pulling data up from its child
            let output_partitions = plan.output_partitioning().partition_count();
            let input_partitions_per_task = output_partitions / input_stage.tasks.len();
            for p in 0..input_partitions_per_task {
                let mut style = "";
                if found_isolator && !partition_group.contains(&p) {
                    style = "[style=invis]";
                }

                writeln!(
                    f,
                    "  {}_{}_{}_{}:t{}:n -> {}_{}_{}_{}:b{}:s {} [color={}]",
                    input_stage.plan.name(),
                    input_stage.num,
                    input_task_i,
                    1, // the repartition exec is always the first node in the plan
                    p,
                    plan.name(),
                    stage.num,
                    task_i,
                    index,
                    p + (input_task_i * input_partitions_per_task),
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

fn build_partition_group(task_i: usize, partitions: usize) -> Vec<usize> {
    ((task_i * partitions)..((task_i + 1) * partitions)).collect::<Vec<_>>()
}
