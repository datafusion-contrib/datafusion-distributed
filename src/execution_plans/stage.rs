use crate::channel_resolver_ext::get_distributed_channel_resolver;
use crate::{ArrowFlightReadExec, ChannelResolver, PartitionIsolatorExec};
use datafusion::common::{internal_datafusion_err, internal_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    displayable, DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
};
use datafusion::prelude::SessionContext;
use itertools::Itertools;
use rand::Rng;
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
    /// Creates a new `ExecutionStage` with the given plan and inputs.  One task will be created
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

    /// Returns an iterator over the child stages of this stage cast as &ExecutionStage
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

    pub fn from_ctx(ctx: &Arc<TaskContext>) -> Result<Arc<StageExec>, DataFusionError> {
        ctx.session_config()
            .get_extension::<StageExec>()
            .ok_or(internal_datafusion_err!(
                "ArrowFlightReadExec requires an ExecutionStage in the session config"
            ))
    }

    pub fn child_stage(&self, i: usize) -> Result<&StageExec, DataFusionError> {
        self.child_stages_iter()
            .find(|s| s.num == i)
            .ok_or(internal_datafusion_err!(
                "ArrowFlightReadExec: no child stage with num {i}"
            ))
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

pub fn display_stage_graphviz(stage: &StageExec) -> Result<String> {
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
    let mut stack: Vec<(&StageExec, Option<&StageExec>)> = vec![(stage, None)];

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
                format_partition_group(&task.partition_group),
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
                                format_partition_group(&task.partition_group),
                                partition,
                                our_parent.num,
                                format_partition_group(&ptask.partition_group),
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
                format_partition_group(&task.partition_group)
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
                format_partition_group(&stage.tasks[i].partition_group),
                stage.num,
                format_partition_group(&stage.tasks[i + 1].partition_group),
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

pub fn format_partition_group(partition_group: &[usize]) -> String {
    if partition_group.len() > 2 {
        format!(
            "{}..{}",
            partition_group[0],
            partition_group[partition_group.len() - 1]
        )
    } else {
        partition_group
            .iter()
            .map(|pg| format!("{pg}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}
