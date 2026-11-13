use crate::metrics::df_metrics_set_to_proto;
use crate::metrics::generated::metrics::{self as pb, PlanDump, StageDump};
use crate::metrics::metrics_proto::metric_proto_to_df;
use crate::stage::{format_each_metric_by_task, metrics_by_task_id, stage_partition_count};
use crate::{
    AvgLatencyMetric, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedExec, DistributedLeafExec,
    MaxLatencyMetric, NetworkBoundaryExt, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric,
    P99LatencyMetric, Stage,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use datafusion::common::{
    HashSet, Result, exec_datafusion_err, exec_err, internal_datafusion_err, internal_err,
};
use datafusion::physical_expr_common::metrics::MetricValue;
use datafusion::physical_plan::metrics::{Metric, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use prost::Message;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt::Write;
use std::io::Read;
use std::ptr;
use std::sync::{Arc, LazyLock};
use zstd::dict::{DecoderDictionary, EncoderDictionary};

/// zstd compression level for plan dumps.
const ZSTD_LEVEL: i32 = 6;

/// zstd dictionary that plan dumps are compressed with. Metric names, operator names and plan
/// fragments repeat across all dumps, and a dictionary of them makes dumps 15% to 40% smaller,
/// measured on benchmark dumps it was not trained on.
///
/// It is trained on the dumps of the TPC-H SF10, TPC-DS SF10 and ClickBench (partitions 0 to 3)
/// local benchmarks, run with `--workers 8 --debug`, which print one dump per query on stdout.
/// Each dump is decoded into the protobuf it contains, as a file of a samples directory:
///
/// ```shell
/// base64 -d <<< "$dump" | zstd -d > samples/<dataset>/<query>.pb
/// zstd --train -r samples --maxdict=65536 --dictID=32768 -o plan_dump.zdict
/// ```
///
/// Dumps record the id of the dictionary they are compressed with, so a dictionary trained again
/// must get a new `--dictID`, and dumps compressed with the previous one no longer decode.
const ZSTD_DICTIONARY: &[u8] = include_bytes!("plan_dump.zdict");

/// [ZSTD_DICTIONARY] prepared once for compressing, instead of on every dump.
static ZSTD_ENCODER_DICTIONARY: LazyLock<EncoderDictionary<'static>> =
    LazyLock::new(|| EncoderDictionary::copy(ZSTD_DICTIONARY, ZSTD_LEVEL));

/// [ZSTD_DICTIONARY] prepared once for decompressing, instead of on every dump.
static ZSTD_DECODER_DICTIONARY: LazyLock<DecoderDictionary<'static>> =
    LazyLock::new(|| DecoderDictionary::copy(ZSTD_DICTIONARY));

/// The identifier of the head stage that runs on the coordinator.
const HEAD_STAGE_ID: usize = 0;

/// The display of a [DistributedLeafExec] in a dump, which tells [operator_tree] that the metrics
/// of the node belong to its `t<i>: …` children.
const LEAF_DISPLAY: &str = "DistributedLeafExec:";

/// Distributed plans can be huge, the more tasks there are, the bigger the plans get, and
/// logging it can get prohibitive or impossible. This function produces a protobuf + zstd + base64
/// compressed representation of the plan with the metrics suitable to be used in logs that can
/// afterward be decoded and printed using the DFD CLI.
///
/// When embedding DFD into an existing service, it's recommended to log this plan dump in the
/// service's observability as tag in a log so that LLMs and humans can troubleshoot it afterward.
pub fn generate_plan_dump(plan: &Arc<dyn ExecutionPlan>) -> Result<String> {
    let (plan, metrics) = match plan.downcast_ref::<DistributedExec>() {
        Some(distributed) => (
            distributed.plan_for_viz_or_base_plan(),
            distributed.metrics(),
        ),
        None => (Arc::clone(plan), None),
    };
    // The head runs as a single task in the coordinator, and it does not have a stage number.
    let head = StageDump {
        stage_id: HEAD_STAGE_ID as u64,
        metrics: dump_metrics(metrics.as_ref())?,
        tasks: 1,
        partitions: plan.output_partitioning().partition_count() as u64,
        plan_dump: Some(Box::new(plan_dump(&plan)?)),
    };
    let compressed = zstd::bulk::Compressor::with_prepared_dictionary(&ZSTD_ENCODER_DICTIONARY)
        .and_then(|mut compressor| compressor.compress(&head.encode_to_vec()))
        .map_err(|err| internal_datafusion_err!("Failed to compress plan dump: {err}"))?;
    Ok(STANDARD.encode(compressed))
}

/// Options for displaying a plan dump.
#[derive(Debug, Clone, Default)]
pub struct DisplayPlanDumpOptions {
    /// The stage id to visualize. If none passed, the tree of stages is rendered.
    stage_id: Option<usize>,
    /// Whether to color the output with ANSI escape codes.
    color: bool,
}

impl DisplayPlanDumpOptions {
    /// Renders the nodes of this stage with their metrics instead of the tree of stages. The head
    /// of the plan, run by the coordinator, is stage 0.
    pub fn with_stage_id(mut self, stage_id: usize) -> Self {
        self.stage_id = Some(stage_id);
        self
    }

    /// Colors the output with ANSI escape codes, for displaying it in a terminal.
    pub fn with_color(mut self, color: bool) -> Self {
        self.color = color;
        self
    }
}

/// Displays a plan dump generated by [generate_plan_dump]. See [DisplayPlanDumpOptions] for
/// different types of rendering.
///
/// This is the function used by DFD's CLI for displaying plan dumps in the terminal.
pub fn display_plan_dump(plan: String, opts: DisplayPlanDumpOptions) -> Result<String> {
    let compressed = STANDARD
        .decode(plan.trim())
        .map_err(|err| exec_datafusion_err!("Plan dump is not valid base64: {err}"))?;
    let mut encoded = vec![];
    zstd::stream::Decoder::with_prepared_dictionary(
        compressed.as_slice(),
        &ZSTD_DECODER_DICTIONARY,
    )
    .and_then(|mut decoder| decoder.read_to_end(&mut encoded))
    .map_err(|err| exec_datafusion_err!("Plan dump is not zstd-compressed: {err}"))?;
    let head = StageDump::decode(encoded.as_slice())
        .map_err(|err| exec_datafusion_err!("Plan dump does not contain a plan: {err}"))?;

    let paint = Paint(opts.color);
    let single_node = input_stages(&head).is_empty();
    let tree = match (opts.stage_id, &head.plan_dump) {
        // A plan without stages is a single-node plan, which is displayed as its nodes.
        (None | Some(HEAD_STAGE_ID), Some(plan)) if single_node => operator_tree(plan, paint)?,
        (None, _) => stage_tree(&head, None, paint)?,
        (Some(id), _) => {
            let Some(stage) = find_stage(&head, id as u64) else {
                return exec_err!("Stage {id} not found in the plan dump");
            };
            let mut node = stage_node(stage, None, paint)?;
            if let Some(plan) = &stage.plan_dump {
                node.children.push(operator_tree(plan, paint)?);
            }
            node
        }
    };
    let mut out = String::new();
    tree.render("", "", paint, &mut out)
        .map_err(|err| internal_datafusion_err!("Failed to render plan dump: {err}"))?;
    Ok(out)
}

fn stage_dump(stage: &Stage) -> Result<StageDump> {
    let plan = stage.local_plan();
    Ok(StageDump {
        stage_id: stage.num() as u64,
        metrics: dump_metrics(Some(&stage.metrics()))?,
        tasks: stage.task_count() as u64,
        partitions: plan.map_or(0, |plan| stage_partition_count(stage.task_count(), plan)) as u64,
        plan_dump: plan.map(plan_dump).transpose()?.map(Box::new),
    })
}

fn plan_dump(plan: &Arc<dyn ExecutionPlan>) -> Result<PlanDump> {
    let metrics = plan.metrics();
    if let Some(leaf) = plan.downcast_ref::<DistributedLeafExec>() {
        return leaf_dump(leaf, metrics);
    }
    let mut dump = PlanDump {
        display: one_line(plan),
        metrics: dump_metrics(metrics.as_ref())?,
        children: vec![],
        input_stage: None,
    };
    match plan.as_network_boundary() {
        Some(boundary) => dump.input_stage = Some(Box::new(stage_dump(boundary.input_stage())?)),
        None => {
            dump.children = plan
                .children()
                .into_iter()
                .map(plan_dump)
                .collect::<Result<_>>()?
        }
    }
    Ok(dump)
}

/// A [DistributedLeafExec] runs a different variant of its leaf in each task, and each variant
/// becomes a `t<i>: …` child. The metrics of all the variants are dumped once on the leaf, where
/// each metric is a single column with the values of every task, and [leaf_tree] hands each
/// variant the metrics of its task when displaying them.
fn leaf_dump(leaf: &DistributedLeafExec, metrics: Option<MetricsSet>) -> Result<PlanDump> {
    let children = leaf
        .variants
        .iter()
        .enumerate()
        .map(|(task, variant)| PlanDump {
            display: format!("t{task}: {}", one_line(variant)),
            ..Default::default()
        })
        .collect();
    Ok(PlanDump {
        display: LEAF_DISPLAY.to_string(),
        metrics: dump_metrics(metrics.as_ref())?,
        children,
        input_stage: None,
    })
}

/// Metrics are dumped per task, which is how they are displayed, instead of per partition. Each
/// metric of a node is dumped once as a [pb::MetricColumn] holding its values for every task,
/// instead of as one [pb::Metric] per task repeating its kind, name and task label, which makes
/// dumps about a third smaller.
fn dump_metrics(metrics: Option<&MetricsSet>) -> Result<Vec<pb::MetricColumn>> {
    let Some(metrics) = metrics else {
        return Ok(vec![]);
    };
    let mut columns = vec![];
    for (_, tasks) in by_name_and_task(metrics)? {
        columns.extend(metric_column(&tasks)?);
    }
    Ok(columns)
}

/// The value of a metric for each task.
type TaskValues<'a> = BTreeMap<u64, Cow<'a, MetricValue>>;

/// The value of each metric for each task, aggregating the values of the task's partitions, with
/// metrics sorted by name so that the same metrics always produce the same dump.
fn by_name_and_task(metrics: &MetricsSet) -> Result<Vec<(&str, TaskValues<'_>)>> {
    let mut by_name: Vec<(&str, TaskValues)> = vec![];
    for metric in metrics.iter() {
        let value = metric.value();
        // Most tasks report a single value per metric, which is used as is.
        tasks_of(&mut by_name, value.name())
            .entry(metric_task(metric)?)
            .and_modify(|aggregated| aggregate(aggregated, value))
            .or_insert(Cow::Borrowed(value));
    }
    by_name.sort_unstable_by_key(|(name, _)| *name);
    Ok(by_name)
}

/// The values of the metric named `name`. A node has a few dozen metric names, reported once per
/// partition with the same `&str`, so comparing pointers first finds them faster than comparing
/// or hashing names.
fn tasks_of<'a, 'b>(
    by_name: &'b mut Vec<(&'a str, TaskValues<'a>)>,
    name: &'a str,
) -> &'b mut TaskValues<'a> {
    let i = by_name
        .iter()
        .position(|(n, _)| ptr::eq(*n, name) || *n == name);
    let i = i.unwrap_or_else(|| {
        by_name.push((name, BTreeMap::new()));
        by_name.len() - 1
    });
    &mut by_name[i].1
}

/// Aggregates `value` into `aggregated`, which becomes owned the first time.
fn aggregate(aggregated: &mut Cow<MetricValue>, value: &MetricValue) {
    if let Cow::Borrowed(first) = *aggregated {
        // Not `Cow::to_mut`, as a clone of a metric shares its values with the plan's.
        let mut owned = first.new_empty();
        owned.aggregate(first);
        *aggregated = Cow::Owned(owned);
    }
    aggregated.to_mut().aggregate(value);
}

/// The column of a metric's values for each task. All of them share the kind and name of the
/// first one, which is dumped as the column's metric, and the rest only contribute their numbers.
/// Metrics that a dump leaves out, like timestamps, have no column.
fn metric_column(tasks: &TaskValues) -> Result<Option<pb::MetricColumn>> {
    let Some(first) = tasks.values().next() else {
        return Ok(None);
    };
    let Some(value) = optimize_metric(first.as_ref().clone()) else {
        return Ok(None);
    };
    // Converted as a set, which leaves out the custom metrics it does not support.
    let mut set = MetricsSet::new();
    set.push(Arc::new(Metric::new(value, None)));
    let Some(mut metric) = df_metrics_set_to_proto(&set)?.metrics.pop() else {
        return Ok(None);
    };
    let mut values = Vec::with_capacity(tasks.len());
    for value in tasks.values() {
        push_numbers(&mut values, value);
    }
    // The column's metric is left without values, which are all in `values`.
    let first_values: Vec<u64> = (metric_values(&mut metric)?.into_iter())
        .map(std::mem::take)
        .collect();
    debug_assert_eq!(first_values, values[..first_values.len()]);
    Ok(Some(pb::MetricColumn {
        metric: Some(metric),
        tasks: tasks.keys().copied().collect(),
        values,
    }))
}

/// Pushes the numbers of a metric that are dumped in its column, in the order of [metric_values].
fn push_numbers(values: &mut Vec<u64>, value: &MetricValue) {
    match value {
        MetricValue::PruningMetrics {
            pruning_metrics, ..
        } => values.extend([pruning_metrics.pruned(), pruning_metrics.matched()].map(|n| n as u64)),
        MetricValue::Ratio { ratio_metrics, .. } => {
            values.extend([ratio_metrics.part(), ratio_metrics.total()].map(|n| n as u64))
        }
        MetricValue::Custom { value, .. } => {
            match value.as_any().downcast_ref::<AvgLatencyMetric>() {
                Some(avg) => values.extend([avg.nanos_sum(), avg.count()].map(|n| n as u64)),
                None => values.push(value.as_usize() as u64),
            }
        }
        value => values.push(value.as_usize() as u64),
    }
}

/// The task that reported `metric`. Metrics without a task label, like the ones of a plan that is
/// not distributed, are taken as reported by task 0.
fn metric_task(metric: &Metric) -> Result<u64> {
    let Some(label) =
        (metric.labels().iter()).find(|l| l.name() == DISTRIBUTED_DATAFUSION_TASK_ID_LABEL)
    else {
        return Ok(0);
    };
    let task = label.value();
    task.parse()
        .map_err(|_| internal_datafusion_err!("Invalid task id label '{task}'"))
}

/// Reverses [dump_metrics], giving each metric of a column its task label back.
fn undump_metrics(columns: &[pb::MetricColumn]) -> Result<MetricsSet> {
    let mut metrics = MetricsSet::new();
    for column in columns {
        let Some(template) = &column.metric else {
            continue;
        };
        let width = metric_values(&mut template.clone())?.len();
        if column.values.len() != column.tasks.len() * width {
            return exec_err!("Plan dump has a metric column with a wrong number of values");
        }
        for (task, values) in column.tasks.iter().zip(column.values.chunks_exact(width)) {
            let mut metric = template.clone();
            for (slot, value) in metric_values(&mut metric)?.into_iter().zip(values) {
                *slot = *value;
            }
            metric.labels.push(pb::Label {
                name: DISTRIBUTED_DATAFUSION_TASK_ID_LABEL.to_string(),
                value: task.to_string(),
            });
            metrics.push(metric_proto_to_df(metric)?);
        }
    }
    Ok(metrics)
}

/// The numbers of a metric, which are dumped as part of a [pb::MetricColumn], while the rest of
/// the metric, its kind and name, is dumped once per column.
fn metric_values(metric: &mut pb::Metric) -> Result<Vec<&mut u64>> {
    use pb::metric::Value;
    Ok(match &mut metric.value {
        Some(Value::OutputRows(v)) => vec![&mut v.value],
        Some(Value::ElapsedCompute(v)) => vec![&mut v.value],
        Some(Value::SpillCount(v)) => vec![&mut v.value],
        Some(Value::SpilledBytes(v)) => vec![&mut v.value],
        Some(Value::SpilledRows(v)) => vec![&mut v.value],
        Some(Value::CurrentMemoryUsage(v)) => vec![&mut v.value],
        Some(Value::Count(v)) => vec![&mut v.value],
        Some(Value::Gauge(v)) => vec![&mut v.value],
        Some(Value::Time(v)) => vec![&mut v.value],
        Some(Value::OutputBytes(v)) => vec![&mut v.value],
        Some(Value::OutputBatches(v)) => vec![&mut v.value],
        Some(Value::PruningMetrics(v)) => vec![&mut v.pruned, &mut v.matched],
        Some(Value::Ratio(v)) => vec![&mut v.part, &mut v.total],
        Some(Value::CustomMinLatency(v)) => vec![&mut v.value],
        Some(Value::CustomMaxLatency(v)) => vec![&mut v.value],
        Some(Value::CustomAvgLatency(v)) => vec![&mut v.nanos_sum, &mut v.count],
        Some(Value::CustomFirstLatency(v)) => vec![&mut v.value],
        Some(Value::CustomBytesCount(v)) => vec![&mut v.value],
        Some(Value::CustomMaxGauge(v)) => vec![&mut v.value],
        Some(Value::PeakMemoryUsage(v)) => vec![&mut v.value],
        // Timestamps are dropped and percentiles resolved by [optimize_metric].
        unsupported @ (Some(
            Value::StartTimestamp(_)
            | Value::EndTimestamp(_)
            | Value::CustomP50Latency(_)
            | Value::CustomP75Latency(_)
            | Value::CustomP95Latency(_)
            | Value::CustomP99Latency(_),
        )
        | None) => {
            return internal_err!("Metric {unsupported:?} cannot be part of a plan dump");
        }
    })
}

/// Optimizes metrics so that they are shipped as compressed as possible over the wire.
pub(super) fn optimize_metric(value: MetricValue) -> Option<MetricValue> {
    [resolve_percentiles, drop_timestamps]
        .iter()
        .try_fold(value, |value, optimizer| optimizer(value))
}

/// Percentile metrics carry a whole sketch of the latencies they observed, which are a quarter of
/// a dump. As the metrics in a dump are already aggregated per task, only the percentile of each
/// sketch is displayed, so it is kept as a [MaxLatencyMetric] instead, which displays the same.
fn resolve_percentiles(value: MetricValue) -> Option<MetricValue> {
    let MetricValue::Custom { name, value } = value else {
        return Some(value);
    };
    let any = value.as_any();
    let is_percentile = any.is::<P50LatencyMetric>()
        || any.is::<P75LatencyMetric>()
        || any.is::<P95LatencyMetric>()
        || any.is::<P99LatencyMetric>();
    let value = match is_percentile {
        true => Arc::new(MaxLatencyMetric::from_nanos(value.as_usize())),
        false => value,
    };
    Some(MetricValue::Custom { name, value })
}

/// Start and end timestamps are not displayed.
fn drop_timestamps(value: MetricValue) -> Option<MetricValue> {
    (!value.is_timestamp()).then_some(value)
}

fn one_line(plan: &Arc<dyn ExecutionPlan>) -> String {
    let display = displayable(plan.as_ref()).one_line().to_string();
    elide_file_groups(display.trim_end())
}

/// A node of a rendered plan dump: a label followed by one line per metric, and its children.
struct Node {
    label: String,
    metrics: Vec<String>,
    children: Vec<Node>,
}

impl Node {
    fn render(
        &self,
        prefix: &str,
        child_prefix: &str,
        paint: Paint,
        out: &mut String,
    ) -> std::fmt::Result {
        writeln!(out, "{}{}", paint.dim(prefix), self.label)?;
        let guide = match self.children.is_empty() {
            true => "  ",
            false => "│ ",
        };
        let metric_prefix = paint.dim(&format!("{child_prefix}{guide}"));
        for metric in &self.metrics {
            writeln!(out, "{metric_prefix}{metric}")?;
        }
        for (i, child) in self.children.iter().enumerate() {
            let (branch, continuation) = match i == self.children.len() - 1 {
                true => ("└─ ", "   "),
                false => ("├─ ", "│  "),
            };
            child.render(
                &format!("{child_prefix}{branch}"),
                &format!("{child_prefix}{continuation}"),
                paint,
                out,
            )?;
        }
        Ok(())
    }
}

/// A stage and its stage-level metrics. `boundary` is the kind of network boundary reading it.
fn stage_node(stage: &StageDump, boundary: Option<&str>, paint: Paint) -> Result<Node> {
    let label = match (stage.stage_id, boundary) {
        (0, None) => paint.stage("DistributedExec"),
        (id, boundary) => {
            let boundary = boundary
                .map(|b| format!(" {}", paint.boundary(&format!("({b})"))))
                .unwrap_or_default();
            let tasks = paint.metric(&format!("tasks={}", stage.tasks));
            let partitions = paint.metric(&format!("partitions={}", stage.partitions));
            format!(
                "{}{boundary} {tasks} {partitions}",
                paint.stage(&format!("Stage {id}"))
            )
        }
    };
    Ok(Node {
        label,
        metrics: metric_lines(&undump_metrics(&stage.metrics)?, paint),
        children: vec![],
    })
}

/// Every stage as a tree, each stage under the stage reading from it.
fn stage_tree(stage: &StageDump, boundary: Option<&str>, paint: Paint) -> Result<Node> {
    let mut node = stage_node(stage, boundary, paint)?;
    node.children = input_stages(stage)
        .into_iter()
        .map(|(boundary, input)| stage_tree(input, Some(boundary), paint))
        .collect::<Result<_>>()?;
    Ok(node)
}

/// The nodes of a stage as a tree. Network boundaries are leaves: the stages they read from are
/// rendered on their own.
fn operator_tree(plan: &PlanDump, paint: Paint) -> Result<Node> {
    if plan.display == LEAF_DISPLAY {
        return leaf_tree(plan, paint);
    }
    Ok(Node {
        label: paint.operator(&plan.display),
        metrics: metric_lines(&undump_metrics(&plan.metrics)?, paint),
        children: plan
            .children
            .iter()
            .map(|child| operator_tree(child, paint))
            .collect::<Result<_>>()?,
    })
}

/// Like [crate::display_plan_ascii], each `t<i>: …` variant of a [DistributedLeafExec] shows the
/// metrics of the task that ran it.
fn leaf_tree(plan: &PlanDump, paint: Paint) -> Result<Node> {
    let by_task = metrics_by_task_id(&undump_metrics(&plan.metrics)?);
    let variant = |task: usize, variant: &PlanDump| Node {
        label: paint.operator(&variant.display),
        metrics: by_task
            .get(&task)
            .map(|metrics| metric_lines(metrics, paint))
            .unwrap_or_default(),
        children: vec![],
    };
    Ok(Node {
        label: paint.operator(&plan.display),
        metrics: vec![],
        children: plan
            .children
            .iter()
            .enumerate()
            .map(|(task, child)| variant(task, child))
            .collect(),
    })
}

fn find_stage(stage: &StageDump, id: u64) -> Option<&StageDump> {
    if stage.stage_id == id {
        return Some(stage);
    }
    input_stages(stage)
        .into_iter()
        .find_map(|(_, input)| find_stage(input, id))
}

/// The stages `stage` reads from, in plan order, along with the kind of network boundary
/// reading each of them.
fn input_stages(stage: &StageDump) -> Vec<(&str, &StageDump)> {
    fn collect<'a>(plan: &'a PlanDump, inputs: &mut Vec<(&'a str, &'a StageDump)>) {
        if let Some(input) = &plan.input_stage {
            inputs.push((boundary_kind(&plan.display), input));
        }
        for child in &plan.children {
            collect(child, inputs);
        }
    }
    let mut inputs = vec![];
    if let Some(plan) = &stage.plan_dump {
        collect(plan, &mut inputs);
    }
    inputs
}

/// `Shuffle` for a `[Stage 2] => NetworkShuffleExec: …` node.
fn boundary_kind(display: &str) -> &str {
    let name = display.split_once("=> ").map_or(display, |(_, rest)| rest);
    let name = name.split(':').next().unwrap_or(name);
    name.trim_start_matches("Network").trim_end_matches("Exec")
}

/// One `name=value` line per metric, with per-task values as `name={task:value, …}`. A metric only
/// reported by task 0, like the ones of a plan that is not distributed, displays as `name=value`.
fn metric_lines(metrics: &MetricsSet, paint: Paint) -> Vec<String> {
    let reported_by_other_tasks: HashSet<&str> = (metrics.iter())
        .filter(|metric| metric_task(metric).is_ok_and(|task| task != 0))
        .map(|metric| metric.value().name())
        .collect();
    let mut displayed = MetricsSet::new();
    for metric in metrics.iter() {
        let only_task_0 = !reported_by_other_tasks.contains(metric.value().name());
        displayed.push(match only_task_0 {
            true => without_task_label(metric),
            false => Arc::clone(metric),
        });
    }
    format_each_metric_by_task(&displayed)
        .iter()
        .map(|metric| paint.metric(metric))
        .collect()
}

fn without_task_label(metric: &Metric) -> Arc<Metric> {
    let labels = (metric.labels().iter())
        .filter(|label| label.name() != DISTRIBUTED_DATAFUSION_TASK_ID_LABEL)
        .cloned()
        .collect();
    Arc::new(Metric::new_with_labels(
        metric.value().clone(),
        None,
        labels,
    ))
}

/// `file_groups={16 groups: [[…]]}` becomes `file_groups={16 groups}`, as file lists are often
/// most of a data source's description.
fn elide_file_groups(display: &str) -> String {
    const FILE_GROUPS: &str = "file_groups={";
    let Some(start) = display.find(FILE_GROUPS).map(|i| i + FILE_GROUPS.len()) else {
        return display.to_string();
    };
    let Some(len) = display[start..].find('}') else {
        return display.to_string();
    };
    let groups = display[start..start + len]
        .split(':')
        .next()
        .unwrap_or_default();
    format!("{}{groups}{}", &display[..start], &display[start + len..])
}

/// Where the parameter name that `before` ends with starts, like `mode` in `…, mode`, if it is a
/// whole lowercase word.
fn param_name_start(before: &str) -> Option<usize> {
    let start = (before.char_indices().rev())
        .take_while(|(_, c)| c.is_ascii_lowercase() || *c == '_')
        .last()?
        .0;
    let at_word_start =
        (before[..start].chars().next_back()).is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
    at_word_start.then_some(start)
}

/// Colors the parts of a rendered plan dump with ANSI escape codes when enabled, similar to how
/// code is highlighted. When disabled, text is returned as it is.
#[derive(Debug, Clone, Copy)]
struct Paint(bool);

impl Paint {
    fn style(self, code: &str, text: &str) -> String {
        match self.0 && !text.is_empty() {
            true => format!("\x1b[{code}m{text}\x1b[0m"),
            false => text.to_string(),
        }
    }

    /// Tree guides, task indexes and separators.
    fn dim(self, text: &str) -> String {
        self.style("90", text)
    }

    fn stage(self, text: &str) -> String {
        self.style("1;36", text)
    }

    fn boundary(self, text: &str) -> String {
        self.style("35", text)
    }

    fn value(self, text: &str) -> String {
        self.style("33", text)
    }

    /// A `name=value` metric, with per-task values as `name={task:value, …}`.
    fn metric(self, metric: &str) -> String {
        let Some((name, value)) = metric.split_once('=') else {
            return metric.to_string();
        };
        let value = match value.strip_prefix('{').and_then(|v| v.strip_suffix('}')) {
            Some(tasks) => self.tasks(tasks),
            None => self.value(value),
        };
        format!("{name}{}{value}", self.dim("="))
    }

    /// The `task:value, …` values of a metric for each task, within braces.
    fn tasks(self, tasks: &str) -> String {
        let tasks = (tasks.split(", "))
            .map(|task| match task.split_once(':') {
                Some((index, value)) => {
                    format!("{}{}", self.dim(&format!("{index}:")), self.value(value))
                }
                None => self.value(task),
            })
            .collect::<Vec<_>>()
            .join(&self.dim(", "));
        format!("{}{tasks}{}", self.dim("{"), self.dim("}"))
    }

    /// An operator, like `[Stage 1] => NetworkShuffleExec: input_tasks=3`, with the stage it reads
    /// from, its name and its parameter names highlighted.
    fn operator(self, label: &str) -> String {
        let mut out = String::new();
        let mut rest = label;
        // The `t<i>: ` prefix of the variants of a DistributedLeafExec.
        if let Some((task, after)) = rest.split_once(": ")
            && task.len() > 1
            && task.starts_with('t')
            && task[1..].chars().all(|c| c.is_ascii_digit())
        {
            out += &self.dim(&format!("{task}: "));
            rest = after;
        }
        // The `[Stage N] => ` prefix of network boundaries.
        if rest.starts_with("[Stage ")
            && let Some((input, after)) = rest.split_once(" => ")
        {
            out += &self.stage(input);
            out += &self.dim(" => ");
            rest = after;
        }
        let name_len = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_'))
            .unwrap_or(rest.len());
        out += &self.style("1;32", &rest[..name_len]);
        out += &self.params(&rest[name_len..]);
        out
    }

    /// Highlights parameter names, like `mode` in `mode=Partitioned`.
    fn params(self, text: &str) -> String {
        let mut out = String::new();
        let mut rest = text;
        while let Some(eq) = rest.find('=') {
            let before = &rest[..eq];
            match param_name_start(before) {
                Some(start) => {
                    out += &before[..start];
                    out += &self.style("34", &before[start..]);
                    out += &self.dim("=");
                }
                None => {
                    out += before;
                    out += "=";
                }
            }
            rest = &rest[eq + 1..];
        }
        out + rest
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LatencyMetricExt;
    use crate::assert_snapshot;
    use crate::test_utils::plans::TestPlanBuilder;
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
    use std::time::Duration;

    #[tokio::test]
    async fn single_node_plan() -> Result<()> {
        let query = r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#;
        let plan = TestPlanBuilder::default()
            .distributed_planner(false)
            .physical_plan(query)
            .await;

        let dump = generate_plan_dump(&plan)?;

        let display = display_plan_dump(dump, DisplayPlanDumpOptions::default())?;
        assert_snapshot!(display, @r"
        ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
        └─ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
           └─ RepartitionExec: partitioning=Hash([RainToday@0], 4), input_partitions=3
              └─ AggregateExec: mode=Partial, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
                 └─ DataSourceExec: file_groups={3 groups}, projection=[RainToday], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn distributed_plan_stages() -> Result<()> {
        let query = r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#;
        let plan = TestPlanBuilder::default().physical_plan(query).await;

        let dump = generate_plan_dump(&plan)?;

        let display = display_plan_dump(dump, DisplayPlanDumpOptions::default())?;
        assert_snapshot!(display, @r"
        DistributedExec
        └─ Stage 2 (Coalesce) tasks=3 partitions=4
           └─ Stage 1 (Shuffle) tasks=3 partitions=12
        ");
        Ok(())
    }

    #[tokio::test]
    async fn distributed_plan_stage() -> Result<()> {
        let query = r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#;
        let plan = TestPlanBuilder::default().physical_plan(query).await;

        let dump = generate_plan_dump(&plan)?;

        let opts = DisplayPlanDumpOptions::default().with_stage_id(2);
        let display = display_plan_dump(dump, opts)?;
        assert_snapshot!(display, @r"
        Stage 2 tasks=3 partitions=4
        └─ ProjectionExec: expr=[RainToday@0 as RainToday, count(Int64(1))@1 as count(*)]
           └─ AggregateExec: mode=FinalPartitioned, gby=[RainToday@0 as RainToday], aggr=[count(Int64(1))]
              └─ [Stage 1] => NetworkShuffleExec: output_partitions=4, input_tasks=3
        ");
        Ok(())
    }

    #[tokio::test]
    async fn distributed_leaf() -> Result<()> {
        let query = r#"SELECT "MinTemp", "RainToday" FROM weather WHERE "Rainfall" > 0"#;
        let plan = TestPlanBuilder::default().physical_plan(query).await;

        let dump = generate_plan_dump(&plan)?;

        let opts = DisplayPlanDumpOptions::default().with_stage_id(1);
        let display = display_plan_dump(dump, opts)?;
        assert_snapshot!(display, @r"
        Stage 1 tasks=3 partitions=12
        └─ FilterExec: Rainfall@1 > 0, projection=[MinTemp@0, RainToday@2]
           └─ RepartitionExec: partitioning=RoundRobinBatch(4), input_partitions=3
              └─ DistributedLeafExec:
                 ├─ t0: DataSourceExec: file_groups={3 groups}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=Rainfall@2 > 0, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 0, required_guarantees=[]
                 ├─ t1: DataSourceExec: file_groups={3 groups}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=Rainfall@2 > 0, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 0, required_guarantees=[]
                 └─ t2: DataSourceExec: file_groups={3 groups}, projection=[MinTemp, Rainfall, RainToday], file_type=parquet, predicate=Rainfall@2 > 0, pruning_predicate=Rainfall_null_count@1 != row_count@2 AND Rainfall_max@0 > 0, required_guarantees=[]
        ");
        Ok(())
    }

    #[test]
    fn metrics_once_dumped() -> Result<()> {
        let metrics = ExecutionPlanMetricsSet::new();
        let task = |task: usize| {
            MetricBuilder::new(&metrics)
                .with_new_label(DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, task.to_string())
        };
        // Task 0 runs two partitions, whose values add up. Metrics only reported by task 0 display
        // as a plain value.
        task(0).output_rows(0).add(10);
        task(0).output_rows(1).add(5);
        task(1).output_rows(0).add(7);
        task(0)
            .elapsed_compute(0)
            .add_duration(Duration::from_millis(3));
        task(1)
            .elapsed_compute(0)
            .add_duration(Duration::from_micros(1500));
        // Metrics with more than one value.
        let pruning = task(1).pruning_metrics("files_ranges_pruned_statistics", 0);
        pruning.add_pruned(3);
        pruning.add_matched(2);
        let ratio = task(0).ratio_metrics("probe_hit_rate", 0);
        ratio.add_part(1);
        ratio.add_total(4);
        task(0)
            .avg_latency("network_latency_avg")
            .add_duration(Duration::from_millis(2));
        // Percentiles are dumped as the value they display.
        let p50 = task(1).p50_latency("network_latency_p50");
        p50.add_duration(Duration::from_millis(1));
        p50.add_duration(Duration::from_millis(5));
        p50.add_duration(Duration::from_millis(9));
        task(2)
            .p50_latency("network_latency_p50")
            .add_duration(Duration::from_millis(7));
        // The coordinator's metrics do not belong to a task, and display like task 0's.
        MetricBuilder::new(&metrics)
            .global_counter("plan_bytes_sent")
            .add(1024);
        let metrics = metrics.clone_inner();

        let dumped = undump_metrics(&dump_metrics(Some(&metrics))?)?;

        assert_snapshot!(metric_lines(&dumped, Paint(false)).join("\n"), @r"
        output_rows={0:15, 1:7}
        elapsed_compute={0:3.00ms, 1:1.50ms}
        files_ranges_pruned_statistics={1:5 total → 2 matched}
        plan_bytes_sent=1.02 K
        probe_hit_rate=25% (1/4)
        network_latency_avg=2.00ms
        network_latency_p50={1:5.03ms, 2:7.06ms}
        ");
        Ok(())
    }

    #[test]
    fn invalid_plan_dump() {
        let err = display_plan_dump("not a plan dump".to_string(), Default::default());
        assert_snapshot!(err.unwrap_err(), @"Execution error: Plan dump is not valid base64: Invalid symbol 32, offset 3.");
    }

    #[tokio::test]
    async fn colors_do_not_change_the_text() -> Result<()> {
        let query = r#"SELECT "RainToday", count(*) FROM weather GROUP BY "RainToday""#;
        let plan = TestPlanBuilder::default().physical_plan(query).await;
        let dump = generate_plan_dump(&plan)?;

        let plain = DisplayPlanDumpOptions::default().with_stage_id(2);
        let colored = display_plan_dump(dump.clone(), plain.clone().with_color(true))?;
        let plain = display_plan_dump(dump, plain)?;

        assert_ne!(colored, plain);
        assert_eq!(remove_ansi_escape_codes(&colored), plain);
        Ok(())
    }

    /// Removes the `\x1b[…m` sequences used for coloring.
    fn remove_ansi_escape_codes(text: &str) -> String {
        let mut out = String::new();
        let mut rest = text;
        while let Some(start) = rest.find('\x1b') {
            out += &rest[..start];
            let end = rest[start..]
                .find('m')
                .map_or(rest.len(), |i| start + i + 1);
            rest = &rest[end..];
        }
        out + rest
    }
}
