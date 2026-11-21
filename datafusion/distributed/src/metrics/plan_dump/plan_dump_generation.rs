use crate::metrics::df_metrics_set_to_proto;
use crate::metrics::generated::metrics::metric::Value;
use crate::metrics::generated::metrics::{self as pb, PlanDump, StageDump};
use crate::stage::stage_partition_count;
use crate::{
    AvgLatencyMetric, DISTRIBUTED_DATAFUSION_TASK_ID_LABEL, DistributedExec, DistributedLeafExec,
    MaxLatencyMetric, NetworkBoundaryExt, P50LatencyMetric, P75LatencyMetric, P95LatencyMetric,
    P99LatencyMetric, Stage,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use datafusion::common::{Result, config_err, internal_datafusion_err, internal_err};
use datafusion::physical_expr_common::metrics::MetricValue;
use datafusion::physical_plan::metrics::{Metric, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, displayable};
use prost::Message;
use sketches_ddsketch::Config;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ptr;
use std::sync::{Arc, LazyLock};
use zstd::dict::EncoderDictionary;

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
pub(super) const ZSTD_DICTIONARY: &[u8] = include_bytes!("plan_dump.zdict");

/// [ZSTD_DICTIONARY] prepared once for compressing, instead of on every dump.
static ZSTD_ENCODER_DICTIONARY: LazyLock<EncoderDictionary<'static>> =
    LazyLock::new(|| EncoderDictionary::copy(ZSTD_DICTIONARY, ZSTD_LEVEL));

/// The identifier of the head stage that runs on the coordinator.
pub(super) const HEAD_STAGE_ID: usize = 0;

/// The display of a [DistributedLeafExec] in a dump, which tells the display of dumps that the
/// metrics of the node belong to its `t<i>: …` children.
pub(super) const LEAF_DISPLAY: &str = "DistributedLeafExec:";

/// Options for generating a plan dump, which trade how precise and complete its metrics are for
/// how small it is. Most of a compressed dump is metric values, as plan displays and metric names
/// repeat and compress well, while values do not.
#[derive(Debug, Clone)]
pub struct GeneratePlanDumpOptions {
    /// Relative accuracy kept of metrics measured in bytes, or 0 for exact.
    bytes_accuracy: f64,
    /// Relative accuracy kept of metrics measured in time, or 0 for exact.
    time_accuracy: f64,
    /// Relative accuracy kept of the rest of the metrics, which count things like rows, or 0 for
    /// exact.
    count_accuracy: f64,
    /// The names of the metrics that are left out of dumps.
    excluded_metrics: Vec<Cow<'static, str>>,
}

impl Default for GeneratePlanDumpOptions {
    fn default() -> Self {
        Self {
            bytes_accuracy: 0.005,
            time_accuracy: 0.05,
            // Exact, as row counts tell whether a plan returns the right results.
            count_accuracy: 0.0,
            // Network latencies are reported as several statistics of which the percentiles and
            // the maximum are enough for spotting slow networks.
            excluded_metrics: [
                "network_latency_min",
                "network_latency_first",
                "network_latency_sum",
                "network_latency_count",
            ]
            .map(Cow::from)
            .to_vec(),
        }
    }
}

impl GeneratePlanDumpOptions {
    /// Dumps metrics measured in bytes, like `output_bytes`, within `accuracy` of their value:
    /// with 0.01, 1000 bytes are read back as anything from 990 to 1010. 0 keeps them exact.
    /// Defaults to 0.005.
    pub fn with_bytes_relative_accuracy(mut self, accuracy: f64) -> Self {
        self.bytes_accuracy = accuracy;
        self
    }

    /// Dumps metrics measured in time, like `elapsed_compute`, within `accuracy` of their value:
    /// with 0.05, 100ms are read back as anything from 95ms to 105ms. 0 keeps them exact.
    /// Defaults to 0.05.
    pub fn with_time_relative_accuracy(mut self, accuracy: f64) -> Self {
        self.time_accuracy = accuracy;
        self
    }

    /// Dumps the metrics that are neither bytes nor time, which count things like `output_rows`,
    /// within `accuracy` of their value. Defaults to 0, which keeps them exact.
    pub fn with_count_relative_accuracy(mut self, accuracy: f64) -> Self {
        self.count_accuracy = accuracy;
        self
    }

    /// Leaves the metrics with these names out of dumps, replacing the ones left out by default,
    /// which are the network latency statistics other than `network_latency_p50`,
    /// `network_latency_p95` and `network_latency_max`.
    pub fn with_excluded_metrics(
        mut self,
        names: impl IntoIterator<Item = impl Into<Cow<'static, str>>>,
    ) -> Self {
        self.excluded_metrics = names.into_iter().map(Into::into).collect();
        self
    }

    /// The relative accuracy kept of the values of `metric`, depending on what it measures.
    fn relative_accuracy(&self, metric: &pb::Metric) -> f64 {
        match metric.value {
            Some(
                Value::SpilledBytes(_)
                | Value::OutputBytes(_)
                | Value::CurrentMemoryUsage(_)
                | Value::PeakMemoryUsage(_)
                | Value::CustomBytesCount(_),
            ) => self.bytes_accuracy,
            Some(
                Value::ElapsedCompute(_)
                | Value::Time(_)
                | Value::CustomMinLatency(_)
                | Value::CustomMaxLatency(_)
                | Value::CustomAvgLatency(_)
                | Value::CustomP50Latency(_)
                | Value::CustomP75Latency(_)
                | Value::CustomP95Latency(_)
                | Value::CustomP99Latency(_)
                | Value::CustomFirstLatency(_),
            ) => self.time_accuracy,
            _ => self.count_accuracy,
        }
    }
}

/// Distributed plans can be huge, the more tasks there are, the bigger the plans get, and
/// logging it can get prohibitive or impossible. This function produces a protobuf + zstd + base64
/// compressed representation of the plan with the metrics suitable to be used in logs that can
/// afterward be decoded and printed using the DFD CLI.
///
/// When embedding DFD into an existing service, it's recommended to log this plan dump in the
/// service's observability as tag in a log so that LLMs and humans can troubleshoot it afterward.
pub fn generate_plan_dump(
    plan: &Arc<dyn ExecutionPlan>,
    opts: GeneratePlanDumpOptions,
) -> Result<String> {
    for accuracy in [opts.bytes_accuracy, opts.time_accuracy, opts.count_accuracy] {
        if !(0.0..1.0).contains(&accuracy) {
            return config_err!("Plan dump relative accuracy must be in [0, 1), got {accuracy}");
        }
    }
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
        metrics: dump_metrics(metrics.as_ref(), &opts)?,
        tasks: 1,
        partitions: plan.output_partitioning().partition_count() as u64,
        plan_dump: Some(Box::new(plan_dump(&plan, &opts)?)),
    };
    encode_plan_dump(&head)
}

fn encode_plan_dump(head: &StageDump) -> Result<String> {
    let compressed = zstd::bulk::Compressor::with_prepared_dictionary(&ZSTD_ENCODER_DICTIONARY)
        .and_then(|mut compressor| compressor.compress(&head.encode_to_vec()))
        .map_err(|err| internal_datafusion_err!("Failed to compress plan dump: {err}"))?;
    Ok(STANDARD.encode(compressed))
}

fn stage_dump(stage: &Stage, opts: &GeneratePlanDumpOptions) -> Result<StageDump> {
    let plan = stage.local_plan();
    Ok(StageDump {
        stage_id: stage.num() as u64,
        metrics: dump_metrics(Some(&stage.metrics()), opts)?,
        tasks: stage.task_count() as u64,
        partitions: plan.map_or(0, |plan| stage_partition_count(stage.task_count(), plan)) as u64,
        plan_dump: (plan.map(|plan| plan_dump(plan, opts)).transpose()?).map(Box::new),
    })
}

fn plan_dump(plan: &Arc<dyn ExecutionPlan>, opts: &GeneratePlanDumpOptions) -> Result<PlanDump> {
    let metrics = plan.metrics();
    if let Some(leaf) = plan.downcast_ref::<DistributedLeafExec>() {
        return leaf_dump(leaf, metrics, opts);
    }
    let mut dump = PlanDump {
        display: one_line(plan),
        metrics: dump_metrics(metrics.as_ref(), opts)?,
        children: vec![],
        input_stage: None,
    };
    match plan.as_network_boundary() {
        Some(boundary) => {
            dump.input_stage = Some(Box::new(stage_dump(boundary.input_stage(), opts)?))
        }
        None => {
            dump.children = plan
                .children()
                .into_iter()
                .map(|child| plan_dump(child, opts))
                .collect::<Result<_>>()?
        }
    }
    Ok(dump)
}

/// A [DistributedLeafExec] runs a different variant of its leaf in each task, and each variant
/// becomes a `t<i>: …` child. The metrics of all the variants are dumped once on the leaf, where
/// each metric is a single column with the values of every task, and the display of dumps hands
/// each variant the metrics of its task.
fn leaf_dump(
    leaf: &DistributedLeafExec,
    metrics: Option<MetricsSet>,
    opts: &GeneratePlanDumpOptions,
) -> Result<PlanDump> {
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
        metrics: dump_metrics(metrics.as_ref(), opts)?,
        children,
        input_stage: None,
    })
}

/// Metrics are dumped per task, which is how they are displayed, instead of per partition. Each
/// metric of a node is dumped once as a [pb::MetricColumn] holding its values for every task,
/// instead of as one [pb::Metric] per task repeating its kind, name and task label, which makes
/// dumps about a third smaller.
fn dump_metrics(
    metrics: Option<&MetricsSet>,
    opts: &GeneratePlanDumpOptions,
) -> Result<Vec<pb::MetricColumn>> {
    let Some(metrics) = metrics else {
        return Ok(vec![]);
    };
    let mut columns = vec![];
    for (name, tasks) in by_name_and_task(metrics)? {
        if !opts
            .excluded_metrics
            .iter()
            .any(|excluded| excluded == name)
        {
            columns.extend(metric_column(&tasks, opts)?);
        }
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
fn metric_column(
    tasks: &TaskValues,
    opts: &GeneratePlanDumpOptions,
) -> Result<Option<pb::MetricColumn>> {
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
    let tasks = (tasks.keys())
        .map(|&task| {
            u32::try_from(task)
                .map_err(|_| internal_datafusion_err!("Task id {task} overflows u32"))
        })
        .collect::<Result<_>>()?;
    let relative_accuracy = opts.relative_accuracy(&metric);
    Ok(Some(pb::MetricColumn {
        metric: Some(metric),
        tasks,
        values: (values.into_iter())
            .map(|value| compress_value(value, relative_accuracy))
            .collect(),
        relative_accuracy,
    }))
}

/// Compresses a metric value into the index of the logarithmic bucket it falls in, which fits all
/// of the u64s in a few thousand buckets even with 0.5% accuracy. Values sharing a bucket become
/// the same number, which compresses well. 0, below the first bucket, is kept as 0.
fn compress_value(value: u64, relative_accuracy: f64) -> u64 {
    // The number of bins and the minimum value only matter to sketches, not to the mapping.
    match (relative_accuracy > 0.0).then(|| Config::new(relative_accuracy, 0, 1.0)) {
        Some(mapping) if value > 0 => mapping.key(value as f64) as u64 + 1,
        _ => value,
    }
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
pub(super) fn metric_task(metric: &Metric) -> Result<u64> {
    let Some(label) =
        (metric.labels().iter()).find(|l| l.name() == DISTRIBUTED_DATAFUSION_TASK_ID_LABEL)
    else {
        return Ok(0);
    };
    let task = label.value();
    task.parse()
        .map_err(|_| internal_datafusion_err!("Invalid task id label '{task}'"))
}

/// The numbers of a metric, which are dumped as part of a [pb::MetricColumn], while the rest of
/// the metric, its kind and name, is dumped once per column.
pub(super) fn metric_values(metric: &mut pb::Metric) -> Result<Vec<&mut u64>> {
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
fn optimize_metric(value: MetricValue) -> Option<MetricValue> {
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
