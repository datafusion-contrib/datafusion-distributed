use datafusion::common::HashMap;
use datafusion::physical_plan::Metric;
use datafusion::physical_plan::metrics::{Label, MetricsSet};
use std::sync::Arc;

/// Same as [MetricsSet::aggregate_by_name], but also keeps every label that is present with the
/// same value in all the aggregated instances of a metric name. Labels that are missing from some
/// instances, or that have different values across them, are dropped, as there is no single value
/// that could represent them in the aggregated metric.
pub(crate) fn aggregate_by_name(metrics: &MetricsSet) -> MetricsSet {
    // Metric names in first-seen order, so the output order is deterministic.
    let mut names = vec![];
    let mut map: HashMap<&str, (Metric, Vec<Label>)> = HashMap::new();

    for metric in metrics.iter() {
        let name = metric.value().name();
        let (accum, labels) = map.entry(name).or_insert_with(|| {
            names.push(name);
            let mut accum =
                Metric::new(metric.value().new_empty(), None).with_type(metric.metric_type());
            if let Some(category) = metric.metric_category() {
                accum = accum.with_category(category);
            }
            (accum, metric.labels().to_vec())
        });
        accum.value_mut().aggregate(metric.value());
        labels.retain(|label| metric.labels().contains(label));
    }

    names
        .into_iter()
        .filter_map(|name| map.remove(name))
        .map(|(accum, labels)| {
            let metric = labels.into_iter().fold(accum, Metric::with_label);
            Arc::new(metric)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::{Count, MetricValue};

    #[test]
    fn keeps_only_labels_shared_by_all_instances() {
        let mut metrics = MetricsSet::new();
        metrics.push(output_rows(
            1,
            vec![("task_id", "0"), ("file", "a.parquet")],
        ));
        metrics.push(output_rows(
            2,
            vec![("task_id", "0"), ("file", "b.parquet")],
        ));
        metrics.push(output_rows(
            3,
            vec![("file", "c.parquet"), ("task_id", "0")],
        ));

        let aggregated = aggregate_by_name(&metrics);

        let metrics = aggregated.iter().collect::<Vec<_>>();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].to_string(), "output_rows{task_id=0}=6");
    }

    #[test]
    fn drops_labels_missing_from_some_instances() {
        let mut metrics = MetricsSet::new();
        metrics.push(output_rows(1, vec![("task_id", "0")]));
        metrics.push(output_rows(2, vec![]));

        let aggregated = aggregate_by_name(&metrics);

        let metrics = aggregated.iter().collect::<Vec<_>>();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].to_string(), "output_rows=3");
    }

    fn output_rows(rows: usize, labels: Vec<(&'static str, &'static str)>) -> Arc<Metric> {
        let count = Count::new();
        count.add(rows);
        let labels = labels.into_iter().map(|(k, v)| Label::new(k, v)).collect();
        Arc::new(Metric::new_with_labels(
            MetricValue::OutputRows(count),
            Some(rows),
            labels,
        ))
    }
}
