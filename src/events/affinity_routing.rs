use std::collections::{BTreeMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::physical_plan::metrics::{MetricBuilder, MetricCategory, MetricValue, Time};
use url::Url;

use super::defaults::dial_with_failover;
use crate::{
    BytesMetricExt, DistributedConfig, RouteTaskEvent, RouteTaskEventResponse, RouteTaskHandler,
    WorkUnitAffinity,
};

/// Opt-in routing of tasks toward stable, connector-supplied data homes.
///
/// Each distinct affinity key elects a worker by rendezvous hashing. The worker
/// owning the greatest total hinted weight is preferred for the whole task. URL
/// ordering and query/task IDs do not affect the choice. Worker URLs must identify
/// stable cache owners, not a load balancer. Hash stability is within a build; an
/// upgrade may change cache placement without affecting query correctness.
///
/// No cache is inspected: this improves the opportunity for reuse but does not
/// guarantee cache hits or bound worker memory. The normal classified connection
/// retry/failover policy is retained (including overload responses). Embedders that
/// need load-aware admission can use `RouteTaskEvent::work_unit_affinity()` in their
/// own routing handler. No hints or no workers defers to the next routing handler.
///
/// Register with `with_distributed_route_task_handler(AffinityRouteTaskHandler)`.
#[derive(Debug, Default)]
pub struct AffinityRouteTaskHandler;

#[async_trait]
impl RouteTaskHandler for AffinityRouteTaskHandler {
    async fn handle(&self, ev: RouteTaskEvent<'_>) -> Option<Result<RouteTaskEventResponse>> {
        let hints = ev.work_unit_affinity();
        if hints.is_empty() {
            return None;
        }
        let duration = Time::new();
        let _timer = duration.timer();
        let weights = Self::distinct_weights(&hints);
        if weights.is_empty() {
            return None;
        }
        let candidates = ev
            .worker_resolver
            .get_urls()
            .map(|urls| Self::rank_weighted_workers(&weights, &urls));
        if candidates.as_ref().is_ok_and(Vec::is_empty) {
            return None;
        }

        // Only non-deferred invocations count as attempts. Keep resolver/configuration
        // errors observable too, and time selection plus dialing (including backoff).
        let metric = || MetricBuilder::new(ev.metrics);
        metric()
            .global_counter("work_unit_affinity_routed_tasks")
            .add(1);
        let preferred = metric().global_counter("work_unit_affinity_preferred_placements");
        let fallback = metric().global_counter("work_unit_affinity_fallback_placements");
        let failures = metric().global_counter("work_unit_affinity_routing_failures");
        metric()
            .with_category(MetricCategory::Timing)
            .build(MetricValue::Time {
                name: "work_unit_affinity_routing_duration".into(),
                time: duration.clone(),
            });
        metric()
            .global_counter("work_unit_affinity_hinted_objects")
            .add(weights.len());
        // Metrics use usize; saturate rather than truncate or wrap large estimates.
        let bytes = weights.values().fold(0_usize, |total, &weight| {
            total.saturating_add(usize::try_from(weight).unwrap_or(usize::MAX))
        });
        metric()
            .bytes_counter("work_unit_affinity_hinted_bytes")
            .add_bytes(bytes);
        drop(weights);

        let result: Result<_> = async {
            let candidates = candidates?;
            let url = candidates[0].clone(); // Empty lists defer above.
            let config = DistributedConfig::from_task_context(ev.task_ctx)?;
            let response =
                dial_with_failover(ev.dialer, url.clone(), candidates, ev.metrics, config).await?;
            Ok((response, url))
        }
        .await;
        Some(match result {
            Ok((response, url)) => {
                if response.url == url {
                    preferred.add(1);
                } else {
                    fallback.add(1);
                }
                Ok(response)
            }
            Err(error) => {
                failures.add(1);
                Err(error)
            }
        })
    }
}

impl AffinityRouteTaskHandler {
    /// Ranks unique worker URLs by weighted affinity. Useful for custom routing
    /// handlers that want the same data homes but different admission policies.
    /// Zero-weight hints are ignored. Duplicate keys use their maximum weight:
    /// repeated scans reuse bytes but do not share decoded/execution state. Equal
    /// scores are broken by a hash of the distinct key set and worker URL, avoiding
    /// systematic preference for low URLs while preserving order independence.
    pub fn rank_workers(hints: &[WorkUnitAffinity], workers: &[Url]) -> Vec<Url> {
        Self::rank_weighted_workers(&Self::distinct_weights(hints), workers)
    }

    fn distinct_weights(hints: &[WorkUnitAffinity]) -> BTreeMap<&str, u64> {
        let mut weights = BTreeMap::<&str, u64>::new();
        for hint in hints {
            if hint.weight > 0 {
                let weight = weights.entry(&hint.key).or_default();
                *weight = (*weight).max(hint.weight);
            }
        }
        weights
    }

    fn rank_weighted_workers(weights: &BTreeMap<&str, u64>, workers: &[Url]) -> Vec<Url> {
        if weights.is_empty() {
            return Vec::new();
        }
        let mut workers = workers.to_vec();
        workers.sort_unstable_by(|a, b| a.as_str().cmp(b.as_str()));
        workers.dedup();
        if workers.is_empty() {
            return workers;
        }
        let mut scores = vec![0_u128; workers.len()];
        // BTreeMap gives a canonical, deduplicated key order. Neither resolver order
        // nor the number/order of references to an object should affect score ties.
        let mut key_set = DefaultHasher::new();
        for (key, weight) in weights {
            key.hash(&mut key_set);
            let owner = workers
                .iter()
                .enumerate()
                .max_by_key(|(_, url)| {
                    let mut hasher = DefaultHasher::new();
                    (key, url.as_str()).hash(&mut hasher);
                    hasher.finish()
                })
                .expect("nonempty workers")
                .0;
            scores[owner] += u128::from(*weight);
        }
        let key_set = key_set.finish();
        let mut ranked: Vec<_> = workers
            .into_iter()
            .zip(scores)
            .map(|(url, score)| {
                let mut tie = DefaultHasher::new();
                ("affinity-score-tie", key_set, url.as_str()).hash(&mut tie);
                (url, score, tie.finish())
            })
            .collect();
        ranked.sort_by(|(a, a_score, a_tie), (b, b_score, b_tie)| {
            b_score
                .cmp(a_score)
                .then(b_tie.cmp(a_tie))
                .then(a.as_str().cmp(b.as_str())) // Only for a hash collision.
        });
        ranked.into_iter().map(|(url, _, _)| url).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    use datafusion::arrow::datatypes::Schema;
    use datafusion::common::{DataFusionError, exec_datafusion_err};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
    use datafusion::prelude::SessionConfig;
    use uuid::Uuid;

    use crate::common::RetryOutcome;
    use crate::events::{TaskWorkUnitAffinity, new_coordinator_to_worker_dialer};
    use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
    use crate::{TaskKey, WorkerResolver};

    #[tokio::test]
    async fn routing_metrics_distinguish_placements_from_connection_attempts() {
        let retry_same = || RetryOutcome::SameUrl.tag(exec_datafusion_err!("retry same worker"));
        let retry_other = || RetryOutcome::OtherUrl.tag(exec_datafusion_err!("try another worker"));
        for (errors, expected, calls) in [
            (vec![], [1, 0, 0], 1),
            (vec![retry_same()], [1, 0, 0], 2),
            (vec![retry_other()], [0, 1, 0], 2),
            (vec![retry_other(), retry_other()], [0, 0, 1], 2),
            (vec![exec_datafusion_err!("terminal error")], [0, 0, 1], 1),
        ] {
            let (result, metrics, dialed) =
                route(errors, &InMemoryWorkerResolver::new(3), metric_hints()).await;
            assert_eq!(result.unwrap().is_ok(), expected[2] == 0);
            assert_eq!(dialed.len(), calls);
            for (name, count) in [
                ("routed_tasks", 1),
                ("preferred_placements", expected[0]),
                ("fallback_placements", expected[1]),
                ("routing_failures", expected[2]),
                ("hinted_objects", 2),
                ("hinted_bytes", 150),
            ] {
                assert_eq!(affinity_value(&metrics, name), count, "{name}");
            }
            assert!(affinity_value(&metrics, "routing_duration") > 0);
        }
    }

    #[tokio::test]
    async fn deferred_routing_does_not_record_affinity_attempts() {
        for (workers, hints) in [
            (0, metric_hints()),
            (2, vec![]),
            (2, vec![WorkUnitAffinity::new("zero", 0)]),
        ] {
            let (result, metrics, dialed) =
                route(vec![], &InMemoryWorkerResolver::new(workers), hints).await;
            assert!(result.is_none());
            assert!(dialed.is_empty());
            assert_eq!(metrics.iter().count(), 0);
        }
    }

    #[tokio::test]
    async fn resolver_errors_are_counted_as_failed_routes_without_dialing() {
        let (result, metrics, dialed) = route(vec![], &FailingResolver, metric_hints()).await;
        assert!(result.unwrap().is_err());
        assert!(dialed.is_empty());
        for name in ["routed_tasks", "routing_failures"] {
            assert_eq!(affinity_value(&metrics, name), 1);
        }
    }

    #[tokio::test]
    async fn large_hinted_byte_totals_saturate_per_route() {
        let hints = vec![
            WorkUnitAffinity::new("a", u64::MAX),
            WorkUnitAffinity::new("b", 1),
        ];
        let (result, metrics, _) = route(vec![], &InMemoryWorkerResolver::new(2), hints).await;
        assert!(result.unwrap().is_ok());
        assert_eq!(affinity_value(&metrics, "hinted_bytes"), usize::MAX);
    }

    #[test]
    fn homes_are_order_independent_and_repeated_keys_do_not_inflate_weight() {
        let workers: Vec<_> = (0..5)
            .map(|i| Url::parse(&format!("http://worker-{i}")).unwrap())
            .collect();
        let hints = vec![
            WorkUnitAffinity::new("s3://bucket/a", 100),
            WorkUnitAffinity::new("s3://bucket/b", 10),
        ];
        let expected = AffinityRouteTaskHandler::rank_workers(&hints, &workers);
        let mut reversed = workers.clone();
        reversed.reverse();
        reversed.push(workers[0].clone());
        assert_eq!(
            expected,
            AffinityRouteTaskHandler::rank_workers(&hints, &reversed)
        );
        let mut repeated = hints.clone();
        repeated.extend(std::iter::repeat_n(hints[1].clone(), 100));
        assert_eq!(
            expected,
            AffinityRouteTaskHandler::rank_workers(&repeated, &workers)
        );
        assert_eq!(
            expected[0],
            AffinityRouteTaskHandler::rank_workers(&hints[..1], &workers)[0]
        );
        assert!(AffinityRouteTaskHandler::rank_workers(&[], &workers).is_empty());
    }

    #[test]
    fn equal_weight_multi_file_tasks_do_not_prefer_low_worker_urls() {
        let workers: Vec<_> = (0..10)
            .map(|i| Url::parse(&format!("http://worker-{i}")).unwrap())
            .collect();
        let reversed_workers: Vec<_> = workers.iter().rev().cloned().collect();
        let mut counts = vec![0; workers.len()];
        for task in 0..20_000 {
            let hints = [
                WorkUnitAffinity::new(format!("s3://bucket/a-{task}"), 1),
                WorkUnitAffinity::new(format!("s3://bucket/b-{task}"), 1),
            ];
            let ranked = AffinityRouteTaskHandler::rank_workers(&hints, &workers);
            let owner = workers.iter().position(|url| *url == ranked[0]).unwrap();
            counts[owner] += 1;
            if task < 100 {
                // Key order, repeated references and resolver order must not affect ties.
                let repeated = [hints[1].clone(), hints[0].clone(), hints[0].clone()];
                assert_eq!(
                    ranked,
                    AffinityRouteTaskHandler::rank_workers(&repeated, &reversed_workers)
                );
            }
        }
        // Fixed inputs; a broad +/-20% bound detects URL-order bias, not performance.
        assert!(
            counts.iter().all(|&count| (1600..=2400).contains(&count)),
            "{counts:?}"
        );
    }

    #[test]
    fn removing_a_non_owner_keeps_the_home_and_removing_owner_fails_over() {
        let workers: Vec<_> = (0..4)
            .map(|i| Url::parse(&format!("http://worker-{i}")).unwrap())
            .collect();
        let hints = [WorkUnitAffinity::new("immutable-object", 1)];
        let ranked = AffinityRouteTaskHandler::rank_workers(&hints, &workers);
        let without_other: Vec<_> = workers
            .iter()
            .filter(|u| **u != ranked[1])
            .cloned()
            .collect();
        assert_eq!(
            ranked[0],
            AffinityRouteTaskHandler::rank_workers(&hints, &without_other)[0]
        );
        let without_owner: Vec<_> = workers
            .iter()
            .filter(|u| **u != ranked[0])
            .cloned()
            .collect();
        let next = AffinityRouteTaskHandler::rank_workers(&hints, &without_owner);
        assert_ne!(ranked[0], next[0]);
    }

    fn affinity_value(metrics: &MetricsSet, name: &str) -> usize {
        let name = format!("work_unit_affinity_{name}");
        // sum_by_name excludes custom byte counters.
        metrics
            .sum(|metric| metric.value().name() == name)
            .unwrap()
            .as_usize()
    }

    struct FailingResolver;

    impl WorkerResolver for FailingResolver {
        fn get_urls(&self) -> Result<Vec<Url>> {
            Err(exec_datafusion_err!("resolver unavailable"))
        }
    }

    fn metric_hints() -> Vec<WorkUnitAffinity> {
        vec![
            WorkUnitAffinity::new("a", 100),
            WorkUnitAffinity::new("a", 20),
            WorkUnitAffinity::new("b", 50),
            WorkUnitAffinity::new("b", 50),
            WorkUnitAffinity::new("zero", 0),
        ]
    }

    async fn route(
        errors: Vec<DataFusionError>,
        worker_resolver: &dyn WorkerResolver,
        hints: Vec<WorkUnitAffinity>,
    ) -> (Option<Result<RouteTaskEventResponse>>, MetricsSet, Vec<Url>) {
        let errors = Mutex::new(errors.into_iter());
        let dialed = Mutex::new(Vec::new());
        let dialer = new_coordinator_to_worker_dialer(|url| {
            dialed.lock().unwrap().push(url.clone());
            let error = errors.lock().unwrap().next();
            async move {
                if let Some(error) = error {
                    return Err(error);
                }
                Ok(RouteTaskEventResponse {
                    url,
                    worker_to_coordinator_stream: Box::pin(futures::stream::empty()),
                })
            }
        });
        let mut config =
            SessionConfig::new().with_extension(Arc::new(TaskWorkUnitAffinity(hints.into())));
        config.options_mut().extensions.insert(DistributedConfig {
            max_coordinator_channel_retries: 1,
            coordinator_channel_retry_initial_backoff_ms: 0,
            ..Default::default()
        });
        let ctx = Arc::new(TaskContext::default().with_session_config(config));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let metrics = ExecutionPlanMetricsSet::new();
        let result = AffinityRouteTaskHandler
            .handle(RouteTaskEvent {
                task_ctx: &ctx,
                metrics: &metrics,
                worker_resolver,
                task_key: TaskKey {
                    query_id: Uuid::nil(),
                    stage_id: 1,
                    task_number: 0,
                },
                task_count: 3,
                task_specialized_plan: &plan,
                dialer: &dialer,
            })
            .await;
        let dialed = dialed.lock().unwrap().clone();
        (result, metrics.clone_inner(), dialed)
    }
}
