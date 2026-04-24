use crate::distributed_planner::NetworkBoundaryExt;
use crate::execution_plans::DistributedExec;
use crate::worker::generated::worker as pb;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;
use std::time::Duration;

/// Waits until all worker tasks have reported their metrics back via the coordinator channel.
pub async fn wait_for_all_metrics(plan: &Arc<dyn ExecutionPlan>) {
    let Some(dist_exec) = plan.as_any().downcast_ref::<DistributedExec>() else {
        return;
    };
    let Ok(prepared) = dist_exec.prepared_plan() else {
        return;
    };
    let mut expected_count = 0usize;
    let _ = prepared.apply(|plan| {
        if let Some(boundary) = plan.as_network_boundary() {
            expected_count += boundary.input_stage().tasks.len();
        }
        Ok(TreeNodeRecursion::Continue)
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while dist_exec.task_metrics.len() < expected_count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Timed out waiting for worker metrics to arrive");
}

/// creates a "distinct" set of metrics from the provided seed
pub fn make_test_metrics_set_proto_from_seed(seed: u64, num_metrics: usize) -> pb::MetricsSet {
    const TEST_TIMESTAMP: i64 = 1758200400000000000; // 2025-09-18 13:00:00 UTC

    let mut result = pb::MetricsSet { metrics: vec![] };

    for i in 0..num_metrics {
        let value = seed + i as u64;
        result.metrics.push(match i % 4 {
            0 => pb::Metric {
                value: Some(pb::metric::Value::OutputRows(pb::OutputRows { value })),
                labels: vec![],
                partition: None,
            },

            1 => pb::Metric {
                value: Some(pb::metric::Value::ElapsedCompute(pb::ElapsedCompute {
                    value,
                })),
                labels: vec![],
                partition: None,
            },
            2 => pb::Metric {
                value: Some(pb::metric::Value::StartTimestamp(pb::StartTimestamp {
                    value: Some(TEST_TIMESTAMP + (value as i64 * 1_000_000_000)),
                })),
                labels: vec![],
                partition: None,
            },
            3 => pb::Metric {
                value: Some(pb::metric::Value::EndTimestamp(pb::EndTimestamp {
                    value: Some(TEST_TIMESTAMP + (value as i64 * 1_000_000_000)),
                })),
                labels: vec![],
                partition: None,
            },
            _ => unreachable!(),
        })
    }
    result
}
