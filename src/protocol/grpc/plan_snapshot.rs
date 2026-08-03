use super::generated::worker as pb;
use super::metrics_proto::{df_metrics_set_to_proto, metrics_set_proto_to_df};
use crate::coordinator::DistributedExec;
use crate::distributed_planner::NetworkBoundaryExt;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::metrics::MetricsSet;
use prost::Message;

#[derive(Clone, PartialEq, prost::Message)]
struct PlanNodeSnapshot {
    #[prost(string, tag = "1")]
    display_name: String,
    #[prost(message, optional, tag = "2")]
    metrics: Option<pb::MetricsSet>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct PlanSnapshot {
    #[prost(message, repeated, tag = "1")]
    nodes: Vec<PlanNodeSnapshot>,
}

/// Encodes the post-execution plan as a base64 protobuf `PlanSnapshot`.
///
/// Walks the full plan tree (including worker stages reachable through network
/// boundaries) and captures each node's display name and metrics.
pub fn encode(plan: &dyn ExecutionPlan) -> Result<String> {
    let mut raw: Vec<(String, Option<MetricsSet>)> = Vec::new();

    let start: &dyn ExecutionPlan = if plan.is::<DistributedExec>() {
        raw.push(("DistributedExec".to_string(), plan.metrics()));
        match plan.children().into_iter().next() {
            Some(child) => child.as_ref(),
            None => return Ok(STANDARD.encode(PlanSnapshot { nodes: vec![] }.encode_to_vec())),
        }
    } else {
        plan
    };

    collect_nodes(start, &mut raw);

    let nodes = raw
        .into_iter()
        .map(|(name, metrics)| PlanNodeSnapshot {
            display_name: name,
            metrics: metrics
                .as_ref()
                .and_then(|m| df_metrics_set_to_proto(m).ok()),
        })
        .collect();

    Ok(STANDARD.encode(PlanSnapshot { nodes }.encode_to_vec()))
}

/// Decodes a base64 `PlanSnapshot` produced by [`encode`] into a human-readable string.
pub fn decode(b64: &str) -> Result<String> {
    let bytes = STANDARD
        .decode(b64)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let snapshot = PlanSnapshot::decode(bytes.as_slice())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let mut out = String::new();
    for node in &snapshot.nodes {
        let metrics_str = node
            .metrics
            .as_ref()
            .and_then(|m| metrics_set_proto_to_df(m).ok())
            .map(|m| {
                let parts: Vec<String> = m
                    .iter()
                    .map(|metric| format!("{}", metric.value()))
                    .collect();
                if parts.is_empty() {
                    String::new()
                } else {
                    format!(" [{}]", parts.join(", "))
                }
            })
            .unwrap_or_default();
        out.push_str(&format!("{}{}\n", node.display_name, metrics_str));
    }
    Ok(out)
}

fn collect_nodes(plan: &dyn ExecutionPlan, nodes: &mut Vec<(String, Option<MetricsSet>)>) {
    let name = displayable(plan).one_line().to_string();
    nodes.push((name.trim_end().to_string(), plan.metrics()));

    if let Some(nb) = plan.as_network_boundary() {
        if let Some(stage_plan) = nb.input_stage().local_plan() {
            collect_nodes(stage_plan.as_ref(), nodes);
        }
        return;
    }

    for child in plan.children() {
        collect_nodes(child.as_ref(), nodes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::DistributedExec;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;
    use std::sync::Arc;

    #[test]
    fn test_encode_decode_roundtrip() {
        let plan = EmptyExec::new(Arc::new(Schema::empty()));
        let encoded = encode(&plan).unwrap();
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.contains("EmptyExec"));
    }

    #[test]
    fn test_distributed_exec_root_appears_first_in_preorder() {
        let schema = Arc::new(Schema::empty());
        let plan = DistributedExec::new(Arc::new(EmptyExec::new(schema)));

        let encoded = encode(&plan).unwrap();
        let decoded = decode(&encoded).unwrap();
        let lines: Vec<&str> = decoded.lines().collect();

        assert_eq!(lines.len(), 2, "expected DistributedExec + one child");
        assert!(
            lines[0].contains("DistributedExec"),
            "root must be DistributedExec, got: {}",
            lines[0]
        );
        assert!(
            lines[1].contains("EmptyExec"),
            "child must be EmptyExec, got: {}",
            lines[1]
        );
    }
}
