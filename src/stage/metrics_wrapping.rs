use std::any::Any;
use std::sync::Arc;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{ExecutionPlan,  PlanProperties, SendableRecordBatchStream};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, Statistics};
use crate::metrics::proto::proto_metrics_set_to_df;
use crate::{ArrowFlightReadExec, ExecutionStage, ProtoMetricsSet};
use datafusion::common::tree_node::{TreeNode, TreeNodeRewriter, Transformed, TreeNodeRecursion};
use datafusion::error::{Result,DataFusionError };
use crate::StageKey;
use std::collections::HashMap;

// Populate metrics in ExecutionStages.
pub struct FullPlanRewriter {
    metrics: HashMap<StageKey, Vec<ProtoMetricsSet>>,
}

impl FullPlanRewriter {
    pub fn new(metrics: HashMap<StageKey, Vec<ProtoMetricsSet>>) -> Self {
        Self { metrics }
    }
}

impl TreeNodeRewriter for FullPlanRewriter {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(exec_stage) = plan.as_any().downcast_ref::<ExecutionStage>() { 
            let mut new_stage = exec_stage.clone();
            for i in 0..exec_stage.tasks.len() {
                let key = StageKey {
                    query_id: exec_stage.query_id.to_string(),
                    task_number: i as u64,
                    stage_id: exec_stage.num as u64,
                };

                let metrics = self.metrics.get(&key).unwrap();
                new_stage.task_metrics.insert(key.clone(), metrics.clone());
            }
            return Ok(Transformed::new(Arc::new(new_stage), true, TreeNodeRecursion::Continue))
        }
        Ok(Transformed::new(plan, false, TreeNodeRecursion::Continue))
    }
}



/// MetricsRewriter is used to enrich a task with metrics
pub struct TaskMetricsRewriter {
    metrics: Vec<ProtoMetricsSet>,
    // idx is the index of the "current" metric set encountered during the tree traversal.
    idx: usize,
}

impl TaskMetricsRewriter {
    /// new creates a new [TaskMetricsRewriter].
    pub fn new(metrics: Vec<ProtoMetricsSet>) -> Self {
        Self {
            metrics,
            idx: 0,
        }
    }

    /// enrich_task_with_metrics rewrites the provided plan with the list of metrics.
    pub fn enrich_task_with_metrics(mut self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(plan.rewrite(&mut self)?.data)
    }
}

// For any tree structure that implements TreeNode
impl TreeNodeRewriter for TaskMetricsRewriter {
    type Node = Arc<dyn ExecutionPlan>; 

    fn f_down(&mut self, plan: Self::Node) -> Result<Transformed<Self::Node>> {
        if let Some(_) = plan.as_any().downcast_ref::<ArrowFlightReadExec>() {
            // Do not recurse into ArrowFlightReadExec.
            return Ok(Transformed::new(plan, false, TreeNodeRecursion::Jump));
        }
        
        // DEBUG: Log what we're processing
        // println!("🔧 TaskMetricsRewriter: processing node '{}' at index {}", plan.name(), self.idx);
        // println!("   ProtoMetricsSet: {:?}", &self.metrics[self.idx]);
        
        // Convert metrics from our proto representation to datafusion metrics.
        let proto_metrics = &self.metrics[self.idx];
        let metrics = proto_metrics_set_to_df(proto_metrics).unwrap();
        
        // println!("   Converted to {} DataFusion metrics", metrics.iter().count());
        // for metric in metrics.iter() {
        //     println!("     - {}: {:?}", metric.value().name(), metric.value());
        // }
        
        let wrapped_plan_node: Arc<dyn ExecutionPlan> = Arc::new(MetricsWrapperExec::new(
            plan.clone(), 
            Some(metrics.clone()),
        ));
        
        // DEBUG: Verify the wrapper has the metrics
        // match wrapped_plan_node.metrics() {
        //     Some(wrapper_metrics) => {
                // println!("   ✅ MetricsWrapperExec has {} metrics", wrapper_metrics.iter().count());
                // for metric in wrapper_metrics.iter() {
                //     println!("      - {}: {:?}", metric.value().name(), metric.value());
                // }
        //     }
        //     None => {
        //         println!("   ❌ MetricsWrapperExec has NO metrics!");
        //     }
        // }
        
        // Transform the node.
        let result = Transformed::new(wrapped_plan_node, true, TreeNodeRecursion::Continue);
        self.idx += 1;
        Ok(result)
    }
}

/// TaskMetrics is a Vec of MetricsSet where `TaskMetrics[i]` represents the metrics for plan node `i` where `i`
/// is the order of the plan node during a pre-order traversal of the plan tree. 
/// Notes:
/// - If there are no metrics for a plan node, an empty MetricsSet is used
/// - Any ExecutionStage in the plan tree (or children of ExecutionStage) are excluded.
type TaskMetrics = Vec<MetricsSet>;


/// A transparent wrapper that delegates all execution to its child
/// but returns custom metrics. This node is invisible during display.
pub struct MetricsWrapperExec {
    wrapped: Arc<dyn ExecutionPlan>,
    /// metrics for this plan node. By convention, plan nodes typicall use None to represent no metrics instead of
    /// an empty MetricsSet.
    metrics: Option<MetricsSet>,

    children: Option<Vec<Arc<dyn ExecutionPlan>>>,
}

impl MetricsWrapperExec {
    pub fn new(wrapped: Arc<dyn ExecutionPlan>, metrics: Option<MetricsSet>) -> Self {
        Self {
            wrapped,
            metrics,
            children: None,
        }
    }
}

impl std::fmt::Debug for MetricsWrapperExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Delegate to child for debug display
        self.wrapped.fmt(f)
    }
}

impl DisplayAs for MetricsWrapperExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // Delegate to child for display - makes wrapper invisible
        self.wrapped.fmt_as(t, f)
    }
}

impl ExecutionPlan for MetricsWrapperExec {
    fn name(&self) -> &str {
        // Delegate to child - wrapper is transparent
        "MetricsWrapperExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.wrapped.properties()   
     }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.children {
            Some(children) => children.iter().collect(),
            None => self.wrapped.children(),
        } 
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return Ok(Arc::new(MetricsWrapperExec{
            wrapped: self.wrapped.clone(),
            metrics: self.metrics.clone(),
            children: Some(children),
        })) 
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Delegate execution completely to child
        self.wrapped.execute(partition, context)
    }

    // metrics returns the wrapped metrics.
    fn metrics(&self) -> Option<MetricsSet> {
        self.metrics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::metrics::{MetricsSet, Metric, MetricValue, Count};
    use crate::test_utils::mock_exec::MockExec;
    use datafusion::arrow::datatypes::{Schema, Field, DataType};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_metrics_wrapper() {
        // Create a base plan
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let mock_exec = Arc::new(MockExec::new(vec![], schema));
        
        // Execute the base plan to generate original metrics
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let _results = collect(mock_exec.clone(), task_ctx).await.unwrap();
        
        // Get original metrics
        let original_metrics = mock_exec.metrics();
        println!("Original metrics:");
        if let Some(metrics) = &original_metrics {
            for metric in metrics.iter() {
                println!("  {} = {}", metric.value().name(), metric.value());
            }
        } else {
            println!("  No original metrics");
        }
        
        // Create custom metrics that override the original ones
        let count = Count::new();
        count.add(99999); // Much larger value to show override
        let custom_metric = Metric::new(MetricValue::OutputRows(count), Some(0));
        let mut custom_metrics = MetricsSet::new();
        custom_metrics.push(Arc::new(custom_metric));
        
        // Wrap with custom metrics
        let wrapper = Arc::new(MetricsWrapperExec::new(
            mock_exec.clone(),
            Some(custom_metrics.clone()),
        ));
        
        // Verify custom metrics override original ones
        let wrapper_metrics = wrapper.metrics().unwrap();
        println!("\nWrapper metrics (should override original):");
        for metric in wrapper_metrics.iter() {
            println!("  {} = {}", metric.value().name(), metric.value());
        }
        
        // Verify the custom metric value is returned (not original)
        let output_rows_metric = wrapper_metrics.iter()
            .find(|m| matches!(m.value(), MetricValue::OutputRows(_)))
            .expect("Should have OutputRows metric");
        
        if let MetricValue::OutputRows(count) = output_rows_metric.value() {
            assert_eq!(count.value(), 99999); // Custom value, not original
            println!("✅ Custom metrics (99999) override original metrics");
        }
        
        // Verify display still shows child name, not wrapper
        let display_str = format!("{:?}", displayable(wrapper.as_ref()));
        assert!(display_str.contains("MockExec"));
        
        // Show EXPLAIN ANALYZE output with custom metrics
        println!("\nEXPLAIN ANALYZE output with custom metrics:");
        let display_with_metrics = datafusion::physical_plan::display::DisplayableExecutionPlan::with_metrics(wrapper.as_ref())
            .indent(true)
            .to_string();
        println!("{}", display_with_metrics);
        
        println!("✅ MetricsWrapperExec successfully overrides inner metrics while staying invisible");
    }
}