use std::collections::HashMap;
use datafusion::{error::DataFusionError, physical_plan::{metrics::{Metric, MetricsSet}, ExecutionPlan}};
use crate::ExecutionStage;

use crate::stage::StageKey;
use datafusion::physical_plan::{ExecutionPlanVisitor, accept};

// MetricsCollector is used to collect metrics from a plan tree.
pub struct MetricsCollector {
    /// metrics contains the metrics for the current `ExecutionStage`.
    task_metrics: Vec<MetricsSet>,
    /// task_metrics contains the metrics for child tasks from other `ExecutionStage`s.
    child_task_metrics: HashMap<StageKey, Vec<MetricsSet>>,
}

impl ExecutionPlanVisitor for MetricsCollector {
    type Error = DataFusionError;

    fn pre_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        // If the plan is an ExecutionStage, assume it has collected metrics already.
        // Instead of traversing further, we can collect its task metrics.
        if let Some(child_stage) = plan.as_any().downcast_ref::<ExecutionStage>() {
            for (stage_key, plan_metrics) in child_stage.task_metrics.iter() {
                // TODO: copying
                // If we already have seen metrics for a task, we might see it again because
                // different tasks in different stages may consume from the same one.
                // TODO: we are trying to avoid such plans because they aren't useful for performance.
                match self.child_task_metrics.get_mut(stage_key){
                    Some(existing_plan_metrics) => {
                        // If two tasks have the same key, they must have the same plan, so the length
                        // of the metrics here should be the same.
                        if existing_plan_metrics.len() != plan_metrics.len() {
                            return Err(DataFusionError::Internal(
                                format!("task metrics length mismatch for key {}", stage_key).to_string(),
                            ));
                        }
                        // Merge the MetricsSets for each plan node.
                        existing_plan_metrics.into_iter().zip(plan_metrics.iter()).for_each(
                            |(existing, new)| {
                                new.iter().for_each(
                                    |metric| {
                                        existing.push(metric.clone());
                                    },
                                )
                            },
                        );
                    },
                     None => {
                         // TODO: copying is not ideal.
                         self.child_task_metrics.insert(stage_key.clone(), plan_metrics.clone());
                    }
                }
            }
            self.child_task_metrics.extend(
                child_stage.task_metrics.iter().map(|(k, v)| (k.clone(), v.clone())));
            return Ok(false);
        }
        // TODO: can this be compressed better?
        match plan.metrics() {
            Some(metrics) => {
                self.task_metrics.push(metrics.clone())
            },
            None => {
                self.task_metrics.push(MetricsSet::new())
            }
        }
        Ok(true)
    }

    fn post_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            task_metrics: Vec::new(),
            child_task_metrics: HashMap::new(),
        }
    }

    // collect metrics from an ExecutionStage plan and any child ExecutionStages.
    pub fn collect(mut self, stage: &ExecutionStage, key: StageKey) -> Result<(), DataFusionError> {
        accept(stage.plan.as_ref(), &mut self)?;
        self.child_task_metrics.insert(key, self.task_metrics);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::*;
    use datafusion::arrow::{
        datatypes::{Schema, Field, DataType}, 
        array::{RecordBatch, Int32Array, StringArray}
    };
    use datafusion::physical_plan::collect;
    use datafusion::datasource::MemTable;
    use uuid::Uuid;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_metrics_collector() {
        // Create a more complex plan with aggregation and joins
        let session_ctx = SessionContext::new();
        
        // Create sample data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));
        
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["A", "B", "A", "B"])),
                Arc::new(Int32Array::from(vec![100, 200, 150, 250])),
            ],
        ).unwrap();
        
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap());
        session_ctx.register_table("test_table", table).unwrap();
        
        // Create a complex query with aggregation and filtering
        let sql = "SELECT name, COUNT(*) as count, SUM(value) as total 
                   FROM test_table 
                   WHERE value > 120 
                   GROUP BY name 
                   ORDER BY total DESC";
        
        let logical_plan = session_ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
        
        // Execute to generate metrics
        let task_ctx = session_ctx.task_ctx();
        let _results = collect(logical_plan.clone(), task_ctx).await.unwrap();
        
        // Wrap in ExecutionStage
        let stage = ExecutionStage {
            query_id: Uuid::new_v4(),
            num: 1,
            name: "ComplexTestStage".to_string(),
            plan: logical_plan,
            inputs: vec![],
            tasks: vec![],
            depth: 0,
            task_metrics: HashMap::new(),
        };
        
        // Create StageKey
        let stage_key = StageKey {
            query_id: stage.query_id.to_string(),
            stage_id: stage.num as u64,
            task_number: 0,
        };
        
        // Test MetricsCollector
        let collector = MetricsCollector::new();
        collector.collect(&stage, stage_key).unwrap();
        
        // The collector should have processed the plan and collected metrics
        // Since we executed the plan, it should have generated some metrics
        println!("Successfully collected metrics from complex execution plan");
    }
}