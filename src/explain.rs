// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    any::Any,
    fmt::Formatter,
    sync::Arc,
};

use arrow::{
    array::StringArray,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion::{
    execution::TaskContext,
    physical_plan::{
        execution_plan::{Boundedness, EmissionType},
        memory::MemoryStream,
        ExecutionPlan, Partitioning,
        PlanProperties, DisplayAs, DisplayFormatType,
        SendableRecordBatchStream, displayable,
    },
    physical_expr::EquivalenceProperties,
};

/// Custom distributed EXPLAIN execution plan that also returns distributed plan and stages
#[derive(Debug)]
pub struct DistributedExplainExec {
    schema: SchemaRef,
    logical_plan: String,
    physical_plan: String,
    distributed_plan: String,
    distributed_stages: String,
    properties: PlanProperties,
}

impl DistributedExplainExec {
    pub fn new(
        schema: SchemaRef,
        logical_plan: String,
        physical_plan: String,
        distributed_plan: String,
        distributed_stages: String,
    ) -> Self {
        // properties required by the ExecutionPlan trait
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            schema,
            logical_plan,
            physical_plan,
            distributed_plan,
            distributed_stages,
            properties,
        }
    }

    pub fn logical_plan(&self) -> &str {
        &self.logical_plan
    }

    pub fn physical_plan(&self) -> &str {
        &self.physical_plan
    }

    pub fn distributed_plan(&self) -> &str {
        &self.distributed_plan
    }

    pub fn distributed_stages(&self) -> &str {
        &self.distributed_stages
    }

    /// Format distributed stages for display
    pub fn format_distributed_stages(stages: &[crate::planning::DFRayStage]) -> String {
        let mut result = String::new();
        for (i, stage) in stages.iter().enumerate() {
            result.push_str(&format!("Stage {}:\n", stage.stage_id));
            result.push_str(&format!("  Partition Groups: {:?}\n", stage.partition_groups));
            result.push_str(&format!("  Full Partitions: {}\n", stage.full_partitions));
            result.push_str("  Plan:\n");
            let plan_display = format!("{}", displayable(stage.plan.as_ref()).indent(true));
            for line in plan_display.lines() {
                result.push_str(&format!("    {}\n", line));
            }
            if i < stages.len() - 1 {
                result.push('\n');
            }
        }
        if result.is_empty() {
            result.push_str("No distributed stages generated");
        }
        result
    }
}

impl DisplayAs for DistributedExplainExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedExplainExec")
    }
}

impl ExecutionPlan for DistributedExplainExec {
    fn name(&self) -> &str {
        "DistributedExplainExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.schema.clone();
        
        // Create the result data with our 4 plan types
        let plan_types = StringArray::from(vec![
            "logical_plan", 
            "physical_plan", 
            "distributed_plan", 
            "distributed_stages"
        ]);
        let plans = StringArray::from(vec![
            self.logical_plan.as_str(),
            self.physical_plan.as_str(),
            self.distributed_plan.as_str(),
            self.distributed_stages.as_str(),
        ]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(plan_types), Arc::new(plans)],
        ).map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?;

        // Use MemoryStream which is designed for DataFusion execution plans
        let stream = MemoryStream::try_new(vec![batch], schema, None)?;
        
        Ok(Box::pin(stream))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Check if this is an EXPLAIN query (but not EXPLAIN ANALYZE)
/// 
/// This function distinguishes between:
/// - EXPLAIN queries (returns true) - show plan information only
/// - EXPLAIN ANALYZE queries (returns false) - execute and show runtime stats
/// - Regular queries (returns false) - normal query execution
pub fn is_explain_query(query: &str) -> bool {
    let query_upper = query.trim().to_uppercase();
    // Must start with "EXPLAIN" followed by whitespace or end of string
    let is_explain = query_upper.starts_with("EXPLAIN") && 
        (query_upper.len() == 7 || query_upper.chars().nth(7).is_some_and(|c| c.is_whitespace()));
    let is_explain_analyze = query_upper.starts_with("EXPLAIN ANALYZE");
    is_explain && !is_explain_analyze
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_explain_query() {
        // Test EXPLAIN queries (should return true)
        assert!(is_explain_query("EXPLAIN SELECT * FROM table"));
        assert!(is_explain_query("explain select * from table"));
        assert!(is_explain_query("  EXPLAIN  SELECT 1"));
        assert!(is_explain_query("EXPLAIN\nSELECT * FROM test"));

        // Test EXPLAIN ANALYZE queries (should return false)
        assert!(!is_explain_query("EXPLAIN ANALYZE SELECT * FROM table"));
        assert!(!is_explain_query("explain analyze SELECT * FROM table"));
        assert!(!is_explain_query("  EXPLAIN ANALYZE  SELECT 1"));

        // Test regular queries (should return false)
        assert!(!is_explain_query("SELECT * FROM table"));
        assert!(!is_explain_query("INSERT INTO table VALUES (1)"));
        assert!(!is_explain_query("UPDATE table SET col = 1"));
        assert!(!is_explain_query("DELETE FROM table"));
        assert!(!is_explain_query("CREATE TABLE test (id INT)"));

        // Test edge cases
        assert!(!is_explain_query(""));
        assert!(!is_explain_query("   "));
        assert!(!is_explain_query("EXPLAINSELECT"));  // No space
        assert!(is_explain_query("EXPLAIN")); // Just EXPLAIN
    }
}
