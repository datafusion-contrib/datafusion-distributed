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

//! Shared test utilities for DataFusion Distributed tests
//! 
//! This module contains common test helper functions that are used across
//! multiple test modules to avoid code duplication.

#[cfg(test)]
pub mod explain_test_helpers {
    use arrow_flight::decode::FlightRecordBatchStream;
    use datafusion::physical_plan::ExecutionPlan;
    use futures::StreamExt;
    
    use crate::{
        flight_handlers::FlightRequestHandler,
        protobuf::{DistributedExplainExecNode, TicketStatementData},
        proxy_service::DfRayProxyHandler,
        query_planner::{QueryPlan, QueryPlanner},
    };

    /// Create a test FlightRequestHandler for testing
    pub fn create_test_flight_handler() -> FlightRequestHandler {
        FlightRequestHandler::new(QueryPlanner::new())
    }

    /// Create a test DfRayProxyHandler for testing - bypasses worker discovery initialization
    pub fn create_test_proxy_handler() -> DfRayProxyHandler {
        // Create the handler directly without calling new() to avoid worker discovery
        // during test initialization.
        DfRayProxyHandler {
            flight_handler: FlightRequestHandler::new(QueryPlanner::new()),
        }
    }

    /// Create TicketStatementData for EXPLAIN testing from QueryPlan
    pub fn create_explain_ticket_statement_data(plans: QueryPlan) -> TicketStatementData {
        let explain_data = plans.explain_data.map(|data| {
            DistributedExplainExecNode {
                schema: data.schema().as_ref().try_into().ok(),
                logical_plan: data.logical_plan().to_string(),
                physical_plan: data.physical_plan().to_string(),
                distributed_plan: data.distributed_plan().to_string(),
                distributed_stages: data.distributed_stages().to_string(),
            }
        });
        
        TicketStatementData {
            query_id: plans.query_id,
            stage_id: plans.final_stage_id,
            stage_addrs: Some(plans.worker_addresses.into()),
            schema: Some(plans.schema.as_ref().try_into().unwrap()),
            explain_data,
        }
    }

    /// Consume a DoGetStream and verify it contains expected EXPLAIN results
    pub async fn verify_explain_stream_results(stream: crate::flight::DoGetStream) {
        
        // Convert the stream to a FlightRecordBatchStream and consume it
        // Map Status errors to FlightError to match the expected stream type
        let mapped_stream = stream.map(|result| {
            result.map_err(|status| arrow_flight::error::FlightError::from(status))
        });
        let mut flight_stream = FlightRecordBatchStream::new_from_flight_data(mapped_stream);
        
        let mut batches = Vec::new();
        while let Some(batch_result) = flight_stream.next().await {
            let batch = batch_result.expect("Failed to get batch from stream");
            batches.push(batch);
        }
        
        // Verify we got exactly one batch with EXPLAIN results
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        
        // Verify schema: should have 2 columns (plan_type, plan)
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "plan_type");
        assert_eq!(batch.schema().field(1).name(), "plan");
        
        // Verify we have 4 rows (logical_plan, physical_plan, distributed_plan, distributed_stages)
        assert_eq!(batch.num_rows(), 4);
        
        // Verify the plan_type column contains the expected values
        let plan_type_column = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert_eq!(plan_type_column.value(0), "logical_plan");
        assert_eq!(plan_type_column.value(1), "physical_plan");
        assert_eq!(plan_type_column.value(2), "distributed_plan");
        assert_eq!(plan_type_column.value(3), "distributed_stages");
        
        // Verify the plan column contains actual plan content
        let plan_column = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
        assert!(plan_column.value(0).contains("Projection: Int64(1) AS test_col"));
        assert!(plan_column.value(1).contains("ProjectionExec"));
        assert!(plan_column.value(2).contains("RayStageExec"));
        assert!(plan_column.value(3).contains("Stage 0:"));
    }
} 