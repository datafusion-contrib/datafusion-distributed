use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    sql::{ProstMessageExt, TicketStatementQuery},
    FlightEndpoint, FlightInfo, Ticket,
};
use datafusion::physical_plan::{
    coalesce_partitions::CoalescePartitionsExec, ExecutionPlan, Partitioning,
};
use futures::TryStreamExt;
use prost::Message;
use tonic::{Response, Status};

use crate::{
    explain::DistributedExplainExec,
    logging::{debug, trace},
    planning::{add_ctx_extentions, get_ctx},
    protobuf::{DistributedExplainExecNode, TicketStatementData},
    query_planner::QueryPlanner,
    result::Result,
    stage_reader::DFRayStageReaderExec,
    util::{display_plan_with_partition_counts, get_addrs},
    vocab::Addrs,
};

/// Handler for Arrow Flight SQL requests and responses
pub struct FlightRequestHandler {
    pub planner: QueryPlanner,
}

impl FlightRequestHandler {
    pub fn new(planner: QueryPlanner) -> Self {
        Self { planner }
    }

    /// Create a FlightInfo response with the given ticket data
    pub fn create_flight_info_response(
        &self,
        query_id: String,
        final_addrs: Addrs,
        final_stage_id: u64,
        schema: SchemaRef,
        explain_data: Option<DistributedExplainExecNode>,
    ) -> Result<FlightInfo, Status> {
        let mut flight_info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Could not create flight info {e:?}")))?;

        let ticket_data = TicketStatementData {
            query_id,
            stage_id: final_stage_id,
            stage_addrs: Some(final_addrs.into()),
            schema: Some(
                schema
                    .try_into()
                    .map_err(|e| Status::internal(format!("Could not convert schema {e:?}")))?,
            ),
            explain_data,
        };

        let ticket = Ticket::new(
            TicketStatementQuery {
                statement_handle: ticket_data.encode_to_vec().into(),
            }
            .as_any()
            .encode_to_vec(),
        );

        let endpoint = FlightEndpoint::new().with_ticket(ticket);
        flight_info = flight_info.with_endpoint(endpoint);

        Ok(flight_info)
    }

    /// Handle EXPLAIN query requests by preparing plans for visualization.
    ///
    /// EXPLAIN queries return comprehensive plan information including logical, physical,
    /// distributed plan, and execution stages for analysis and debugging purposes.
    pub async fn handle_explain_request(
        &self,
        query: &str,
    ) -> Result<Response<FlightInfo>, Status> {
        let plans = self
            .planner
            .prepare_explain(query)
            .await
            .map_err(|e| Status::internal(format!("Could not prepare EXPLAIN query {e:?}")))?;

        debug!("get flight info: EXPLAIN query id {}", plans.query_id);

        let explain_data = plans.explain_data.map(|data| DistributedExplainExecNode {
            schema: data.schema().as_ref().try_into().ok(),
            logical_plan: data.logical_plan().to_string(),
            physical_plan: data.physical_plan().to_string(),
            distributed_plan: data.distributed_plan().to_string(),
            distributed_stages: data.distributed_stages().to_string(),
        });

        let flight_info = self.create_flight_info_response(
            plans.query_id,
            plans.worker_addresses,
            plans.final_stage_id,
            plans.schema,
            explain_data,
        )?;

        trace!("get_flight_info_statement done for EXPLAIN");
        Ok(Response::new(flight_info))
    }

    /// Handle query requests by preparing execution plans and stages.
    ///
    /// Query focus on execution readiness, returning only the essential
    /// metadata needed to execute the distributed query plan.
    pub async fn handle_query_request(&self, query: &str) -> Result<Response<FlightInfo>, Status> {
        let query_plan = self
            .planner
            .prepare_query(query)
            .await
            .map_err(|e| Status::internal(format!("Could not prepare query {e:?}")))?;

        debug!("get flight info: query id {}", query_plan.query_id);

        let flight_info = self.create_flight_info_response(
            query_plan.query_id,
            query_plan.worker_addresses,
            query_plan.final_stage_id,
            query_plan.schema,
            None, // Regular queries don't have explain data
        )?;

        trace!("get_flight_info_statement done");
        Ok(Response::new(flight_info))
    }

    /// Handle execution of EXPLAIN statement queries.
    ///
    /// This function does not execute the plan but returns all plans we want to display to the user.
    pub async fn handle_explain_statement_execution(
        &self,
        tsd: TicketStatementData,
        remote_addr: &str,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        let explain_data = tsd.explain_data.as_ref().ok_or_else(|| {
            Status::internal("No explain_data in TicketStatementData for EXPLAIN query")
        })?;

        let schema: Schema = explain_data
            .schema
            .as_ref()
            .ok_or_else(|| Status::internal("No schema in ExplainData"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Cannot convert schema {e}")))?;

        let explain_plan = Arc::new(DistributedExplainExec::new(
            Arc::new(schema),
            explain_data.logical_plan.clone(),
            explain_data.physical_plan.clone(),
            explain_data.distributed_plan.clone(),
            explain_data.distributed_stages.clone(),
        )) as Arc<dyn ExecutionPlan>;

        debug!(
            "EXPLAIN request for query_id {} from {} explain plan:\n{}",
            tsd.query_id,
            remote_addr,
            display_plan_with_partition_counts(&explain_plan)
        );

        // Create dummy addresses for EXPLAIN execution
        let mut dummy_addrs = std::collections::HashMap::new();
        let mut partition_addrs = std::collections::HashMap::new();
        partition_addrs.insert(
            0u64,
            vec![("explain_local".to_string(), "local".to_string())],
        );
        dummy_addrs.insert(0u64, partition_addrs);

        self.execute_plan_and_build_stream(explain_plan, tsd.query_id, dummy_addrs)
            .await
    }

    /// Handle execution of regular statement queries
    ///
    /// This function executes the plan and returns the results to the client.
    pub async fn handle_regular_statement_execution(
        &self,
        tsd: TicketStatementData,
        remote_addr: &str,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        let schema: Schema = tsd
            .schema
            .as_ref()
            .ok_or_else(|| Status::internal("No schema in TicketStatementData"))?
            .try_into()
            .map_err(|e| Status::internal(format!("Cannot convert schema {e}")))?;

        // Create an Addrs from the final stage information in the tsd
        let stage_addrs = tsd.stage_addrs.ok_or_else(|| {
            Status::internal("No stages_addrs in TicketStatementData, cannot proceed")
        })?;

        let addrs: Addrs = get_addrs(&stage_addrs).map_err(|e| {
            Status::internal(format!("Cannot get addresses from stage_addrs {e:?}"))
        })?;

        trace!("calculated addrs: {:?}", addrs);

        // Validate that addrs contains exactly one stage
        self.validate_single_stage_addrs(&addrs, tsd.stage_id)?;

        let stage_partition_addrs = addrs.get(&tsd.stage_id).ok_or_else(|| {
            Status::internal(format!(
                "No partition addresses found for stage_id {}",
                tsd.stage_id
            ))
        })?;

        let plan = Arc::new(
            DFRayStageReaderExec::try_new(
                Partitioning::UnknownPartitioning(stage_partition_addrs.len()),
                Arc::new(schema),
                tsd.stage_id,
            )
            // TODO: revisit this to allow for consuming a particular partition
            .map(|stg| CoalescePartitionsExec::new(Arc::new(stg)))
            .map_err(|e| Status::internal(format!("Unexpected error {e}")))?,
        ) as Arc<dyn ExecutionPlan>;

        debug!(
            "request for query_id {} from {} reader plan:\n{}",
            tsd.query_id,
            remote_addr,
            display_plan_with_partition_counts(&plan)
        );

        self.execute_plan_and_build_stream(plan, tsd.query_id, addrs)
            .await
    }

    /// Validate that addresses contain exactly one stage with the expected stage_id
    pub fn validate_single_stage_addrs(
        &self,
        addrs: &Addrs,
        expected_stage_id: u64,
    ) -> Result<(), Status> {
        if addrs.len() != 1 {
            return Err(Status::internal(format!(
                "Expected exactly one stage in addrs, got {}",
                addrs.len()
            )));
        }
        if !addrs.contains_key(&expected_stage_id) {
            return Err(Status::internal(format!(
                "No addresses found for stage_id {} in addrs",
                expected_stage_id
            )));
        }
        Ok(())
    }

    /// Execute a plan and build the response stream.
    ///
    /// This function handles the common execution logic for both regular queries (which return data)
    /// and EXPLAIN queries (which return plan information as text).
    pub async fn execute_plan_and_build_stream(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        query_id: String,
        addrs: Addrs,
    ) -> Result<Response<crate::flight::DoGetStream>, Status> {
        let mut ctx =
            get_ctx().map_err(|e| Status::internal(format!("Could not create context {e:?}")))?;

        add_ctx_extentions(&mut ctx, "proxy", &query_id, addrs, None)
            .map_err(|e| Status::internal(format!("Could not add context extensions {e:?}")))?;

        // TODO: revisit this to allow for consuming a partitular partition
        trace!("calling execute plan");
        let partition = 0;
        let stream = plan
            .execute(partition, ctx.task_ctx())
            .map_err(|e| {
                Status::internal(format!(
                    "Error executing plan for query_id {} partition {}: {e:?}",
                    query_id, partition
                ))
            })?
            .map_err(|e| FlightError::ExternalError(Box::new(e)));

        let out_stream = FlightDataEncoderBuilder::new()
            .build(stream)
            .map_err(move |e| Status::internal(format!("Unexpected error building stream {e:?}")));

        Ok(Response::new(Box::pin(out_stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::explain_test_helpers::{
        create_explain_ticket_statement_data, create_test_flight_handler,
        verify_explain_stream_results,
    };
    use std::collections::HashMap;

    // //////////////////////////////////////////////////////////////
    // Test helper functions
    // //////////////////////////////////////////////////////////////

    /// Create test worker addresses
    fn create_test_addrs() -> Addrs {
        let mut addrs = HashMap::new();
        let mut stage_addrs = HashMap::new();
        stage_addrs.insert(
            1u64,
            vec![
                ("worker1".to_string(), "localhost:8001".to_string()),
                ("worker2".to_string(), "localhost:8002".to_string()),
            ],
        );
        addrs.insert(1u64, stage_addrs);
        addrs
    }

    // //////////////////////////////////////////////////////////////
    // Unit tests
    // //////////////////////////////////////////////////////////////

    #[test]
    fn test_validate_single_stage_addrs_success() {
        let handler = create_test_flight_handler();
        let addrs = create_test_addrs();

        // Should succeed with valid single stage
        let result = handler.validate_single_stage_addrs(&addrs, 1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_single_stage_addrs_multiple_stages() {
        let handler = create_test_flight_handler();

        // Create addresses with multiple stages
        let mut addrs = HashMap::new();
        let mut stage1_addrs = HashMap::new();
        stage1_addrs.insert(
            1u64,
            vec![("worker1".to_string(), "localhost:8001".to_string())],
        );
        let mut stage2_addrs = HashMap::new();
        stage2_addrs.insert(
            2u64,
            vec![("worker2".to_string(), "localhost:8002".to_string())],
        );
        addrs.insert(1u64, stage1_addrs);
        addrs.insert(2u64, stage2_addrs);

        // Should fail with multiple stages
        let result = handler.validate_single_stage_addrs(&addrs, 1);
        assert!(result.is_err());

        if let Err(status) = result {
            assert!(status.message().contains("Expected exactly one stage"));
        }
    }

    #[test]
    fn test_validate_single_stage_addrs_wrong_stage_id() {
        let handler = create_test_flight_handler();
        let addrs = create_test_addrs(); // Contains stage_id 1

        // Should fail when looking for non-existent stage
        let result = handler.validate_single_stage_addrs(&addrs, 999);
        assert!(result.is_err());

        if let Err(status) = result {
            assert!(status
                .message()
                .contains("No addresses found for stage_id 999"));
        }
    }

    #[test]
    fn test_validate_single_stage_addrs_empty() {
        let handler = create_test_flight_handler();
        let addrs: Addrs = HashMap::new();

        // Should fail with empty addresses
        let result = handler.validate_single_stage_addrs(&addrs, 1);
        assert!(result.is_err());

        if let Err(status) = result {
            assert!(status.message().contains("Expected exactly one stage"));
        }
    }

    // //////////////////////////////////////////////////////////////
    // Handler core function tests
    // //////////////////////////////////////////////////////////////

    #[tokio::test]
    async fn test_handle_explain_request() {
        let handler = create_test_flight_handler();
        let query = "EXPLAIN SELECT 1 as test_col, 'hello' as text_col";

        let result = handler.handle_explain_request(query).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let flight_info = response.into_inner();

        // Verify FlightInfo structure
        assert!(!flight_info.schema.is_empty());
        assert_eq!(flight_info.endpoint.len(), 1);
        assert!(flight_info.endpoint[0].ticket.is_some());

        // Verify that ticket has content (encoded TicketStatementData)
        let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap();
        assert!(!ticket.ticket.is_empty());

        println!(
            "✓ FlightInfo created successfully with {} schema bytes and ticket with {} bytes",
            flight_info.schema.len(),
            ticket.ticket.len()
        );
    }

    #[tokio::test]
    async fn test_handle_explain_request_invalid_query() {
        let handler = create_test_flight_handler();

        // Test with EXPLAIN ANALYZE (should fail)
        let query = "EXPLAIN ANALYZE SELECT 1";
        let result = handler.handle_explain_request(query).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Internal);
        assert!(error.message().contains("Could not prepare EXPLAIN query"));
    }

    #[tokio::test]
    async fn test_handle_explain_statement_execution() {
        let handler = create_test_flight_handler();

        // First prepare an EXPLAIN query to get the ticket data structure
        let query = "EXPLAIN SELECT 1 as test_col";
        let plans = handler.planner.prepare_explain(query).await.unwrap();

        // Create the TicketStatementData that would be sent to do_get_statement
        let tsd = create_explain_ticket_statement_data(plans);

        // Test the execution
        let result = handler
            .handle_explain_statement_execution(tsd, "test_remote")
            .await;
        assert!(result.is_ok());

        let response = result.unwrap();
        let stream = response.into_inner();

        // Use shared verification function
        verify_explain_stream_results(stream).await;
    }

    #[tokio::test]
    async fn test_handle_explain_statement_execution_missing_explain_data() {
        let handler = create_test_flight_handler();

        // Create TicketStatementData without explain_data (should fail)
        let tsd = TicketStatementData {
            query_id: "test_query".to_string(),
            stage_id: 0,
            stage_addrs: None,
            schema: None,
            explain_data: None,
        };

        let result = handler
            .handle_explain_statement_execution(tsd, "test_remote")
            .await;
        assert!(result.is_err());

        if let Err(error) = result {
            assert_eq!(error.code(), tonic::Code::Internal);
            assert!(error
                .message()
                .contains("No explain_data in TicketStatementData"));
        }
    }
}
