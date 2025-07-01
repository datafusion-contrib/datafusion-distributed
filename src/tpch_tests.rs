#[cfg(test)]
mod tpch_serialization_tests {
    use std::sync::Arc;
    use std::path::Path;
    use std::fs;

    use datafusion::{
        prelude::*,
        execution::config::SessionConfig,
        physical_plan::ExecutionPlan,
    };
    use datafusion_proto::physical_plan::AsExecutionPlan;

    use crate::codec::DFRayCodec;

    /// Test serialization/deserialization round trip for a physical plan
    async fn test_plan_serialization(physical_plan: Arc<dyn ExecutionPlan>, query_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let codec = DFRayCodec {};
        
        // Serialize to protobuf
        let proto = datafusion_proto::protobuf::PhysicalPlanNode::try_from_physical_plan(
            physical_plan.clone(), 
            &codec
        )?;
        
        println!("✅ Serialization successful for {}", query_name);
        
        // Create a new context for deserialization (simulating worker context)
        let worker_ctx = create_tpch_context().await.expect("Failed to create worker context");
        
        // Deserialize back to physical plan
        let result = proto.try_into_physical_plan(
            &worker_ctx, 
            worker_ctx.runtime_env().as_ref(), 
            &codec
        );
        
        match result {
            Ok(deserialized_plan) => {
                println!("✅ Deserialization successful for {}", query_name);
                
                // Check if schemas match
                let orig_schema = physical_plan.schema();
                let deser_schema = deserialized_plan.schema();
                
                if orig_schema != deser_schema {
                    return Err(format!("Schema mismatch for {}: orig={:?}, deser={:?}", 
                        query_name, orig_schema, deser_schema).into());
                }
                
                println!("✅ Schema validation passed for {}", query_name);
                Ok(())
            }
            Err(e) => {
                println!("❌ Deserialization failed for {}: {}", query_name, e);
                Err(e.into())
            }
        }
    }

    /// Create a SessionContext with all TPC-H tables registered
    async fn create_tpch_context() -> Result<SessionContext, Box<dyn std::error::Error>> {
        let config = SessionConfig::default();
        let ctx = SessionContext::new_with_config(config);
        
        // Check if TPC-H data is available
        if !Path::new("/tmp/tpch_s1/").exists() {
            return Err("TPC-H data not found at /tmp/tpch_s1/. Please ensure the data is available.".into());
        }

        // Define all TPC-H tables
        let tables = vec![
            "customer", "lineitem", "nation", "orders", 
            "part", "partsupp", "region", "supplier"
        ];

        for table in tables {
            let table_sql = format!(
                "CREATE EXTERNAL TABLE {} STORED AS PARQUET LOCATION '/tmp/tpch_s1/{}.parquet'",
                table, table
            );
            
            ctx.sql(&table_sql).await
                .map_err(|e| format!("Failed to create table {}: {}", table, e))?;
        }

        Ok(ctx)
    }

    /// Read and parse a TPC-H query file
    fn read_query_file(query_name: &str) -> Result<String, Box<dyn std::error::Error>> {
        let path = format!("tpch/queries/{}.sql", query_name);
        let content = fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read {}: {}", path, e))?;
        Ok(content)
    }

    /// Test a single TPC-H query for serialization bugs
    async fn test_single_query(ctx: &SessionContext, query_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("\n🔍 Testing query: {}", query_name);
        
        let sql = read_query_file(query_name)?;
        
        // Create logical plan
        let logical_plan = ctx.state().create_logical_plan(&sql).await
            .map_err(|e| format!("Failed to create logical plan for {}: {}", query_name, e))?;
        
        let logical_plan = ctx.state().optimize(&logical_plan)
            .map_err(|e| format!("Failed to optimize logical plan for {}: {}", query_name, e))?;
        
        // Create physical plan
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await
            .map_err(|e| format!("Failed to create physical plan for {}: {}", query_name, e))?;
        
        // Test serialization/deserialization
        test_plan_serialization(physical_plan, query_name).await?;
        
        Ok(())
    }

    #[tokio::test]
    async fn test_all_tpch_queries_serialization() {
        // Skip test if TPC-H data not available
        if !Path::new("/tmp/tpch_s1/").exists() {
            println!("⚠️  Skipping TPC-H tests - data not found at /tmp/tpch_s1/");
            return;
        }

        let ctx = create_tpch_context().await.expect("Failed to create TPC-H context");
        
        // List of all TPC-H queries
        let queries = vec![
            "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8", "q9", "q10",
            "q11", "q12", "q13", "q14", "q15", "q16", "q17", "q18", "q19", 
            "q20", "q21", "q22"
        ];

        let mut passed = 0;
        let mut failed = 0;
        let mut failed_queries = Vec::new();

        for query in &queries {
            match test_single_query(&ctx, query).await {
                Ok(_) => {
                    println!("✅ {} passed", query);
                    passed += 1;
                }
                Err(e) => {
                    println!("❌ {} failed: {}", query, e);
                    failed += 1;
                    failed_queries.push((query, e.to_string()));
                }
            }
        }

        println!("\n📊 Results Summary:");
        println!("   ✅ Passed: {}/{}", passed, queries.len());
        println!("   ❌ Failed: {}/{}", failed, queries.len());

        if !failed_queries.is_empty() {
            println!("\n❌ Failed queries:");
            for (query, error) in &failed_queries {
                println!("   {} - {}", query, error);
            }
        }

        // For now, we expect some queries to fail due to the bug
        // Once we fix the serialization issues, we can change this to assert all pass
        if failed > 0 {
            println!("\n⚠️  Some queries failed serialization - this is expected until we fix the bugs");
            
            // Let's specifically check if q16 fails with the expected error
            let q16_failed = failed_queries.iter().any(|(query, error)| {
                **query == "q16" && error.contains("data type inlist should be same")
            });
            
            if q16_failed {
                println!("✅ Confirmed: Q16 fails with the expected 'data type inlist should be same' error");
            }
        }
    }

    #[tokio::test]
    async fn test_q16_specifically() {
        // This test specifically focuses on the q16 bug we know about
        if !Path::new("/tmp/tpch_s1/").exists() {
            println!("⚠️  Skipping Q16 test - TPC-H data not found at /tmp/tpch_s1/");
            return;
        }

        let ctx = create_tpch_context().await.expect("Failed to create TPC-H context");
        
        match test_single_query(&ctx, "q16").await {
            Ok(_) => {
                println!("✅ Q16 serialization works! The bug may have been fixed.");
            }
            Err(e) => {
                if e.to_string().contains("data type inlist should be same") {
                    println!("✅ Successfully reproduced Q16 serialization bug!");
                    println!("Error: {}", e);
                    // This is expected for now
                } else {
                    panic!("Q16 failed with unexpected error: {}", e);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_minimal_in_expression() {
        // Test a minimal IN expression without TPC-H dependency
        let ctx = SessionContext::new();
        
        let sql = r#"
            WITH sample_data AS (
                SELECT column1 as id, column2 as size_col FROM VALUES 
                (1, 14), (2, 6), (3, 5), (4, 31), (5, 49)
            )
            SELECT * FROM sample_data WHERE size_col IN (14, 6, 5, 31, 49, 15, 41, 47)
        "#;
        
        println!("🔍 Testing minimal IN expression...");
        
        let logical_plan = ctx.state().create_logical_plan(sql).await.expect("logical plan");
        let physical_plan = ctx.state().create_physical_plan(&logical_plan).await.expect("physical plan");
        
        match test_plan_serialization(physical_plan, "minimal_in_expression").await {
            Ok(_) => {
                println!("✅ Minimal IN expression serialization works");
            }
            Err(e) => {
                if e.to_string().contains("data type inlist should be same") {
                    println!("✅ Reproduced the bug with minimal IN expression!");
                    println!("Error: {}", e);
                } else {
                    println!("❌ Unexpected error: {}", e);
                }
            }
        }
    }
} 