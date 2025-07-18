use anyhow::Result;
use datafusion::common::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_proto::physical_plan::{AsExecutionPlan, DefaultPhysicalExtensionCodec};
use datafusion_proto::protobuf::PhysicalPlanNode;
use std::sync::Arc;

/// Helper function to create physical plan and its version after serialization and deserialization
async fn physical_plan_before_after_serde(
    sql: &str,
    ctx: &SessionContext,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<dyn ExecutionPlan>), DataFusionError> {
    // Build physical plan from sql
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;

    let codec = DefaultPhysicalExtensionCodec {};

    // Serialize the physical plan
    let proto: PhysicalPlanNode =
        PhysicalPlanNode::try_from_physical_plan(plan.clone(), &codec).expect("to proto");

    // Deserialize the physical plan
    let runtime = ctx.runtime_env();
    let de_plan: Arc<dyn ExecutionPlan> = proto
        .try_into_physical_plan(ctx, runtime.as_ref(), &codec)
        .expect("from proto");

    Ok((plan, de_plan))
}

/// Perform a serde (serialize and deserialize) roundtrip for the specified sql's physical plan, and assert that
async fn assert_serde_roundtrip(sql: &str, ctx: &SessionContext) -> Result<(), DataFusionError> {
    let (plan, de_plan) = physical_plan_before_after_serde(sql, ctx).await?;

    // Assert that the physical plan is identical after serialization and deserialization
    assert_eq!(format!("{plan:?}"), format!("{de_plan:?}"));

    Ok(())
}

/// Helper function to create a SessionContext with TPC-H tables registered from parquet files
async fn tpch_context() -> Result<SessionContext, DataFusionError> {
    let ctx = SessionContext::new();

    // TPC-H table definitions with their corresponding parquet file paths
    let tables = [
        ("customer", "tpch/data/tpch_customer_small.parquet"),
        ("lineitem", "tpch/data/tpch_lineitem_small.parquet"),
        ("nation", "tpch/data/tpch_nation_small.parquet"),
        ("orders", "tpch/data/tpch_orders_small.parquet"),
        ("part", "tpch/data/tpch_part_small.parquet"),
        ("partsupp", "tpch/data/tpch_partsupp_small.parquet"),
        ("region", "tpch/data/tpch_region_small.parquet"),
        ("supplier", "tpch/data/tpch_supplier_small.parquet"),
    ];

    // Register all TPC-H tables
    for (table_name, file_path) in &tables {
        ctx.register_parquet(*table_name, *file_path, ParquetReadOptions::default())
            .await?;
    }

    // Create the revenue0 view required for query 15
    let revenue0_sql = "CREATE VIEW revenue0 (supplier_no, total_revenue) AS SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) FROM lineitem WHERE l_shipdate >= date '1996-08-01' AND l_shipdate < date '1996-08-01' + interval '3' month GROUP BY l_suppkey";
    ctx.sql(revenue0_sql).await?.collect().await?;

    Ok(ctx)
}

/// Helper function to get TPC-H query SQL from the repository
fn get_tpch_query_sql(query: usize) -> Result<Vec<String>, DataFusionError> {
    use std::fs;

    if !(1..=22).contains(&query) {
        return Err(DataFusionError::External(
            format!("Invalid TPC-H query number: {query}").into(),
        ));
    }

    let filename = format!("tpch/queries/q{query}.sql");
    let contents = fs::read_to_string(&filename).map_err(|e| {
        DataFusionError::External(format!("Failed to read query file {filename}: {e}").into())
    })?;

    Ok(contents
        .split(';')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect())
}

/// Test that we are able to build the physical plan and its version after serialization and deserialization
#[tokio::test]
async fn test_serialize_deserialize_tpch_queries() -> Result<()> {
    // Create context with TPC-H tables from parquet files
    let ctx = tpch_context().await?;

    // repeat to run all 22 queries
    for query in 1..=22 {
        // run all statements in the query
        let sql = get_tpch_query_sql(query)?;
        for stmt in sql {
            // Skip empty statements and comment-only statements
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            // Skip statements that are only comments
            if trimmed
                .lines()
                .all(|line| line.trim().is_empty() || line.trim().starts_with("--"))
            {
                continue;
            }

            // Ensure we are able to build the physical plan and its version after serialization and deserialization
            physical_plan_before_after_serde(&stmt, &ctx).await?;
        }
    }

    Ok(())
}

// Test compare the result of the physical plan before and after serialization and deserialization
#[tokio::test]
async fn test_round_trip_tpch_queries() -> Result<(), DataFusionError> {
    // Create context with TPC-H tables
    let ctx = tpch_context().await?;

    // Verify q3, q5, q10, q12 pass the round trip
    // todo: after bug https://github.com/apache/datafusion/issues/16772 is fixed, test with all 22 queries
    for query in [3, 5, 10, 12] {
        let sql = get_tpch_query_sql(query)?;
        for stmt in sql {
            assert_serde_roundtrip(&stmt, &ctx).await?;
        }
    }

    Ok(())
}
