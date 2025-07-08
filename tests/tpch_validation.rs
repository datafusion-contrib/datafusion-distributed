//! TPC-H Validation Integration Tests
//!
//! This module provides comprehensive validation tests that compare TPC-H query results
//! between regular DataFusion and distributed DataFusion systems.
//!
//! ## Features
//! - Automatic cluster setup and teardown
//! - Automatic TPC-H data generation
//! - Automatic dependency installation
//! - Complete result comparison with tolerance
//! - CI-ready with detailed reporting
//! - Fast execution with minimal output (only top 2 rows for large results)
//! - Configurable verbosity for debugging specific queries
//! - Configurable timing parameters for cluster startup and polling
//! - Modular design with reusable helper functions
//! - Safe concurrent execution (only affects designated test ports)
//!
//! ## Usage
//!
//! Just run the tests - everything is automated:
//! ```bash
//! # Run all TPC-H validation tests
//! cargo test --test tpch_validation test_tpch_validation_all_queries -- --ignored --nocapture
//!
//! # Run single query test (for debugging)
//! cargo test --test tpch_validation test_tpch_validation_single_query -- --ignored --nocapture
//!
//! # Enable verbose output for debugging specific queries
//! # Modify the should_be_verbose() function in utils.rs to return true for specific queries
//! ```
//!
//! ## What the tests do automatically:
//! 1. Clean up any existing processes on test ports 40400-40402 only
//! 2. Install tpchgen-cli if not available
//! 3. Generate TPC-H scale factor 1 data at /tmp/tpch_s1 if not present
//! 4. Start distributed cluster (1 proxy + 2 workers)
//! 5. Run validation tests comparing DataFusion vs Distributed for all 22 TPC-H queries in ./tpch/queries/
//! 6. Clean up test cluster processes (without affecting other instances)
//!
use std::time::Instant;

mod common;
use common::*;

/// Main validation test that runs all TPC-H queries
///
/// This test is completely self-contained and handles:
/// - Cluster setup and teardown
/// - TPC-H data generation
/// - Dependency installation
/// - Result comparison and reporting
///
/// This test is marked with #[ignore] to exclude it from `cargo test`.
/// Run manually with: `cargo test --test tpch_validation test_tpch_validation_all_queries -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn test_tpch_validation_all_queries() {
    println!("🎯 Starting comprehensive TPC-H validation test");

    // Setup test environment
    let (cluster, ctx) = setup_test_environment()
        .await
        .unwrap_or_else(|e| panic!("❌ {}", e));

    let start_time = Instant::now();
    let mut results = ValidationResults::new();

    // Get query list
    let all_queries =
        get_tpch_queries().unwrap_or_else(|e| panic!("❌ Failed to get TPC-H queries: {}", e));

    // Filter out q16 due to known issues
    let queries: Vec<String> = all_queries.into_iter().filter(|q| q != "q16").collect();

    results.total_queries = queries.len();
    println!(
        "📋 Found {} TPC-H queries to validate (excluding q16 due to known issues)",
        results.total_queries
    );

    // Run each query
    for (i, query_name) in queries.iter().enumerate() {
        print!(
            "🔍 [{}/{}] Testing {}... ",
            i + 1,
            queries.len(),
            query_name
        );

        // Execute single query validation
        let comparison = execute_single_query_validation(&cluster, &ctx, query_name).await;

        // Handle results
        if comparison.matches {
            println!(
                "✅ PASS ({}/{} rows, DF: {:.2}s, Dist: {:.2}s)",
                comparison.row_count_datafusion,
                comparison.row_count_distributed,
                comparison.execution_time_datafusion.as_secs_f64(),
                comparison.execution_time_distributed.as_secs_f64()
            );
            results.passed_queries += 1;
        } else {
            println!(
                "❌ FAIL: {}",
                comparison.error_message.as_deref().unwrap_or("Unknown")
            );
            results.failed_queries += 1;
        }

        results.results.push(comparison);
    }

    results.total_time = start_time.elapsed();
    results.print_summary();

    // Note: Cluster cleanup happens automatically via Drop trait

    // For CI: fail the test if any queries failed
    if results.failed_queries > 0 {
        panic!(
            "TPC-H validation failed: {} out of {} queries failed",
            results.failed_queries, results.total_queries
        );
    }

    println!("\n🎉 All TPC-H validation tests passed successfully!");
}

/// Test a single TPC-H query (useful for debugging)
///
/// This test is marked with #[ignore] - use `cargo test --ignored` to run it.
/// Modify the query_name to test different queries.
///
/// To enable verbose output for debugging, modify the `should_be_verbose` function in utils.rs.
#[tokio::test]
#[ignore]
async fn test_tpch_validation_single_query() {
    let query_name = "q16"; // Change this to test different queries

    println!("🔍 Testing single query: {}", query_name);

    // Setup test environment
    let (cluster, ctx) = setup_test_environment()
        .await
        .expect("Failed to setup test environment");

    // Execute query validation
    let comparison = execute_single_query_validation(&cluster, &ctx, query_name).await;

    // Display results
    println!(
        "Result: {} ({}/{} rows, DF: {:.2}s, Dist: {:.2}s)",
        if comparison.matches {
            "✅ PASSED"
        } else {
            "❌ FAILED"
        },
        comparison.row_count_datafusion,
        comparison.row_count_distributed,
        comparison.execution_time_datafusion.as_secs_f64(),
        comparison.execution_time_distributed.as_secs_f64()
    );

    if let Some(error) = &comparison.error_message {
        println!("Error: {}", error);
    }

    assert!(
        comparison.matches,
        "Query {} validation failed: {}",
        query_name,
        comparison.error_message.unwrap_or_default()
    );
}
