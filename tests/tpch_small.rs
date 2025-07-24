//! TPC-H Small Dataset Tests
//!
//! This module contains tests for TPC-H queries using the small dataset.
//! Each test executes one TPC-H query and validates the results using snapshots.

use datafusion::arrow::util::pretty::pretty_format_batches;
use insta::assert_snapshot;
use std::fs;
use tokio::sync::OnceCell;

mod cluster_setup;
use cluster_setup::TestCluster;

/// Global cluster instance shared across all tests
static CLUSTER: OnceCell<TestCluster> = OnceCell::const_new();

/// Get or initialize the shared TPC-H cluster
async fn get_cluster() -> &'static TestCluster {
    CLUSTER
        .get_or_init(|| async {
            println!("🚀 Initializing shared TPC-H small cluster...");
            let cluster = TestCluster::start_tpch_small_cluster()
                .await
                .expect("Failed to start TPC-H small cluster");
            println!("✅ Shared TPC-H cluster ready for all tests!");
            cluster
        })
        .await
}

/// Read SQL query from file
fn read_query(query_num: u8) -> String {
    let path = format!("tpch/queries/q{}.sql", query_num);
    fs::read_to_string(&path).unwrap_or_else(|_| panic!("Failed to read query file: {}", path))
}

/// Execute a TPC-H query and return formatted results (now supports concurrent execution!)
async fn execute_tpch_query(query_num: u8) -> String {
    println!(
        "🔍 Executing TPC-H Q{:02} concurrently on shared cluster...",
        query_num
    );

    // Get the shared cluster instance (no mutex - concurrent access allowed!)
    let cluster = get_cluster().await;

    let sql = read_query(query_num);
    let batches = cluster
        .execute_sql(&sql)
        .await
        .expect("Failed to execute TPC-H query");

    println!("✅ TPC-H Q{:02} completed successfully", query_num);

    pretty_format_batches(&batches).unwrap().to_string()
}

#[tokio::test]
async fn test_tpch_q01() {
    let result = execute_tpch_query(1).await;
    assert_snapshot!(result, @r"
    +--------------+--------------+---------+----------------+----------------+---------------+-----------+--------------+----------+-------------+
    | l_returnflag | l_linestatus | sum_qty | sum_base_price | sum_disc_price | sum_charge    | avg_qty   | avg_price    | avg_disc | count_order |
    +--------------+--------------+---------+----------------+----------------+---------------+-----------+--------------+----------+-------------+
    | A            | F            | 142.00  | 206668.09      | 190541.1008    | 197576.405324 | 28.400000 | 41333.618000 | 0.066000 | 5           |
    | N            | O            | 234.00  | 282449.39      | 264970.4878    | 277302.112308 | 23.400000 | 28244.939000 | 0.067000 | 10          |
    | R            | F            | 163.00  | 208243.51      | 194976.6738    | 199678.732608 | 32.600000 | 41648.702000 | 0.058000 | 5           |
    +--------------+--------------+---------+----------------+----------------+---------------+-----------+--------------+----------+-------------+
    ");
}

#[tokio::test]
async fn test_tpch_q02() {
    let result = execute_tpch_query(2).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q03() {
    let result = execute_tpch_query(3).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q04() {
    let result = execute_tpch_query(4).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q05() {
    let result = execute_tpch_query(5).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q06() {
    let result = execute_tpch_query(6).await;
    assert_snapshot!(result, @r"
    +---------+
    | revenue |
    +---------+
    |         |
    +---------+
    ");
}

#[tokio::test]
async fn test_tpch_q07() {
    let result = execute_tpch_query(7).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q08() {
    let result = execute_tpch_query(8).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q09() {
    let result = execute_tpch_query(9).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q10() {
    let result = execute_tpch_query(10).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q11() {
    let result = execute_tpch_query(11).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q12() {
    let result = execute_tpch_query(12).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q13() {
    let result = execute_tpch_query(13).await;
    assert_snapshot!(result, @r"
    +---------+----------+
    | c_count | custdist |
    +---------+----------+
    | 0       | 20       |
    +---------+----------+
    ");
}

#[tokio::test]
async fn test_tpch_q14() {
    let result = execute_tpch_query(14).await;
    assert_snapshot!(result, @r"
    +---------------+
    | promo_revenue |
    +---------------+
    |               |
    +---------------+
    ");
}

#[tokio::test]
async fn test_tpch_q15() {
    let result = execute_tpch_query(15).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q16() {
    let result = execute_tpch_query(16).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q17() {
    let result = execute_tpch_query(17).await;
    assert_snapshot!(result, @r"
    +------------+
    | avg_yearly |
    +------------+
    |            |
    +------------+
    ");
}

#[tokio::test]
async fn test_tpch_q18() {
    let result = execute_tpch_query(18).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q19() {
    let result = execute_tpch_query(19).await;
    assert_snapshot!(result, @r"
    +---------+
    | revenue |
    +---------+
    |         |
    +---------+
    ");
}

#[tokio::test]
async fn test_tpch_q20() {
    let result = execute_tpch_query(20).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q21() {
    let result = execute_tpch_query(21).await;
    assert_snapshot!(result, @r"
    ++
    ++
    ");
}

#[tokio::test]
async fn test_tpch_q22() {
    let result = execute_tpch_query(22).await;
    assert_snapshot!(result, @r"
    +-----------+---------+------------+
    | cntrycode | numcust | totacctbal |
    +-----------+---------+------------+
    | 16        | 1       | 5494.43    |
    | 30        | 1       | 7638.57    |
    +-----------+---------+------------+
    ");
}
