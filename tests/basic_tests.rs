//! Basic integration tests using mock data
//!
//! This module tests basic distributed query functionality using simple,
//! controlled test data to verify join operations and basic SQL features.

use datafusion::arrow::util::pretty::pretty_format_batches;
use insta::assert_snapshot;
use std::sync::Once;
use tokio::sync::OnceCell;

mod cluster_setup;
mod mock_data;

use cluster_setup::{ClusterConfig, TestCluster};
use mock_data::generate_mock_data;

/// Ensure mock data is generated only once across all tests
static MOCK_DATA_INIT: Once = Once::new();

/// Global cluster instance shared across basic tests
static BASIC_CLUSTER: OnceCell<TestCluster> = OnceCell::const_new();

/// Initialize mock data once for all tests
fn ensure_mock_data() {
    MOCK_DATA_INIT.call_once(|| {
        generate_mock_data().expect("Failed to generate mock data");
        println!("✅ Mock data initialized for all basic tests");
    });
}

/// Get or initialize the shared basic test cluster
async fn get_basic_cluster() -> &'static TestCluster {
    BASIC_CLUSTER
        .get_or_init(|| async {
            println!("🚀 Initializing shared basic test cluster...");

            // Ensure mock data exists
            ensure_mock_data();

            // Configure cluster with mock data tables
            let config = ClusterConfig::new()
                .with_base_port(33000) // Use different port range from TPC-H tests
                .with_csv_table("customers", "testdata/mock/customers.csv")
                .with_csv_table("orders", "testdata/mock/orders.csv");

            let cluster = TestCluster::start_with_config(config)
                .await
                .expect("Failed to start basic test cluster");

            println!("✅ Shared basic test cluster ready!");
            cluster
        })
        .await
}

/// Execute a SQL query and return formatted results (now supports concurrent execution!)
async fn execute_basic_query(sql: &str) -> String {
    println!("🔍 Executing query concurrently: {}", sql);

    // Get the shared cluster instance (no mutex - concurrent access allowed!)
    let cluster = get_basic_cluster().await;

    let batches = cluster
        .execute_sql(sql)
        .await
        .expect("Failed to execute query");

    println!("✅ Query completed successfully");

    pretty_format_batches(&batches).unwrap().to_string()
}

#[tokio::test]
async fn test_select_customers() {
    let result = execute_basic_query("SELECT * FROM customers ORDER BY customer_id").await;
    assert_snapshot!(result, @r"
    +-------------+---------------+----------+-----------+
    | customer_id | name          | city     | country   |
    +-------------+---------------+----------+-----------+
    | 1           | Alice Johnson | New York | USA       |
    | 2           | Bob Smith     | London   | UK        |
    | 3           | Carol Davis   | Paris    | France    |
    | 4           | David Wilson  | Tokyo    | Japan     |
    | 5           | Eve Brown     | Sydney   | Australia |
    +-------------+---------------+----------+-----------+
    ");
}

#[tokio::test]
async fn test_select_orders() {
    let result = execute_basic_query("SELECT * FROM orders ORDER BY order_id").await;
    assert_snapshot!(result, @r"
    +----------+-------------+------------+--------+------------+
    | order_id | customer_id | product    | amount | order_date |
    +----------+-------------+------------+--------+------------+
    | 101      | 1           | Laptop     | 1200.0 | 2024-01-15 |
    | 102      | 1           | Mouse      | 25.5   | 2024-01-16 |
    | 103      | 2           | Keyboard   | 75.0   | 2024-01-17 |
    | 104      | 3           | Monitor    | 350.0  | 2024-01-18 |
    | 105      | 2           | Headphones | 120.0  | 2024-01-19 |
    | 106      | 4           | Tablet     | 600.0  | 2024-01-20 |
    | 107      | 5           | Phone      | 800.0  | 2024-01-21 |
    | 108      | 1           | Cable      | 15.99  | 2024-01-22 |
    | 109      | 3           | Speaker    | 200.0  | 2024-01-23 |
    | 110      | 4           | Charger    | 45.0   | 2024-01-24 |
    +----------+-------------+------------+--------+------------+
    ");
}

#[tokio::test]
async fn test_customer_order_count() {
    let result = execute_basic_query(
        "SELECT c.name, COUNT(o.order_id) as order_count 
         FROM customers c 
         LEFT JOIN orders o ON c.customer_id = o.customer_id 
         GROUP BY c.customer_id, c.name 
         ORDER BY c.name",
    )
    .await;
    assert_snapshot!(result, @r"
    +---------------+-------------+
    | name          | order_count |
    +---------------+-------------+
    | Alice Johnson | 3           |
    | Bob Smith     | 2           |
    | Carol Davis   | 2           |
    | David Wilson  | 2           |
    | Eve Brown     | 1           |
    +---------------+-------------+
    ");
}

#[tokio::test]
async fn test_customer_total_spending() {
    let result = execute_basic_query(
        "SELECT c.name, c.city, COALESCE(SUM(o.amount), 0) as total_spent 
         FROM customers c 
         LEFT JOIN orders o ON c.customer_id = o.customer_id 
         GROUP BY c.customer_id, c.name, c.city 
         ORDER BY total_spent DESC",
    )
    .await;
    assert_snapshot!(result, @r"
    +---------------+----------+-------------+
    | name          | city     | total_spent |
    +---------------+----------+-------------+
    | Alice Johnson | New York | 1241.49     |
    | Eve Brown     | Sydney   | 800.0       |
    | David Wilson  | Tokyo    | 645.0       |
    | Carol Davis   | Paris    | 550.0       |
    | Bob Smith     | London   | 195.0       |
    +---------------+----------+-------------+
    ");
}

#[tokio::test]
async fn test_orders_by_country() {
    let result = execute_basic_query(
        "SELECT c.country, COUNT(o.order_id) as order_count, SUM(o.amount) as total_amount
         FROM customers c 
         INNER JOIN orders o ON c.customer_id = o.customer_id 
         GROUP BY c.country 
         ORDER BY total_amount DESC",
    )
    .await;
    assert_snapshot!(result, @r"
    +-----------+-------------+--------------+
    | country   | order_count | total_amount |
    +-----------+-------------+--------------+
    | USA       | 3           | 1241.49      |
    | Australia | 1           | 800.0        |
    | Japan     | 2           | 645.0        |
    | France    | 2           | 550.0        |
    | UK        | 2           | 195.0        |
    +-----------+-------------+--------------+
    ");
}

#[tokio::test]
async fn test_expensive_orders() {
    let result = execute_basic_query(
        "SELECT o.order_id, c.name, o.product, o.amount 
         FROM orders o 
         INNER JOIN customers c ON o.customer_id = c.customer_id 
         WHERE o.amount > 100 
         ORDER BY o.amount DESC",
    )
    .await;
    assert_snapshot!(result, @r"
    +----------+---------------+------------+--------+
    | order_id | name          | product    | amount |
    +----------+---------------+------------+--------+
    | 101      | Alice Johnson | Laptop     | 1200.0 |
    | 107      | Eve Brown     | Phone      | 800.0  |
    | 106      | David Wilson  | Tablet     | 600.0  |
    | 104      | Carol Davis   | Monitor    | 350.0  |
    | 109      | Carol Davis   | Speaker    | 200.0  |
    | 105      | Bob Smith     | Headphones | 120.0  |
    +----------+---------------+------------+--------+
    ");
}

#[tokio::test]
async fn test_product_sales_summary() {
    let result = execute_basic_query(
        "SELECT o.product, COUNT(*) as quantity_sold, SUM(o.amount) as total_revenue
         FROM orders o 
         GROUP BY o.product 
         ORDER BY total_revenue DESC",
    )
    .await;
    assert_snapshot!(result, @r"
    +------------+---------------+---------------+
    | product    | quantity_sold | total_revenue |
    +------------+---------------+---------------+
    | Laptop     | 1             | 1200.0        |
    | Phone      | 1             | 800.0         |
    | Tablet     | 1             | 600.0         |
    | Monitor    | 1             | 350.0         |
    | Speaker    | 1             | 200.0         |
    | Headphones | 1             | 120.0         |
    | Keyboard   | 1             | 75.0          |
    | Charger    | 1             | 45.0          |
    | Mouse      | 1             | 25.5          |
    | Cable      | 1             | 15.99         |
    +------------+---------------+---------------+
    ");
}
