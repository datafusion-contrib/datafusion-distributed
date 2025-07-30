//! Mock data generator for basic integration tests
//!
//! This module provides utilities to generate simple test data for basic queries,
//! including joins between customers and orders tables.

use std::fs;
use std::path::Path;

/// Generates mock data files for basic tests
pub fn generate_mock_data() -> Result<(), Box<dyn std::error::Error>> {
    let test_data_dir = "testdata/mock";

    // Create directory if it doesn't exist
    fs::create_dir_all(test_data_dir)?;

    generate_customers_csv(&format!("{}/customers.csv", test_data_dir))?;
    generate_orders_csv(&format!("{}/orders.csv", test_data_dir))?;

    println!("✅ Mock data generated in {}", test_data_dir);
    Ok(())
}

/// Generate customers CSV file
fn generate_customers_csv(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let customers_data = vec![
        "customer_id,name,city,country",
        "1,Alice Johnson,New York,USA",
        "2,Bob Smith,London,UK",
        "3,Carol Davis,Paris,France",
        "4,David Wilson,Tokyo,Japan",
        "5,Eve Brown,Sydney,Australia",
    ];

    fs::write(path, customers_data.join("\n"))?;
    println!(
        "  📝 Generated customers.csv with {} rows",
        customers_data.len() - 1
    );
    Ok(())
}

/// Generate orders CSV file
fn generate_orders_csv(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let orders_data = vec![
        "order_id,customer_id,product,amount,order_date",
        "101,1,Laptop,1200.00,2024-01-15",
        "102,1,Mouse,25.50,2024-01-16",
        "103,2,Keyboard,75.00,2024-01-17",
        "104,3,Monitor,350.00,2024-01-18",
        "105,2,Headphones,120.00,2024-01-19",
        "106,4,Tablet,600.00,2024-01-20",
        "107,5,Phone,800.00,2024-01-21",
        "108,1,Cable,15.99,2024-01-22",
        "109,3,Speaker,200.00,2024-01-23",
        "110,4,Charger,45.00,2024-01-24",
    ];

    fs::write(path, orders_data.join("\n"))?;
    println!(
        "  📝 Generated orders.csv with {} rows",
        orders_data.len() - 1
    );
    Ok(())
}

/// Clean up mock data files
#[allow(dead_code)]
pub fn cleanup_mock_data() -> Result<(), Box<dyn std::error::Error>> {
    let test_data_dir = "testdata/mock";

    if Path::new(test_data_dir).exists() {
        fs::remove_dir_all(test_data_dir)?;
        println!("🧹 Cleaned up mock data directory");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_generate_mock_data() {
        // Print current directory for debugging
        println!(
            "Current working directory: {:?}",
            env::current_dir().unwrap()
        );

        // Test data generation
        generate_mock_data().expect("Failed to generate mock data");

        // Build absolute paths for verification
        let current_dir = env::current_dir().unwrap();
        let customers_path = current_dir.join("testdata/mock/customers.csv");
        let orders_path = current_dir.join("testdata/mock/orders.csv");

        println!("Looking for customers.csv at: {:?}", customers_path);
        println!("Looking for orders.csv at: {:?}", orders_path);

        // Verify files exist
        assert!(
            customers_path.exists(),
            "customers.csv not found at {:?}",
            customers_path
        );
        assert!(
            orders_path.exists(),
            "orders.csv not found at {:?}",
            orders_path
        );

        // Verify file contents
        let customers_content = fs::read_to_string(&customers_path).unwrap();
        assert!(customers_content.contains("Alice Johnson"));
        assert!(customers_content.contains("customer_id,name,city,country"));

        let orders_content = fs::read_to_string(&orders_path).unwrap();
        assert!(orders_content.contains("Laptop"));
        assert!(orders_content.contains("order_id,customer_id,product,amount,order_date"));

        // Note: We intentionally don't cleanup here to avoid race conditions
        // with other tests that need the mock data. The testdata/ directory
        // can be cleaned up manually or by CI systems between test runs.
        println!("✅ Mock data test completed (cleanup skipped to avoid race conditions)");
    }
}
