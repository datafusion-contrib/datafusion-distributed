use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::displayable;
use datafusion::prelude::*;
use datafusion_distributed::physical_optimizer::DistributedPhysicalOptimizerRule;
use datafusion_distributed::stage::ExecutionStage;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // TODO: uncomment for distributed example
    // let config = SessionConfig::new().with_target_partitions(10);
    //
    // let rule = DistributedPhysicalOptimizerRule::default().with_maximum_partitions_per_task(4);
    //
    // let state = SessionStateBuilder::new()
    //     .with_default_features()
    //     .with_config(config)
    //     .with_physical_optimizer_rule(Arc::new(rule))
    //     .build();

    let ctx = SessionContext::new();

    ctx.register_parquet(
        "customer",
        "testdata/customer.parquet",
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "orders",
        "testdata/orders.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    let sql = "
        SELECT c.c_name, o.o_orderkey, o.o_totalprice 
        FROM customer c 
        JOIN orders o ON c.c_custkey = o.o_custkey 
        WHERE o.o_totalprice > 100000
        ORDER BY o.o_totalprice DESC
        LIMIT 10
    ";

    println!("Executing distributed join query...\n");

    let logical_plan = ctx.sql(sql).await?.into_optimized_plan()?;
    println!("Logical plan:\n{}", logical_plan.display_indent());

    let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;
    println!(
        "\nPhysical plan:\n{}",
        displayable(physical_plan.as_ref()).indent(true)
    );

    let stage = ExecutionStage::new(1, physical_plan, vec![]);
    println!("\nExecution stage created: {:?}", stage.name());

    let df = ctx.sql(sql).await?;
    println!("\nExecuting query and displaying results:");
    df.show().await?;

    Ok(())
}
