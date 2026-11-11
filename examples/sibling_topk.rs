use datafusion::common::Result;
use datafusion::physical_plan::collect;
use datafusion::prelude::ParquetReadOptions;
use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
use datafusion_distributed::{DefaultSessionBuilder, display_plan_ascii};

/// Run a Top-K query with two simulated workers and opt-in sibling bound sharing.
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = start_configured_in_memory_context(2, DefaultSessionBuilder, |worker| worker).await;
    ctx.sql("SET distributed.enable_sibling_topk_filter_pushdown = true")
        .await?;
    ctx.register_parquet("weather", "testdata/weather", ParquetReadOptions::default())
        .await?;

    let plan = ctx
        .sql(r#"SELECT "MinTemp" FROM weather ORDER BY "MinTemp" DESC NULLS LAST LIMIT 10"#)
        .await?
        .create_physical_plan()
        .await?;
    println!("{}", display_plan_ascii(plan.as_ref(), false));
    let batches = collect(plan, ctx.task_ctx()).await?;
    datafusion::arrow::util::pretty::print_batches(&batches)?;
    Ok(())
}
