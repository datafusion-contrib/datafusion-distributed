#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::util::pretty;
    use datafusion::{
        physical_plan::{collect, displayable},
        prelude::{ParquetReadOptions, SessionContext},
    };

    #[tokio::test]
    async fn test_join() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        ctx.register_parquet(
            "dim",
            "testdata/join/parquet/dim",
            ParquetReadOptions::new(),
        )
        .await?;
        ctx.register_parquet(
            "fact",
            "testdata/join/parquet/fact",
            ParquetReadOptions::new(),
        )
        .await?;

        let sql = "SELECT * FROM dim JOIN fact ON dim.key = fact.key";
        let df = ctx.sql(sql).await?;

        let (state, logical_plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        println!("\n——————— PHYSICAL PLAN ———————\n");
        println!("{}", displayable(physical_plan.as_ref()).indent(true));

        let result = collect(physical_plan, state.task_ctx()).await?;
        pretty::print_batches(&result)?;
        Ok(())
    }
}

