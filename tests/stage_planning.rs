mod common;
mod tpch;

#[cfg(test)]
mod tests {
    use crate::tpch::tpch_query;
    use crate::{assert_snapshot, tpch};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::{display_stage, display_stage_tree, ExecutionStage, StagePlanner};
    use futures::TryStreamExt;
    use std::error::Error;

    #[tokio::test]
    async fn stage_planning() -> Result<(), Box<dyn Error>> {
        let ctx = SessionContext::new();

        for table_name in [
            "lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier",
        ] {
            let query_path = format!("testdata/tpch/{}.parquet", table_name);
            ctx.register_parquet(
                table_name,
                query_path,
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await?;
        }
        let sql = tpch_query(2);
        println!("SQL Query:\n{}", sql);

        let df = ctx.sql(&sql).await?;

        let physical = df.create_physical_plan().await?;

        println!(
            "\n\nPhysical Plan:\n{}",
            displayable(physical.as_ref()).indent(false)
        );

        let stage_plan: ExecutionStage = physical.try_into()?;

        let stage_str = display_stage_tree(&stage_plan)?;
        println!("\n\nStage Plan Tree:\n{}", stage_str);
        let stage_str = display_stage(&stage_plan)?;

        println!("\n\nStage Plan:\n{}", stage_str);

        assert_snapshot!(stage_str,
            @r"
        ",
        );

        /*let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");*/

        Ok(())
    }
}
