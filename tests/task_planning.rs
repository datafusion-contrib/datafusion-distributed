mod common;
mod tpch;

#[cfg(test)]
mod tests {
    use crate::tpch::tpch_query;
    use crate::{assert_snapshot, tpch};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::task::TaskPlanner;
    use datafusion_distributed::{display_stage, display_stage_tree, ExecutionStage, StagePlanner};
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    #[tokio::test]
    async fn task_planning() -> Result<(), Box<dyn Error>> {
        let config = SessionConfig::new().with_target_partitions(3);

        let ctx = SessionContext::new_with_config(config);

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

        let physical_str = displayable(physical.as_ref()).indent(false).to_string();
        println!("\n\nPhysical Plan:\n{}", physical_str);

        let stage_plan = physical.try_into().map(Arc::new)?;

        let stage_str = display_stage(&stage_plan)?;

        println!("\n\nStage Plan:\n{}", stage_str);

        let task = TaskPlanner::new(2, None).create_task(stage_plan)?;

        let task_str = displayable(task.as_ref()).tree_render().to_string();

        println!("\n\nTask Plan:\n{}", task_str);

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
