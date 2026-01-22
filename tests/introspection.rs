#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::physical_plan::execute_stream;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{WorkerQueryContext, assert_snapshot, display_plan_ascii};
    use futures::TryStreamExt;
    use std::error::Error;

    #[tokio::test]
    async fn distributed_show_columns() -> Result<(), Box<dyn Error>> {
        let (ctx, _guard) = start_localhost_context(3, |mut ctx: WorkerQueryContext| async {
            let cfg = ctx.builder.config().get_or_insert_default();
            let opts = cfg.options_mut();
            opts.catalog.information_schema = true;
            Ok(ctx.builder.build())
        })
        .await;
        register_parquet_tables(&ctx).await?;

        let df = ctx.sql(r#"SHOW COLUMNS from weather"#).await?;
        let physical_distributed = df.create_physical_plan().await?;

        let physical_distributed_str = display_plan_ascii(physical_distributed.as_ref(), false);

        assert_snapshot!(physical_distributed_str,
            @r"
        ProjectionExec: expr=[table_catalog@0 as table_catalog, table_schema@1 as table_schema, table_name@2 as table_name, column_name@3 as column_name, data_type@5 as data_type, is_nullable@4 as is_nullable]
          FilterExec: table_name@2 = weather
            RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1
              StreamingTableExec: partition_sizes=1, projection=[table_catalog, table_schema, table_name, column_name, is_nullable, data_type]
        ",
        );

        let batches_distributed = pretty_format_batches(
            &execute_stream(physical_distributed, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;
        assert_snapshot!(batches_distributed, @r"
        +---------------+--------------+------------+---------------+-----------+-------------+
        | table_catalog | table_schema | table_name | column_name   | data_type | is_nullable |
        +---------------+--------------+------------+---------------+-----------+-------------+
        | datafusion    | public       | weather    | MinTemp       | Float64   | YES         |
        | datafusion    | public       | weather    | MaxTemp       | Float64   | YES         |
        | datafusion    | public       | weather    | Rainfall      | Float64   | YES         |
        | datafusion    | public       | weather    | Evaporation   | Float64   | YES         |
        | datafusion    | public       | weather    | Sunshine      | Utf8View  | YES         |
        | datafusion    | public       | weather    | WindGustDir   | Utf8View  | YES         |
        | datafusion    | public       | weather    | WindGustSpeed | Utf8View  | YES         |
        | datafusion    | public       | weather    | WindDir9am    | Utf8View  | YES         |
        | datafusion    | public       | weather    | WindDir3pm    | Utf8View  | YES         |
        | datafusion    | public       | weather    | WindSpeed9am  | Utf8View  | YES         |
        | datafusion    | public       | weather    | WindSpeed3pm  | Int64     | YES         |
        | datafusion    | public       | weather    | Humidity9am   | Int64     | YES         |
        | datafusion    | public       | weather    | Humidity3pm   | Int64     | YES         |
        | datafusion    | public       | weather    | Pressure9am   | Float64   | YES         |
        | datafusion    | public       | weather    | Pressure3pm   | Float64   | YES         |
        | datafusion    | public       | weather    | Cloud9am      | Int64     | YES         |
        | datafusion    | public       | weather    | Cloud3pm      | Int64     | YES         |
        | datafusion    | public       | weather    | Temp9am       | Float64   | YES         |
        | datafusion    | public       | weather    | Temp3pm       | Float64   | YES         |
        | datafusion    | public       | weather    | RainToday     | Utf8View  | YES         |
        | datafusion    | public       | weather    | RISK_MM       | Float64   | YES         |
        | datafusion    | public       | weather    | RainTomorrow  | Utf8View  | YES         |
        +---------------+--------------+------------+---------------+-----------+-------------+
        ");

        Ok(())
    }
}
