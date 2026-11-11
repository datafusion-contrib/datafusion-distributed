#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion_distributed::test_utils::localhost::start_localhost_context;
    use datafusion_distributed::test_utils::parquet::register_parquet_tables;
    use datafusion_distributed::{DefaultSessionBuilder, assert_snapshot};

    #[tokio::test]
    async fn window_partition_by_projected_constant() -> Result<()> {
        let (ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        register_parquet_tables(&ctx).await?;

        // Only WindGustDir needs sorting: typ is projected from a literal and RainToday
        // is fixed by the filter. Workers must retain both constant facts.
        let result = ctx
            .sql(
                r#"
            SELECT typ, "RainToday", "WindGustDir"
            FROM (
                SELECT typ, "RainToday", "WindGustDir",
                       row_number() OVER (PARTITION BY typ, "RainToday", "WindGustDir") AS rn
                FROM (SELECT 'weather' AS typ, * FROM weather)
                WHERE "RainToday" = 'No' AND "WindGustDir" IN ('N', 'S')
            )
            WHERE rn = 1
            ORDER BY "WindGustDir"
        "#,
            )
            .await?
            .collect()
            .await?;

        let result = pretty_format_batches(&result)?.to_string();
        assert_snapshot!(result, @r"
        +---------+-----------+-------------+
        | typ     | RainToday | WindGustDir |
        +---------+-----------+-------------+
        | weather | No        | N           |
        | weather | No        | S           |
        +---------+-----------+-------------+
        ");
        Ok(())
    }
}
