#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::common::Result;
    use datafusion::physical_plan::operator_statistics::StatisticsRegistry;
    use datafusion_distributed::ColumnByteStatisticsProvider;
    use datafusion_distributed_iceberg::IcebergExt;
    use datafusion_distributed_iceberg::test_utils::{
        IcebergTestHarness, IcebergTestHarnessBuilder,
    };

    #[tokio::test]
    async fn column_statistics_preserve_nonempty_and_empty_semi_join_results() -> Result<()> {
        let baseline = harness(false)?.build().await?;
        let improved = harness(true)?.build().await?;
        for date in ["2024-01-10", "1900-01-01"] {
            let sql = query(date);
            let (_, expected) = baseline.query(&sql).await?;
            let (_, actual) = improved.query(&sql).await?;
            assert_eq!(actual, expected);
        }
        Ok(())
    }

    #[cfg(feature = "integration")]
    #[tokio::test]
    async fn propagated_statistics_preserve_distributed_join_results() -> Result<()> {
        let (_, expected) = harness(false)?
            .build()
            .await?
            .query(&query("2024-01-10"))
            .await?;
        let (_, actual) = harness(true)?
            .with_workers(2)
            .build()
            .await?
            .query(&query("2024-01-10"))
            .await?;
        assert_eq!(actual, expected);
        Ok(())
    }

    fn harness(enabled: bool) -> Result<IcebergTestHarnessBuilder> {
        IcebergTestHarness::builder().configure_session(|mut state| {
            state.set_iceberg_column_stats_enabled(enabled);
            state
                .config()
                .get_or_insert_default()
                .options_mut()
                .optimizer
                .use_statistics_registry = enabled;
            if enabled {
                state = state.with_statistics_registry(StatisticsRegistry::with_providers(vec![
                    Arc::new(ColumnByteStatisticsProvider),
                ]));
            }
            Ok(state)
        })
    }

    fn query(date: &str) -> String {
        format!(
            "SELECT vendor_id, count(*) AS trips FROM taxi WHERE vendor_id IN \
            (SELECT DISTINCT vendor_id FROM taxi WHERE pickup_date = DATE '{date}') \
            GROUP BY vendor_id ORDER BY vendor_id"
        )
    }
}
