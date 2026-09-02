#[cfg(test)]
mod tests {
    use crate::common::TestQuery;
    use datafusion::common::Result;

    #[tokio::test]
    async fn completed_filter_collection_can_be_disabled() -> Result<()> {
        TestQuery::new(
            r#"
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT "RainToday" AS key
                    FROM weather
                ) build
                JOIN weather probe ON build.key = probe."RainToday"
            "#,
        )
        .without_dynamic_filter_collection()
         // execute() asserts that dynamic filters were not collected for display by asserting
        // that the plan is not rewritten.
        .execute()
        .await?;
        Ok(())
    }
}
