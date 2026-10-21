use datafusion::common::Result;
use datafusion::config::Dialect;
use datafusion_distributed::DefaultSessionBuilder;
use datafusion_distributed::test_utils::localhost::start_localhost_context;

#[tokio::main]
async fn main() -> Result<()> {
    let (ctx, _workers, _) = start_localhost_context(2, DefaultSessionBuilder).await;
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .sql_parser
        .dialect = Dialect::PostgreSQL;

    ctx.sql(
        "WITH x AS MATERIALIZED (
            SELECT id, random() AS value FROM (VALUES (1), (2), (3)) AS source(id)
        )
        SELECT a.id, a.value = b.value AS same_value
        FROM x a JOIN x b ON a.id = b.id
        ORDER BY a.id",
    )
    .await?
    .show()
    .await?;
    Ok(())
}
