use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

/// Adds two test tables to the provided [SessionContext]:
/// - `flights_1m`: 1M rows of flight data.
/// - `weather`: smaller dataset with weather forecast.
///
/// Useful for testing queries using SQL directly.
pub async fn register_parquet_tables(ctx: &SessionContext) -> Result<(), DataFusionError> {
    ctx.register_parquet(
        "flights_1m",
        "testdata/flights-1m.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    ctx.register_parquet("weather", "testdata/weather", ParquetReadOptions::default())
        .await?;

    Ok(())
}
