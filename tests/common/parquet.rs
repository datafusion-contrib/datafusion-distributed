use datafusion::error::DataFusionError;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

pub async fn register_parquet_tables(ctx: &SessionContext) -> Result<(), DataFusionError> {
    ctx.register_parquet(
        "flights_1m",
        "testdata/flights-1m.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    ctx.register_parquet(
        "weather",
        "testdata/weather.parquet",
        ParquetReadOptions::default(),
    )
    .await?;

    Ok(())
}
