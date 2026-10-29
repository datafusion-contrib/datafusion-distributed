use datafusion::error::DataFusionError;

/// Converts an Iceberg error into a DataFusion error, keeping it as the source.
pub(crate) fn df_err(error: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
