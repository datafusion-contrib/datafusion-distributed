use datafusion::prelude::Expr;

/// Optional fields for building an [IcebergDataSource](crate::IcebergDataSource).
#[derive(Default, Clone)]
pub(crate) struct IcebergDataSourceOptions<'a> {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) fetch: Option<usize>,
    pub(crate) filters: &'a [Expr],
    pub(crate) iceberg_runtime: Option<iceberg::Runtime>,
}
