use std::collections::HashMap;

use datafusion::common::{exec_datafusion_err, exec_err, internal_datafusion_err};
use datafusion::error::Result;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, Struct,
    TableMetadata,
};

/// A data file that [`Append::described_file`](crate::Append::described_file)
/// lists in a manifest without writing it.
///
/// Its record count, size, partition and column metrics are exactly the ones
/// given, so tests can plan against metrics that real files wouldn't have,
/// such as a missing null count. Scans that open it fail, since the file
/// doesn't exist.
pub struct DescribedFile {
    name: String,
    record_count: u64,
    file_size_in_bytes: u64,
    partition: Vec<Literal>,
    columns: Vec<(String, ColumnMetrics)>,
}

impl DescribedFile {
    /// Describes file `name` in the table's `data` directory, holding
    /// `record_count` rows in `file_size_in_bytes` bytes.
    pub fn new(name: impl Into<String>, record_count: u64, file_size_in_bytes: u64) -> Self {
        Self {
            name: name.into(),
            record_count,
            file_size_in_bytes,
            partition: Vec::new(),
            columns: Vec::new(),
        }
    }

    /// Sets the file's partition values, one per field of the table's
    /// partition spec, in order.
    pub fn partition(mut self, values: impl IntoIterator<Item = Literal>) -> Self {
        self.partition = values.into_iter().collect();
        self
    }

    /// Sets the metrics of top-level `column`. Columns without metrics have
    /// none in the manifest.
    pub fn column(mut self, column: impl Into<String>, metrics: ColumnMetrics) -> Self {
        self.columns.push((column.into(), metrics));
        self
    }

    /// Returns the manifest entry for this file in the table `metadata`
    /// describes.
    pub(crate) fn into_data_file(self, metadata: &TableMetadata) -> Result<DataFile> {
        let schema = metadata.current_schema();
        let partition_spec = metadata.default_partition_spec();
        if self.partition.len() != partition_spec.fields().len() {
            return exec_err!(
                "described file {} has {} partition values, but the table is partitioned by {} fields",
                self.name,
                self.partition.len(),
                partition_spec.fields().len()
            );
        }
        let mut null_value_counts = HashMap::new();
        let mut column_sizes = HashMap::new();
        let mut lower_bounds = HashMap::new();
        let mut upper_bounds = HashMap::new();
        for (column, metrics) in self.columns {
            let field = schema
                .field_by_name(&column)
                .ok_or_else(|| exec_datafusion_err!("the table has no column {column}"))?;
            if let Some(null_count) = metrics.null_count {
                null_value_counts.insert(field.id, null_count);
            }
            if let Some(size) = metrics.size {
                column_sizes.insert(field.id, size);
            }
            if let Some((lower, upper)) = metrics.bounds {
                let column_type = field.field_type.as_primitive_type();
                if column_type != Some(lower.data_type()) || column_type != Some(upper.data_type())
                {
                    return exec_err!(
                        "the bounds of {column} must have its type, {}",
                        field.field_type
                    );
                }
                lower_bounds.insert(field.id, lower);
                upper_bounds.insert(field.id, upper);
            }
        }
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_format(DataFileFormat::Parquet)
            .file_path(format!("{}/data/{}", metadata.location(), self.name))
            .record_count(self.record_count)
            .file_size_in_bytes(self.file_size_in_bytes)
            .partition(Struct::from_iter(self.partition.into_iter().map(Some)))
            .partition_spec_id(partition_spec.spec_id())
            .null_value_counts(null_value_counts)
            .column_sizes(column_sizes)
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            .build()
            .map_err(|error| internal_datafusion_err!("invalid described file: {error}"))
    }
}

/// The manifest metrics of one column of a [`DescribedFile`]. Metrics that
/// aren't set are left out of the manifest.
#[derive(Clone, Debug, Default)]
pub struct ColumnMetrics {
    null_count: Option<u64>,
    size: Option<u64>,
    bounds: Option<(Datum, Datum)>,
}

impl ColumnMetrics {
    /// Sets the number of null values in the column.
    pub fn null_count(mut self, null_count: u64) -> Self {
        self.null_count = Some(null_count);
        self
    }

    /// Sets the column's size on disk, in bytes.
    pub fn size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    /// Sets the column's lower and upper bounds, which must have its type.
    pub fn bounds(mut self, lower: Datum, upper: Datum) -> Self {
        self.bounds = Some((lower, upper));
        self
    }
}
