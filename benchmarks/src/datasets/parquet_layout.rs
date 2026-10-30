use super::output::DatasetOutput;
use arrow::array::RecordBatch;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::object_store::buffered::BufWriter;
use parquet::arrow::AsyncArrowWriter;
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::WriterProperties;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use structopt::StructOpt;

/// How generated tables are laid out in Parquet files, parsed from command line flags.
///
/// Every field is part of the dataset path, so datasets generated with different layouts never
/// share a directory.
#[derive(Debug, Clone, PartialEq, StructOpt)]
pub struct ParquetLayout {
    /// Approximate maximum bytes per file. A file can exceed it by up to one batch
    #[structopt(long, default_value = "134217728")]
    pub max_file_size: usize,

    /// Dictionary encoding: none, pages or arrow
    #[structopt(long, default_value = "pages")]
    pub dictionary: Dictionary,

    /// Write Parquet sorting_columns metadata for the columns the generator emits in order
    #[structopt(long)]
    pub sorted: bool,
}

impl Default for ParquetLayout {
    /// The layout the command line flags default to.
    fn default() -> Self {
        Self::from_iter(["parquet-layout"])
    }
}

impl ParquetLayout {
    /// The `key=value` path segments describing this layout, in their fixed order.
    pub fn path(&self) -> String {
        format!(
            "max_file_size={}/dictionary={}/sorted={}",
            self.max_file_size, self.dictionary, self.sorted,
        )
    }

    fn writer_properties(&self, schema: &Schema, sort_keys: &[&str]) -> Result<WriterProperties> {
        let mut builder =
            WriterProperties::builder().set_dictionary_enabled(self.dictionary != Dictionary::None);
        if self.sorted && !sort_keys.is_empty() {
            let sorting_columns = sort_keys
                .iter()
                .map(|name| {
                    Ok(SortingColumn {
                        column_idx: schema.index_of(name)? as i32,
                        descending: false,
                        nulls_first: false,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            builder = builder.set_sorting_columns(Some(sorting_columns));
        }
        Ok(builder.build())
    }
}

/// Which dictionary encoding generated columns use.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Dictionary {
    /// Plain-encoded Parquet pages only.
    None,
    /// Parquet dictionary pages, falling back to plain encoding for large dictionaries.
    Pages,
    /// Parquet dictionary pages, plus Arrow `Dictionary` types for the suite's low-cardinality
    /// string columns, which readers then load as dictionary arrays.
    Arrow,
}

impl fmt::Display for Dictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "none"),
            Self::Pages => write!(f, "pages"),
            Self::Arrow => write!(f, "arrow"),
        }
    }
}

impl FromStr for Dictionary {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "none" => Ok(Self::None),
            "pages" => Ok(Self::Pages),
            "arrow" => Ok(Self::Arrow),
            value => Err(format!("expected none, pages or arrow, got '{value}'")),
        }
    }
}

/// Columns of one table that the layout treats specially.
#[derive(Debug, Clone, Copy, Default)]
pub struct TableColumns {
    /// Columns the generator emits in ascending order, declared when the layout is sorted.
    pub sort_keys: &'static [&'static str],
    /// Low-cardinality string columns stored as Arrow dictionaries with [`Dictionary::Arrow`].
    pub dictionary: &'static [&'static str],
}

/// Writes the ordered stream of batches of one table as `<table>/1.parquet`,
/// `<table>/2.parquet`, ..., starting a new file whenever the layout's file size is reached.
pub struct TableWriter<'a> {
    output: &'a DatasetOutput,
    layout: &'a ParquetLayout,
    table: &'a str,
    columns: TableColumns,
    /// Set up from the schema of the first batch, which every file of the table shares.
    properties: Option<WriterProperties>,
    writer: Option<AsyncArrowWriter<BufWriter>>,
    files: usize,
}

impl<'a> TableWriter<'a> {
    pub fn new(
        output: &'a DatasetOutput,
        layout: &'a ParquetLayout,
        table: &'a str,
        columns: TableColumns,
    ) -> Self {
        Self {
            output,
            layout,
            table,
            columns,
            properties: None,
            writer: None,
            files: 0,
        }
    }

    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = if self.layout.dictionary == Dictionary::Arrow {
            dictionary_encode(batch, self.columns.dictionary)?
        } else {
            batch
        };
        if self.properties.is_none() {
            self.properties = Some(
                self.layout
                    .writer_properties(batch.schema_ref(), self.columns.sort_keys)?,
            );
        }
        let mut writer = match self.writer.take() {
            Some(writer) => writer,
            None => {
                self.files += 1;
                AsyncArrowWriter::try_new(
                    self.output
                        .writer(&format!("{}/{}.parquet", self.table, self.files)),
                    batch.schema(),
                    self.properties.clone(),
                )?
            }
        };
        writer.write(&batch).await?;
        // The file can exceed the limit by up to one batch.
        if writer.bytes_written() + writer.in_progress_size() >= self.layout.max_file_size {
            writer.close().await?;
        } else {
            self.writer = Some(writer);
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close().await?;
        }
        Ok(())
    }
}

/// Casts the given string columns to `Dictionary(UInt16, Utf8)`.
fn dictionary_encode(batch: RecordBatch, columns: &[&str]) -> Result<RecordBatch> {
    if columns.is_empty() {
        return Ok(batch);
    }
    let dictionary_type =
        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8));
    let schema = batch.schema();
    let (fields, arrays): (Vec<_>, Vec<_>) = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, array)| {
            if columns.contains(&field.name().as_str()) {
                Ok((
                    Arc::new(Field::new(
                        field.name(),
                        dictionary_type.clone(),
                        field.is_nullable(),
                    )),
                    cast(array, &dictionary_type)?,
                ))
            } else {
                Ok((Arc::clone(field), Arc::clone(array)))
            }
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
        arrays,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_layout_path() {
        assert_eq!(
            ParquetLayout::default().path(),
            "max_file_size=134217728/dictionary=pages/sorted=false"
        );
    }
}
