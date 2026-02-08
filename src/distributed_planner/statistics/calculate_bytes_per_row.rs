use datafusion::arrow::datatypes::{DataType, IntervalUnit, SchemaRef};

/// Default data size estimate for variable-width columns when no statistics are available.
///
/// Reference: Trino's PlanNodeStatsEstimate.java:40
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/PlanNodeStatsEstimate.java#L40
const DEFAULT_DATA_SIZE_PER_COLUMN: usize = 50;

/// This function returns the amount of bytes each row is estimated to occupy.
///
/// The estimation follows Trino's approach for calculating output size per row:
/// - For fixed-width (primitive) types: uses the type's fixed byte width
/// - For variable-width types: uses a default estimate plus offset overhead
/// - Accounts for validity bitmap overhead (1 bit per value, rounded to 1 byte per row)
///
/// DataFusion has `Statistics::calculate_total_byte_size()` which uses `DataType::primitive_width()`,
/// but it returns `Precision::Absent` (unknown) when encountering any non-primitive type:
/// https://github.com/apache/datafusion/blob/branch-52/datafusion/common/src/stats.rs#L326-L347
///
/// For distributed query planning, we need estimates even for variable-width types to make
/// cost-based decisions about data shuffling and task count assignation. This implementation
/// provides estimates for all types following Trino's cost model.
///
/// Reference: Trino's PlanNodeStatsEstimate.getOutputSizeForSymbol()
/// https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/PlanNodeStatsEstimate.java#L89-L114
pub(crate) fn calculate_bytes_returned_per_row(schema: &SchemaRef) -> usize {
    schema
        .fields()
        .iter()
        .map(|field| calculate_bytes_returned_per_cell(field.data_type()))
        .sum()
}

pub(crate) fn calculate_bytes_returned_per_cell(data_type: &DataType) -> usize {
    // 1 byte for validity bitmap per row (Arrow uses 1 bit, but we round up for estimation).
    // Trino calls this the "is null" boolean array.
    // Reference: PlanNodeStatsEstimate.java:98-99
    // https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/PlanNodeStatsEstimate.java#L98-L99
    const VALIDITY_OVERHEAD: usize = 1;

    // Handle non-primitive types.
    // NOTE: The cases below are Arrow-specific adaptations. Trino only distinguishes between
    // FixedWidthType and variable-width types, using Integer.BYTES (4) for offsets.
    // Reference: PlanNodeStatsEstimate.java:108-109
    // https://github.com/trinodb/trino/blob/458/core/trino-main/src/main/java/io/trino/cost/PlanNodeStatsEstimate.java#L108-L109
    match data_type {
        // Primitive types from data_type.primitive_width()
        DataType::Int8 => VALIDITY_OVERHEAD + 1,
        DataType::Int16 => VALIDITY_OVERHEAD + 2,
        DataType::Int32 => VALIDITY_OVERHEAD + 4,
        DataType::Int64 => VALIDITY_OVERHEAD + 8,
        DataType::UInt8 => VALIDITY_OVERHEAD + 1,
        DataType::UInt16 => VALIDITY_OVERHEAD + 2,
        DataType::UInt32 => VALIDITY_OVERHEAD + 4,
        DataType::UInt64 => VALIDITY_OVERHEAD + 8,
        DataType::Float16 => VALIDITY_OVERHEAD + 2,
        DataType::Float32 => VALIDITY_OVERHEAD + 4,
        DataType::Float64 => VALIDITY_OVERHEAD + 8,
        DataType::Timestamp(_, _) => VALIDITY_OVERHEAD + 8,
        DataType::Date32 => VALIDITY_OVERHEAD + 4,
        DataType::Date64 => VALIDITY_OVERHEAD + 8,
        DataType::Time32(_) => VALIDITY_OVERHEAD + 4,
        DataType::Time64(_) => VALIDITY_OVERHEAD + 8,
        DataType::Duration(_) => VALIDITY_OVERHEAD + 8,
        DataType::Interval(IntervalUnit::YearMonth) => VALIDITY_OVERHEAD + 4,
        DataType::Interval(IntervalUnit::DayTime) => VALIDITY_OVERHEAD + 8,
        DataType::Interval(IntervalUnit::MonthDayNano) => VALIDITY_OVERHEAD + 16,
        DataType::Decimal32(_, _) => VALIDITY_OVERHEAD + 4,
        DataType::Decimal64(_, _) => VALIDITY_OVERHEAD + 8,
        DataType::Decimal128(_, _) => VALIDITY_OVERHEAD + 16,
        DataType::Decimal256(_, _) => VALIDITY_OVERHEAD + 32,
        // Null type has no data (Arrow-specific)
        DataType::Null => 0,

        // Boolean is stored as bits (1/8 byte per value), but we round up (Arrow-specific)
        DataType::Boolean => VALIDITY_OVERHEAD + 1,

        // Fixed-size binary: just the fixed size + validity (Arrow-specific)
        DataType::FixedSizeBinary(size) => VALIDITY_OVERHEAD + (*size as usize),

        // Fixed-size list: fixed count * element size (Arrow-specific)
        DataType::FixedSizeList(field, size) => {
            VALIDITY_OVERHEAD
                + (*size as usize) * calculate_bytes_returned_per_cell(field.data_type())
        }

        // Struct: sum of all child field sizes (Arrow-specific)
        // Trino would treat ROW types as variable-width
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| calculate_bytes_returned_per_cell(f.data_type()))
            .sum(),

        // Dictionary-encoded: just the key indices, values are shared across rows (Arrow-specific)
        // Trino doesn't have dictionary encoding at the type level
        DataType::Dictionary(key_type, _value_type) => calculate_bytes_returned_per_cell(key_type),

        // Union: type_id (1 byte) + max child size (Arrow-specific)
        DataType::Union(fields, _) => {
            let max_child_size = fields
                .iter()
                .map(|(_, f)| calculate_bytes_returned_per_cell(f.data_type()))
                .max()
                .unwrap_or(0);
            1 + max_child_size
        }

        // Run-end encoded: estimate as if it were the value type (Arrow-specific)
        // Actual compression depends on data distribution
        DataType::RunEndEncoded(_, values) => calculate_bytes_returned_per_cell(values.data_type()),

        // Variable-width string/binary types.
        // Offset size follows Trino's Integer.BYTES (4 bytes).
        // Reference: PlanNodeStatsEstimate.java:109
        DataType::Utf8 | DataType::Binary => {
            VALIDITY_OVERHEAD + size_of::<i32>() + DEFAULT_DATA_SIZE_PER_COLUMN
        }
        // Large variants use i64 offsets (Arrow-specific, Trino doesn't have large variants)
        DataType::LargeUtf8 | DataType::LargeBinary => {
            VALIDITY_OVERHEAD + size_of::<i64>() + DEFAULT_DATA_SIZE_PER_COLUMN
        }
        // View types use 16-byte inline representation (Arrow-specific)
        // Reference: https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout
        DataType::Utf8View | DataType::BinaryView => VALIDITY_OVERHEAD + 16,

        // List types (Arrow-specific adaptation)
        // Trino doesn't have explicit array size estimation; we assume 10 elements average.
        // This is a heuristic not from Trino.
        DataType::List(field) => {
            VALIDITY_OVERHEAD
                + size_of::<i32>()
                + 10 * calculate_bytes_returned_per_cell(field.data_type())
        }
        DataType::LargeList(field) => {
            VALIDITY_OVERHEAD
                + size_of::<i64>()
                + 10 * calculate_bytes_returned_per_cell(field.data_type())
        }
        DataType::ListView(field) | DataType::LargeListView(field) => {
            VALIDITY_OVERHEAD + 8 + 10 * calculate_bytes_returned_per_cell(field.data_type())
        }

        // Map type: stored as List<Struct<key, value>> (Arrow-specific)
        // Uses same 10-element heuristic as List types.
        DataType::Map(field, _) => {
            VALIDITY_OVERHEAD
                + size_of::<i32>()
                + 10 * calculate_bytes_returned_per_cell(field.data_type())
        } // Fallback for any other types - use Trino's default
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_primitive_types() {
        // Int32: 1 byte validity + 4 bytes data = 5 bytes
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        assert_eq!(calculate_bytes_returned_per_row(&schema), 5);

        // Int64: 1 byte validity + 8 bytes data = 9 bytes
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        assert_eq!(calculate_bytes_returned_per_row(&schema), 9);

        // Float64: 1 byte validity + 8 bytes data = 9 bytes
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        assert_eq!(calculate_bytes_returned_per_row(&schema), 9);

        // Boolean: 1 byte validity + 1 byte data = 2 bytes
        // (Arrow stores booleans as bits, but we round up for estimation)
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Boolean, true)]));
        assert_eq!(calculate_bytes_returned_per_row(&schema), 2);
    }

    #[test]
    fn test_variable_width_types() {
        // Utf8: 1 byte validity + 4 bytes offset + 50 bytes default = 55 bytes
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        assert_eq!(calculate_bytes_returned_per_row(&schema), 55);

        // LargeUtf8: 1 byte validity + 8 bytes offset + 50 bytes default = 59 bytes
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            DataType::LargeUtf8,
            true,
        )]));
        assert_eq!(calculate_bytes_returned_per_row(&schema), 59);
    }

    #[test]
    fn test_multiple_columns() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("balance", DataType::Float64, false),
        ]));
        // Int32: 5 + Utf8: 55 + Float64: 9 = 69 bytes
        assert_eq!(calculate_bytes_returned_per_row(&schema), 69);
    }

    #[test]
    fn test_nested_struct() {
        let inner_fields = vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, false),
        ];
        let schema = Arc::new(Schema::new(vec![Field::new(
            "point",
            DataType::Struct(inner_fields.into()),
            true,
        )]));
        // Struct with 2 Int32 fields: (1 + 4) + (1 + 4) = 10 bytes
        assert_eq!(calculate_bytes_returned_per_row(&schema), 10);
    }

    #[test]
    fn test_dictionary_encoded() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "category",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            true,
        )]));
        // Dictionary with UInt16 keys: 1 byte validity + 2 bytes key = 3 bytes
        // (value dictionary is shared, not counted per row)
        assert_eq!(calculate_bytes_returned_per_row(&schema), 3);
    }

    #[test]
    fn test_fixed_size_binary() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hash",
            DataType::FixedSizeBinary(32),
            false,
        )]));
        // FixedSizeBinary(32): 1 byte validity + 32 bytes data = 33 bytes
        assert_eq!(calculate_bytes_returned_per_row(&schema), 33);
    }

    #[test]
    fn test_list_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]));
        // List<Int32>: 1 byte validity + 4 bytes offset + 10 * (1 + 4) = 55 bytes
        assert_eq!(calculate_bytes_returned_per_row(&schema), 55);
    }
}
