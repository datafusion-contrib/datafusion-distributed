use datafusion::arrow::datatypes::{DataType, IntervalUnit};

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
pub(crate) fn default_bytes_for_datatype(data_type: &DataType) -> usize {
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
            VALIDITY_OVERHEAD + (*size as usize) * default_bytes_for_datatype(field.data_type())
        }

        // Struct: sum of all child field sizes (Arrow-specific)
        // Trino would treat ROW types as variable-width
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| default_bytes_for_datatype(f.data_type()))
            .sum(),

        // Dictionary-encoded: just the key indices, values are shared across rows (Arrow-specific)
        // Trino doesn't have dictionary encoding at the type level
        DataType::Dictionary(key_type, _value_type) => default_bytes_for_datatype(key_type),

        // Union: type_id (1 byte) + max child size (Arrow-specific)
        DataType::Union(fields, _) => {
            let max_child_size = fields
                .iter()
                .map(|(_, f)| default_bytes_for_datatype(f.data_type()))
                .max()
                .unwrap_or(0);
            1 + max_child_size
        }

        // Run-end encoded: estimate as if it were the value type (Arrow-specific)
        // Actual compression depends on data distribution
        DataType::RunEndEncoded(_, values) => default_bytes_for_datatype(values.data_type()),

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
        // Spark assumes 1 element average for collections (SPARK-18853). Trino treats them
        // as flat variable-width with 50-byte default. We follow Spark's 1-element assumption
        // to avoid massive overestimation (e.g. Map<Int,String> was 605 bytes with 10 elements).
        DataType::List(field) => {
            VALIDITY_OVERHEAD + size_of::<i32>() + default_bytes_for_datatype(field.data_type())
        }
        DataType::LargeList(field) => {
            VALIDITY_OVERHEAD + size_of::<i64>() + default_bytes_for_datatype(field.data_type())
        }
        DataType::ListView(field) | DataType::LargeListView(field) => {
            VALIDITY_OVERHEAD + 8 + default_bytes_for_datatype(field.data_type())
        }

        // Map type: stored as List<Struct<key, value>> (Arrow-specific)
        // Uses same 1-element assumption as List types (following Spark).
        DataType::Map(field, _) => {
            VALIDITY_OVERHEAD + size_of::<i32>() + default_bytes_for_datatype(field.data_type())
        } // Fallback for any other types - use Trino's default
    }
}
