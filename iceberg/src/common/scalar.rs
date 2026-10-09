use datafusion::scalar::ScalarValue;
use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType};

/// Conversion function of iceberg's Datum
pub(crate) fn datum_to_scalar(d: &Datum) -> Option<ScalarValue> {
    primitive_to_scalar(d.data_type(), d.literal())
}

/// Converts an Iceberg primitive literal of the given type into a [ScalarValue]. Returns `None`
/// for the types that have no supported conversion.
pub(crate) fn primitive_to_scalar(
    data_type: &PrimitiveType,
    literal: &PrimitiveLiteral,
) -> Option<ScalarValue> {
    match (data_type, literal) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(v)) => {
            Some(ScalarValue::Boolean(Some(*v)))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(v)) => Some(ScalarValue::Int32(Some(*v))),
        (PrimitiveType::Long, PrimitiveLiteral::Long(v)) => Some(ScalarValue::Int64(Some(*v))),
        (PrimitiveType::Float, PrimitiveLiteral::Float(v)) => {
            Some(ScalarValue::Float32(Some(v.into_inner())))
        }
        (PrimitiveType::Double, PrimitiveLiteral::Double(v)) => {
            Some(ScalarValue::Float64(Some(v.into_inner())))
        }
        (PrimitiveType::String, PrimitiveLiteral::String(s)) => {
            Some(ScalarValue::Utf8(Some(s.clone())))
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(v)) => Some(ScalarValue::Date32(Some(*v))),
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(v)) => {
            Some(ScalarValue::TimestampMicrosecond(Some(*v), None))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(v)) => Some(
            ScalarValue::TimestampMicrosecond(Some(*v), Some("UTC".into())),
        ),
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(v)) => Some(
            ScalarValue::Decimal128(Some(*v), *precision as u8, *scale as i8),
        ),
        _ => None,
    }
}
