use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_ffi::udf::FFI_ScalarUDF;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[pyclass(from_py_object, module = "datafusion_distributed_ffi_test")]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ForeignIsNullUDF {
    signature: Signature,
}

#[pymethods]
impl ForeignIsNullUDF {
    #[new]
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Any(1), Volatility::Immutable),
        }
    }

    fn __datafusion_scalar_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(ScalarUDF::from(self.clone()));
        PyCapsule::new(
            py,
            FFI_ScalarUDF::from(udf),
            Some(cr"datafusion_scalar_udf".into()),
        )
    }
}

impl ScalarUDFImpl for ForeignIsNullUDF {
    fn name(&self) -> &str {
        "foreign_is_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(match &args.args[0] {
            ColumnarValue::Array(array) if array.is_nullable() => {
                let nulls = array.nulls().unwrap();
                ColumnarValue::Array(Arc::new(BooleanArray::from_iter(
                    nulls.iter().map(|valid| Some(!valid)),
                )))
            }
            ColumnarValue::Array(_) => ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
            ColumnarValue::Scalar(value) => {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(value == &ScalarValue::Null)))
            }
        })
    }
}
