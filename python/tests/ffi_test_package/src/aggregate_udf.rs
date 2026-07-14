use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Signature};
use datafusion_ffi::udaf::FFI_AggregateUDF;
use datafusion_functions_aggregate::sum::Sum;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[pyclass(from_py_object, module = "datafusion_distributed_ffi_test")]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct ForeignSumUDF {
    inner: Arc<Sum>,
}

#[pymethods]
impl ForeignSumUDF {
    #[new]
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Sum::new()),
        }
    }

    fn __datafusion_aggregate_udf__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(AggregateUDF::from(self.clone()));
        PyCapsule::new(
            py,
            FFI_AggregateUDF::from(udf),
            Some(cr"datafusion_aggregate_udf".into()),
        )
    }
}

impl AggregateUDFImpl for ForeignSumUDF {
    fn name(&self) -> &str {
        "foreign_sum"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        self.inner.accumulator(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}
