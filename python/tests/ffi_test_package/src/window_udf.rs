use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::Result;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{PartitionEvaluator, Signature, WindowUDF, WindowUDFImpl};
use datafusion_ffi::udwf::FFI_WindowUDF;
use datafusion_functions_window::rank::rank_udwf;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[pyclass(from_py_object, module = "datafusion_distributed_ffi_test")]
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct ForeignRankUDF {
    inner: Arc<WindowUDF>,
}

#[pymethods]
impl ForeignRankUDF {
    #[new]
    pub(crate) fn new() -> Self {
        Self { inner: rank_udwf() }
    }

    fn __datafusion_window_udf__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let udf = Arc::new(WindowUDF::from(self.clone()));
        PyCapsule::new(
            py,
            FFI_WindowUDF::from(udf),
            Some(cr"datafusion_window_udf".into()),
        )
    }
}

impl WindowUDFImpl for ForeignRankUDF {
    fn name(&self) -> &str {
        "foreign_rank"
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        self.inner.inner().partition_evaluator(args)
    }

    fn field(&self, args: WindowUDFFieldArgs) -> Result<FieldRef> {
        self.inner.inner().field(args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }
}
