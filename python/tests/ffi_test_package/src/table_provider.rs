use std::fmt::Formatter;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_ffi::table_provider::FFI_TableProvider;
use datafusion_python_util::{ffi_logical_codec_from_pycapsule, get_tokio_runtime};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

#[derive(Debug)]
pub(crate) struct ForeignScanExec {
    input: Arc<dyn ExecutionPlan>,
    projection: Option<Vec<usize>>,
}

impl ForeignScanExec {
    pub(crate) fn try_new(projection: Option<Vec<usize>>) -> Result<Self> {
        let (schema, batch) = ForeignTable::data()?;
        let input = datafusion::datasource::memory::MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            schema,
            projection.clone(),
        )?;
        Ok(Self { input, projection })
    }

    pub(crate) fn projection(&self) -> Option<&[usize]> {
        self.projection.as_deref()
    }
}

impl DisplayAs for ForeignScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ForeignScanExec")
    }
}

impl ExecutionPlan for ForeignScanExec {
    fn name(&self) -> &str {
        "ForeignScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return datafusion::common::plan_err!("ForeignScanExec is a leaf plan");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }
}

#[derive(Debug)]
pub(crate) struct ForeignTable;

impl ForeignTable {
    pub(crate) fn try_new() -> Result<Self> {
        Ok(Self)
    }

    fn data() -> Result<(SchemaRef, RecordBatch)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![Some(10), None, Some(30), Some(20)])),
                Arc::new(StringArray::from(vec!["a", "a", "b", "b"])),
            ],
        )?;
        Ok((schema, batch))
    }
}

#[async_trait]
impl TableProvider for ForeignTable {
    fn schema(&self) -> SchemaRef {
        ForeignTable::data()
            .expect("static foreign table data is valid")
            .0
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let _ = (session, filters, limit);
        Ok(Arc::new(ForeignScanExec::try_new(projection.cloned())?))
    }
}

#[pyclass(from_py_object, module = "datafusion_distributed_ffi_test")]
#[derive(Clone, Default)]
pub(crate) struct ForeignTableProvider;

#[pymethods]
impl ForeignTableProvider {
    #[new]
    fn new() -> Self {
        Self
    }

    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = ForeignTable::try_new()
            .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
        let runtime = get_tokio_runtime().handle().clone();
        let provider =
            FFI_TableProvider::new_with_ffi_codec(Arc::new(provider), false, Some(runtime), codec);
        PyCapsule::new(py, provider, Some(cr"datafusion_table_provider".into()))
    }
}
