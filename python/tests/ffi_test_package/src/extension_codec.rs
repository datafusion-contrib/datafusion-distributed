use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::{Result, TableReference, not_impl_err, plan_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{AggregateUDF, Extension, LogicalPlan, ScalarUDF, WindowUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ffi::execution::FFI_TaskContextProvider;
use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_python_util::get_tokio_runtime;
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

use crate::aggregate_udf::ForeignSumUDF;
use crate::scalar_udf::ForeignIsNullUDF;
use crate::table_provider::{ForeignScanExec, ForeignTable};
use crate::window_udf::ForeignRankUDF;

const TABLE_TAG: &[u8] = b"df-third-party-table-v1";
const EXEC_TAG: &[u8] = b"df-third-party-exec-v1";
const SCALAR_TAG: &[u8] = b"df-third-party-scalar-v1";
const AGGREGATE_TAG: &[u8] = b"df-third-party-aggregate-v1";
const WINDOW_TAG: &[u8] = b"df-third-party-window-v1";

#[derive(Default)]
struct Counters {
    logical_provider_encode: AtomicUsize,
    logical_provider_decode: AtomicUsize,
    physical_plan_encode: AtomicUsize,
    physical_plan_decode: AtomicUsize,
    udf_encode: AtomicUsize,
    udf_decode: AtomicUsize,
}

struct ForeignLogicalCodec {
    counters: Arc<Counters>,
    _session: Py<PyAny>,
}

impl std::fmt::Debug for ForeignLogicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForeignLogicalCodec")
            .finish_non_exhaustive()
    }
}

impl LogicalExtensionCodec for ForeignLogicalCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[LogicalPlan],
        _ctx: &TaskContext,
    ) -> Result<Extension> {
        not_impl_err!("third-party logical extension node is unsupported")
    }

    fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
        not_impl_err!("third-party logical extension node is unsupported")
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        _table_ref: &TableReference,
        _schema: SchemaRef,
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn TableProvider>> {
        if buf != TABLE_TAG {
            return plan_err!("unknown third-party table-provider payload");
        }
        self.counters
            .logical_provider_decode
            .fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ForeignTable::try_new()?))
    }

    fn try_encode_table_provider(
        &self,
        _table_ref: &TableReference,
        node: Arc<dyn TableProvider>,
        buf: &mut Vec<u8>,
    ) -> Result<()> {
        if !node.is::<ForeignTable>() {
            return plan_err!("not a third-party table provider");
        }
        self.counters
            .logical_provider_encode
            .fetch_add(1, Ordering::SeqCst);
        buf.extend_from_slice(TABLE_TAG);
        Ok(())
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name != "foreign_is_null" || buf != SCALAR_TAG {
            return plan_err!("unknown third-party scalar UDF payload");
        }
        self.counters.udf_decode.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ScalarUDF::from(ForeignIsNullUDF::new())))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if !node.inner().is::<ForeignIsNullUDF>() {
            return plan_err!("not a third-party scalar UDF");
        }
        self.counters.udf_encode.fetch_add(1, Ordering::SeqCst);
        buf.extend_from_slice(SCALAR_TAG);
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if name != "foreign_sum" || buf != AGGREGATE_TAG {
            return plan_err!("unknown third-party aggregate UDF payload");
        }
        self.counters.udf_decode.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(AggregateUDF::from(ForeignSumUDF::new())))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        if !node.inner().is::<ForeignSumUDF>() {
            return plan_err!("not a third-party aggregate UDF");
        }
        self.counters.udf_encode.fetch_add(1, Ordering::SeqCst);
        buf.extend_from_slice(AGGREGATE_TAG);
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if name != "foreign_rank" || buf != WINDOW_TAG {
            return plan_err!("unknown third-party window UDF payload");
        }
        self.counters.udf_decode.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(WindowUDF::from(ForeignRankUDF::new())))
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        if !node.inner().is::<ForeignRankUDF>() {
            return plan_err!("not a third-party window UDF");
        }
        self.counters.udf_encode.fetch_add(1, Ordering::SeqCst);
        buf.extend_from_slice(WINDOW_TAG);
        Ok(())
    }
}

struct ForeignPhysicalCodec {
    counters: Arc<Counters>,
    _session: Py<PyAny>,
}

impl std::fmt::Debug for ForeignPhysicalCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForeignPhysicalCodec")
            .finish_non_exhaustive()
    }
}

impl PhysicalExtensionCodec for ForeignPhysicalCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(projection) = buf.strip_prefix(EXEC_TAG) {
            if !inputs.is_empty() {
                return plan_err!("ForeignScanExec is a leaf plan");
            }
            let projection = match projection {
                [u8::MAX] => None,
                [len, indices @ ..] if indices.len() == *len as usize => {
                    Some(indices.iter().map(|index| *index as usize).collect())
                }
                _ => return plan_err!("invalid third-party execution-plan payload"),
            };
            self.counters
                .physical_plan_decode
                .fetch_add(1, Ordering::SeqCst);
            return Ok(Arc::new(ForeignScanExec::try_new(projection)?));
        }
        plan_err!("unknown third-party execution-plan payload")
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        self.counters
            .physical_plan_encode
            .fetch_add(1, Ordering::SeqCst);
        if let Some(node) = node.downcast_ref::<ForeignScanExec>() {
            buf.extend_from_slice(EXEC_TAG);
            match node.projection() {
                None => buf.push(u8::MAX),
                Some(projection) => {
                    let length = u8::try_from(projection.len()).map_err(|_| {
                        datafusion::common::DataFusionError::Plan(
                            "ForeignScanExec projection is too large".to_string(),
                        )
                    })?;
                    buf.push(length);
                    for index in projection {
                        buf.push(u8::try_from(*index).map_err(|_| {
                            datafusion::common::DataFusionError::Plan(
                                "ForeignScanExec projection index is too large".to_string(),
                            )
                        })?);
                    }
                }
            }
            return Ok(());
        }

        plan_err!("not a third-party execution plan")
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        if name != "foreign_is_null" || buf != SCALAR_TAG {
            return plan_err!("unknown third-party scalar UDF payload");
        }
        self.counters.udf_decode.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(ScalarUDF::from(ForeignIsNullUDF::new())))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        if !node.inner().is::<ForeignIsNullUDF>() {
            return plan_err!("not a third-party scalar UDF");
        }
        self.counters.udf_encode.fetch_add(1, Ordering::SeqCst);
        buf.extend_from_slice(SCALAR_TAG);
        Ok(())
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        if name != "foreign_sum" || buf != AGGREGATE_TAG {
            return plan_err!("unknown third-party aggregate UDF payload");
        }
        self.counters.udf_decode.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(AggregateUDF::from(ForeignSumUDF::new())))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        if !node.inner().is::<ForeignSumUDF>() {
            return plan_err!("not a third-party aggregate UDF");
        }
        self.counters.udf_encode.fetch_add(1, Ordering::SeqCst);
        buf.extend_from_slice(AGGREGATE_TAG);
        Ok(())
    }

    fn try_decode_udwf(&self, name: &str, buf: &[u8]) -> Result<Arc<WindowUDF>> {
        if name != "foreign_rank" || buf != WINDOW_TAG {
            return plan_err!("unknown third-party window UDF payload");
        }
        self.counters.udf_decode.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(WindowUDF::from(ForeignRankUDF::new())))
    }

    fn try_encode_udwf(&self, node: &WindowUDF, buf: &mut Vec<u8>) -> Result<()> {
        if !node.inner().is::<ForeignRankUDF>() {
            return plan_err!("not a third-party window UDF");
        }
        self.counters.udf_encode.fetch_add(1, Ordering::SeqCst);
        buf.extend_from_slice(WINDOW_TAG);
        Ok(())
    }
}

fn task_ctx_provider_from_python(obj: &Bound<'_, PyAny>) -> PyResult<FFI_TaskContextProvider> {
    let capsule = obj
        .getattr("__datafusion_task_context_provider__")?
        .call0()?;
    let capsule = capsule.cast::<PyCapsule>()?;
    let data: NonNull<FFI_TaskContextProvider> = capsule
        .pointer_checked(Some(c"datafusion_task_context_provider"))?
        .cast();
    Ok(unsafe { data.as_ref().clone() })
}

#[pyclass(module = "datafusion_distributed_ffi_test")]
pub(crate) struct ForeignExtensionCodec {
    counters: Arc<Counters>,
    session: Py<PyAny>,
    task_ctx_provider: FFI_TaskContextProvider,
}

#[pymethods]
impl ForeignExtensionCodec {
    #[new]
    fn new(session: Bound<'_, PyAny>) -> PyResult<Self> {
        let task_ctx_provider = task_ctx_provider_from_python(&session)?;
        Ok(Self {
            counters: Arc::new(Counters::default()),
            session: session.unbind(),
            task_ctx_provider,
        })
    }

    fn logical_provider_encode_calls(&self) -> usize {
        self.counters.logical_provider_encode.load(Ordering::SeqCst)
    }

    fn logical_provider_decode_calls(&self) -> usize {
        self.counters.logical_provider_decode.load(Ordering::SeqCst)
    }

    fn physical_plan_encode_calls(&self) -> usize {
        self.counters.physical_plan_encode.load(Ordering::SeqCst)
    }

    fn physical_plan_decode_calls(&self) -> usize {
        self.counters.physical_plan_decode.load(Ordering::SeqCst)
    }

    fn udf_encode_calls(&self) -> usize {
        self.counters.udf_encode.load(Ordering::SeqCst)
    }

    fn udf_decode_calls(&self) -> usize {
        self.counters.udf_decode.load(Ordering::SeqCst)
    }

    fn __datafusion_logical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let codec: Arc<dyn LogicalExtensionCodec> = Arc::new(ForeignLogicalCodec {
            counters: Arc::clone(&self.counters),
            _session: self.session.clone_ref(py),
        });
        let ffi = FFI_LogicalExtensionCodec::new(
            codec,
            Some(get_tokio_runtime().handle().clone()),
            self.task_ctx_provider.clone(),
        );
        PyCapsule::new(py, ffi, Some(cr"datafusion_logical_extension_codec".into()))
    }

    fn __datafusion_physical_extension_codec__<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let codec: Arc<dyn PhysicalExtensionCodec + Send> = Arc::new(ForeignPhysicalCodec {
            counters: Arc::clone(&self.counters),
            _session: self.session.clone_ref(py),
        });
        let ffi = FFI_PhysicalExtensionCodec::new(
            codec,
            Some(get_tokio_runtime().handle().clone()),
            self.task_ctx_provider.clone(),
        );
        PyCapsule::new(
            py,
            ffi,
            Some(cr"datafusion_physical_extension_codec".into()),
        )
    }
}
