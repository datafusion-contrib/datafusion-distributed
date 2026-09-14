//! Protobuf bridge for expressions created by the installed `datafusion` module.
//!
//! Its `PyExpr` class has a different PyO3 identity from the one linked into this
//! extension, so it cannot be passed to a local `PyDataFrame` directly. Expressions
//! have a stable protobuf representation, allowing conversion without exposing a
//! second public expression API.

use datafusion_python::context::PySessionContext;
use datafusion_python::expr::PyExpr;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Decode an expression produced by the separately loaded datafusion-python module.
#[pyfunction]
pub(crate) fn deserialize_expression(
    context: &Bound<'_, PySessionContext>,
    expression: Bound<'_, PyBytes>,
) -> PyResult<PyExpr> {
    Ok(PyExpr::from_bytes(context.borrow().clone(), expression)?)
}

/// Encode an expression owned by this extension's datafusion-python module.
#[pyfunction]
pub(crate) fn serialize_expression<'py>(
    py: Python<'py>,
    context: &Bound<'_, PySessionContext>,
    expression: PyExpr,
) -> PyResult<Py<PyBytes>> {
    Ok(expression
        .to_bytes(py, Some(context.borrow().clone()))?
        .unbind())
}
