//! Registration bridge for UDFs created by the installed `datafusion` module.
//!
//! Scalar, aggregate, and window UDF wrappers contain native expression classes
//! owned by that module. Python serializes the corresponding invocation expression,
//! and these functions deserialize it against the local session before registering
//! the embedded function definition. This avoids re-exposing the UDF constructors.
//! UDTFs cannot use this mechanism because they may materialize arbitrary table
//! providers and have no equivalent serialization or FFI contract.

use datafusion::logical_expr::{Expr, WindowFunctionDefinition};
use datafusion_python::context::PySessionContext;
use datafusion_python::expr::PyExpr;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

fn deserialize_expression(
    context: &Bound<'_, PySessionContext>,
    expression: Bound<'_, PyBytes>,
) -> PyResult<Expr> {
    Ok(PyExpr::from_bytes(context.borrow().clone(), expression)?.expr)
}

#[pyfunction]
pub(crate) fn register_scalar_udf(
    context: &Bound<'_, PySessionContext>,
    expression: Bound<'_, PyBytes>,
) -> PyResult<()> {
    let Expr::ScalarFunction(function) = deserialize_expression(context, expression)? else {
        return Err(PyTypeError::new_err(
            "expected an expression containing a scalar UDF",
        ));
    };
    context
        .borrow()
        .ctx
        .register_udf(function.func.as_ref().clone());
    Ok(())
}

#[pyfunction]
pub(crate) fn register_aggregate_udf(
    context: &Bound<'_, PySessionContext>,
    expression: Bound<'_, PyBytes>,
) -> PyResult<()> {
    let Expr::AggregateFunction(function) = deserialize_expression(context, expression)? else {
        return Err(PyTypeError::new_err(
            "expected an expression containing an aggregate UDF",
        ));
    };
    context
        .borrow()
        .ctx
        .register_udaf(function.func.as_ref().clone());
    Ok(())
}

#[pyfunction]
pub(crate) fn register_window_udf(
    context: &Bound<'_, PySessionContext>,
    expression: Bound<'_, PyBytes>,
) -> PyResult<()> {
    let Expr::WindowFunction(function) = deserialize_expression(context, expression)? else {
        return Err(PyTypeError::new_err(
            "expected an expression containing a window UDF",
        ));
    };
    let WindowFunctionDefinition::WindowUDF(function) = function.fun else {
        return Err(PyTypeError::new_err(
            "expected an expression containing a window UDF",
        ));
    };
    context
        .borrow()
        .ctx
        .register_udwf(function.as_ref().clone());
    Ok(())
}
