mod aggregate_udf;
mod extension_codec;
mod scalar_udf;
mod table_provider;
mod window_udf;

use aggregate_udf::ForeignSumUDF;
use extension_codec::ForeignExtensionCodec;
use pyo3::prelude::*;
use scalar_udf::ForeignIsNullUDF;
use table_provider::ForeignTableProvider;
use window_udf::ForeignRankUDF;

#[pymodule]
fn datafusion_distributed_ffi_test(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();
    m.add_class::<ForeignIsNullUDF>()?;
    m.add_class::<ForeignSumUDF>()?;
    m.add_class::<ForeignRankUDF>()?;
    m.add_class::<ForeignTableProvider>()?;
    m.add_class::<ForeignExtensionCodec>()?;
    Ok(())
}
