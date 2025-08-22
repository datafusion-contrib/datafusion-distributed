use datafusion::common::not_impl_err;
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::fmt::Debug;
use std::sync::Arc;

// Idea taken from
// https://github.com/apache/datafusion/blob/0eebc0c7c0ffcd1514f5c6d0f8e2b6d0c69a07f5/datafusion-examples/examples/composed_extension_codec.rs#L236-L291

/// A [PhysicalExtensionCodec] that holds multiple [PhysicalExtensionCodec] and tries them
/// sequentially until one works.
#[derive(Debug, Clone, Default)]
pub(crate) struct ComposedPhysicalExtensionCodec {
    codecs: Vec<Arc<dyn PhysicalExtensionCodec>>,
}

impl ComposedPhysicalExtensionCodec {
    /// Adds a new [PhysicalExtensionCodec] to the list. These codecs will be tried
    /// sequentially until one works.
    pub(crate) fn push(&mut self, codec: impl PhysicalExtensionCodec + 'static) {
        self.codecs.push(Arc::new(codec));
    }

    /// Adds a new [PhysicalExtensionCodec] to the list. These codecs will be tried
    /// sequentially until one works.
    pub(crate) fn push_arc(&mut self, codec: Arc<dyn PhysicalExtensionCodec>) {
        self.codecs.push(codec);
    }

    fn try_any<T>(
        &self,
        mut f: impl FnMut(&dyn PhysicalExtensionCodec) -> Result<T, DataFusionError>,
    ) -> Result<T, DataFusionError> {
        let mut errs = vec![];
        for codec in &self.codecs {
            match f(codec.as_ref()) {
                Ok(node) => return Ok(node),
                Err(err) => errs.push(err),
            }
        }

        if errs.is_empty() {
            return not_impl_err!("Empty list of composed codecs");
        }

        let mut msg = "None of the provided PhysicalExtensionCodec worked:".to_string();
        for err in &errs {
            msg += &format!("\n    {err}");
        }
        not_impl_err!("{msg}")
    }
}

impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.try_any(|codec| codec.try_decode(buf, inputs, registry))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode(node.clone(), buf))
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>, DataFusionError> {
        self.try_any(|codec| codec.try_decode_udf(name, buf))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode_udf(node, buf))
    }

    fn try_decode_udaf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<AggregateUDF>, DataFusionError> {
        self.try_any(|codec| codec.try_decode_udaf(name, buf))
    }

    fn try_encode_udaf(
        &self,
        node: &AggregateUDF,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode_udaf(node, buf))
    }
}
