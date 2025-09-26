use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::Message;
use std::fmt::Debug;
use std::sync::Arc;
// Code taken from https://github.com/apache/datafusion/blob/10f41887fa40d7d425c19b07857f80115460a98e/datafusion/proto/src/physical_plan/mod.rs
// TODO: It's not yet on DF 49, once upgrading to DF 50 we can remove this

/// DataEncoderTuple captures the position of the encoder
/// in the codec list that was used to encode the data and actual encoded data
#[derive(Clone, PartialEq, prost::Message)]
struct DataEncoderTuple {
    /// The position of encoder used to encode data
    /// (to be used for decoding)
    #[prost(uint32, tag = 1)]
    pub encoder_position: u32,

    #[prost(bytes, tag = 2)]
    pub blob: Vec<u8>,
}

/// A PhysicalExtensionCodec that tries one of multiple inner codecs
/// until one works
#[derive(Debug)]
pub struct ComposedPhysicalExtensionCodec {
    codecs: Vec<Arc<dyn PhysicalExtensionCodec>>,
}

impl ComposedPhysicalExtensionCodec {
    // Position in this codecs list is important as it will be used for decoding.
    // If new codec is added it should go to last position.
    pub fn new(codecs: Vec<Arc<dyn PhysicalExtensionCodec>>) -> Self {
        Self { codecs }
    }

    fn decode_protobuf<R>(
        &self,
        buf: &[u8],
        decode: impl FnOnce(&dyn PhysicalExtensionCodec, &[u8]) -> Result<R, DataFusionError>,
    ) -> Result<R, DataFusionError> {
        let proto =
            DataEncoderTuple::decode(buf).map_err(|e| DataFusionError::Internal(e.to_string()))?;

        let pos = proto.encoder_position as usize;
        let codec = self.codecs.get(pos).ok_or_else(|| {
            internal_datafusion_err!(
                "Can't find required codec in position {pos} in codec list with {} elements",
                self.codecs.len()
            )
        })?;

        decode(codec.as_ref(), &proto.blob)
    }

    fn encode_protobuf(
        &self,
        buf: &mut Vec<u8>,
        mut encode: impl FnMut(&dyn PhysicalExtensionCodec, &mut Vec<u8>) -> Result<()>,
    ) -> Result<(), DataFusionError> {
        let mut data = vec![];
        let mut last_err = None;
        let mut encoder_position = None;

        // find the encoder
        for (position, codec) in self.codecs.iter().enumerate() {
            match encode(codec.as_ref(), &mut data) {
                Ok(_) => {
                    encoder_position = Some(position as u32);
                    break;
                }
                Err(err) => last_err = Some(err),
            }
        }

        let encoder_position = encoder_position.ok_or_else(|| {
            last_err.unwrap_or_else(|| {
                DataFusionError::NotImplemented("Empty list of composed codecs".to_owned())
            })
        })?;

        // encode with encoder position
        let proto = DataEncoderTuple {
            encoder_position,
            blob: data,
        };
        proto
            .encode(buf)
            .map_err(|e| DataFusionError::Internal(e.to_string()))
    }
}

impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode(data, inputs, registry))
    }

    fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode(Arc::clone(&node), data))
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_udf(name, data))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode_udf(node, data))
    }

    fn try_decode_udaf(&self, name: &str, buf: &[u8]) -> Result<Arc<AggregateUDF>> {
        self.decode_protobuf(buf, |codec, data| codec.try_decode_udaf(name, data))
    }

    fn try_encode_udaf(&self, node: &AggregateUDF, buf: &mut Vec<u8>) -> Result<()> {
        self.encode_protobuf(buf, |codec, data| codec.try_encode_udaf(node, data))
    }
}
