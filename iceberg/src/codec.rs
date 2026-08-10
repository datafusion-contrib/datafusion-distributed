use crate::work_unit_feed::FileScanTaskMessage;
use bytes::{Buf, BufMut};
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use prost::encoding::{DecodeContext, WireType};
use prost::{DecodeError, Message};
use std::sync::Arc;

#[derive(Debug)]
pub struct IcebergCodec;

// TODO: Implement protobuf codecs for the IcebergDataSource.
impl PhysicalExtensionCodec for IcebergCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn try_encode(&self, _node: Arc<dyn ExecutionPlan>, _buf: &mut Vec<u8>) -> Result<()> {
        unimplemented!()
    }
}

// TODO: Implement serde for FileScanTaskMessage
//  This message is an individual WorkUnit, but it cannot be serialized yet. During distributed
//  execution, this will be streamed over the wire from coordinator to workers, but for that to
//  happen, it will need to be represented as a prost::Message.
//
// WARNING: for the ones who end up implementing this. I have no idea if implementing Message here
//  is really the best option for serialization, it might not be.
impl Message for FileScanTaskMessage {
    fn encode_raw(&self, _buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> std::result::Result<(), DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn encoded_len(&self) -> usize {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}
