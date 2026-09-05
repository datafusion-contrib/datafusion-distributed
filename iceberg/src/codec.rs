use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
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
