use std::any::TypeId;
use std::sync::Arc;

use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

#[derive(Clone)]
struct RegisteredCodec {
    type_id: TypeId,
    codec: Arc<dyn PhysicalExtensionCodec>,
}

pub struct UserProvidedCodecs(Vec<RegisteredCodec>);

pub(crate) fn set_distributed_user_codec_arc(
    cfg: &mut SessionConfig,
    codec: Arc<dyn PhysicalExtensionCodec>,
) {
    let mut codecs = cfg
        .get_extension::<UserProvidedCodecs>()
        .map(|previous| previous.0.clone())
        .unwrap_or_default();
    let type_id = codec.as_ref().type_id();
    if let Some(previous) = codecs.iter_mut().find(|entry| entry.type_id == type_id) {
        previous.codec = codec;
    } else {
        codecs.push(RegisteredCodec { type_id, codec });
    }
    cfg.set_extension(Arc::new(UserProvidedCodecs(codecs)))
}

pub(crate) fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(
    cfg: &mut SessionConfig,
    codec: T,
) {
    set_distributed_user_codec_arc(cfg, Arc::new(codec))
}

pub(crate) fn get_distributed_user_codecs(
    cfg: &SessionConfig,
) -> Vec<Arc<dyn PhysicalExtensionCodec>> {
    match cfg.get_extension::<UserProvidedCodecs>() {
        None => vec![],
        Some(value) => value
            .0
            .iter()
            .map(|entry| Arc::clone(&entry.codec))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
    use datafusion::common::{Result, not_impl_err};
    use datafusion::execution::TaskContext;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;
    use datafusion_proto::protobuf::proto_error;

    use crate::{DistributedCodec, DistributedExt};

    use super::*;

    #[derive(Debug)]
    struct FirstCodec(u8);

    #[derive(Debug)]
    struct SecondCodec;

    impl PhysicalExtensionCodec for FirstCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            decode_empty(buf, self.0, "first")
        }

        fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
            encode_empty(node, buf, self.0, "first")
        }
    }

    impl PhysicalExtensionCodec for SecondCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            decode_empty(buf, 2, "second")
        }

        fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> Result<()> {
            encode_empty(node, buf, 2, "second")
        }
    }

    #[test]
    fn same_type_registration_replaces_in_place() -> Result<()> {
        let mut coordinator = SessionConfig::new();
        coordinator.set_distributed_user_codec(FirstCodec(1));
        coordinator.set_distributed_user_codec(SecondCodec);
        coordinator.set_distributed_user_codec(FirstCodec(3));

        let codecs = get_distributed_user_codecs(&coordinator);
        assert_eq!(codecs.len(), 2);
        assert_eq!(codecs[0].as_ref().type_id(), TypeId::of::<FirstCodec>());
        assert_eq!(codecs[1].as_ref().type_id(), TypeId::of::<SecondCodec>());

        let mut worker = SessionConfig::new();
        worker.set_distributed_user_codec(FirstCodec(3));
        worker.set_distributed_user_codec(SecondCodec);
        assert_composed_roundtrip(&coordinator, &worker, "first")?;
        assert_composed_roundtrip(&coordinator, &worker, "second")
    }

    fn assert_composed_roundtrip(
        coordinator: &SessionConfig,
        worker: &SessionConfig,
        field_name: &str,
    ) -> Result<()> {
        let plan: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema(field_name)));
        let mut bytes = Vec::new();
        DistributedCodec::new_combined_with_user(coordinator).try_encode(plan, &mut bytes)?;
        let decoded = DistributedCodec::new_combined_with_user(worker).try_decode(
            &bytes,
            &[],
            &SessionContext::new().task_ctx(),
        )?;
        assert_eq!(decoded.schema(), schema(field_name));
        Ok(())
    }

    fn encode_empty(
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        marker: u8,
        field_name: &str,
    ) -> Result<()> {
        if node.downcast_ref::<EmptyExec>().is_none() || node.schema() != schema(field_name) {
            return not_impl_err!("codec does not support this plan");
        }
        buf.push(marker);
        Ok(())
    }

    fn decode_empty(buf: &[u8], marker: u8, field_name: &str) -> Result<Arc<dyn ExecutionPlan>> {
        if buf != [marker] {
            return Err(proto_error("unexpected test codec marker"));
        }
        Ok(Arc::new(EmptyExec::new(schema(field_name))))
    }

    fn schema(field_name: &str) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            field_name,
            datafusion::arrow::datatypes::DataType::Null,
            true,
        )]))
    }
}
