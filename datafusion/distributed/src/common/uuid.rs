use datafusion::common::Result;
use datafusion_proto::protobuf::proto_error;
use uuid::Uuid;

pub(crate) fn deserialize_uuid(id: &[u8]) -> Result<Uuid> {
    Uuid::from_slice(id).map_err(|err| proto_error(format!("Invalid Uuid bytes: {err}")))
}

pub(crate) fn serialize_uuid(id: &Uuid) -> Vec<u8> {
    id.as_bytes().to_vec()
}
