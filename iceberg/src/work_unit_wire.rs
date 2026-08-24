use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::{Result, exec_datafusion_err, exec_err};
use iceberg::expr::BoundPredicate;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataFileFormat, Datum, Literal, NameMapping, PartitionSpec, PrimitiveLiteral, PrimitiveType,
    Schema, SchemaRef, Struct, Type,
};
use prost::Message;
use serde::{Deserialize, Serialize};

/// Wire representation of one Iceberg file scan task.
#[derive(Clone, PartialEq, Message)]
pub struct FileScanTaskMessage {
    #[prost(bytes = "vec", tag = "1")]
    payload: Vec<u8>,
}

impl FileScanTaskMessage {
    fn from_wire(wire: &FileScanTaskWire) -> Result<Self> {
        let payload = rmp_serde::to_vec_named(wire).map_err(|error| {
            exec_datafusion_err!("failed to serialize Iceberg file scan task: {error}")
        })?;
        Ok(Self { payload })
    }

    fn into_wire(self) -> Result<FileScanTaskWire> {
        rmp_serde::from_slice(&self.payload).map_err(|error| {
            exec_datafusion_err!("failed to deserialize Iceberg file scan task: {error}")
        })
    }
}

#[derive(Serialize, Deserialize)]
struct FileScanTaskWire {
    context_id: u32,
    context: Option<SharedTaskContext>,
    body: FileScanTaskBody,
}

#[derive(PartialEq, Serialize, Deserialize)]
struct SharedTaskContext {
    schema: SchemaRef,
    project_field_ids: Vec<i32>,
    predicate: Option<BoundPredicate>,
    partition_spec: Option<Arc<PartitionSpec>>,
    name_mapping: Option<Arc<NameMapping>>,
    case_sensitive: bool,
}

impl SharedTaskContext {
    fn from_task(task: &FileScanTask) -> Self {
        Self {
            schema: Arc::clone(&task.schema),
            project_field_ids: task.project_field_ids.clone(),
            predicate: task.predicate.clone(),
            partition_spec: task.partition_spec.clone(),
            name_mapping: task.name_mapping.clone(),
            case_sensitive: task.case_sensitive,
        }
    }

    fn matches(&self, task: &FileScanTask) -> bool {
        self.schema == task.schema
            && self.project_field_ids == task.project_field_ids
            && self.predicate == task.predicate
            && self.partition_spec == task.partition_spec
            && self.name_mapping == task.name_mapping
            && self.case_sensitive == task.case_sensitive
    }

    fn validate(mut self) -> Result<Self> {
        for field_id in &self.project_field_ids {
            if self.schema.field_by_id(*field_id).is_none() {
                return exec_err!("Iceberg work unit projects unknown field id {field_id}");
            }
        }
        self.partition_spec = self
            .partition_spec
            .take()
            .map(|spec| {
                Arc::unwrap_or_clone(spec)
                    .into_unbound()
                    .bind(Arc::clone(&self.schema))
                    .map(Arc::new)
                    .map_err(|error| {
                        exec_datafusion_err!("invalid Iceberg work unit partition spec: {error}")
                    })
            })
            .transpose()?;
        Ok(self)
    }
}

#[derive(Serialize, Deserialize)]
struct FileScanTaskBody {
    file_size_in_bytes: u64,
    start: u64,
    length: u64,
    record_count: Option<u64>,
    data_file_path: String,
    data_file_format: DataFileFormat,
    deletes: Vec<FileScanTaskDeleteFile>,
    partition: Option<Vec<Option<Vec<u8>>>>,
}

impl FileScanTaskBody {
    fn try_from_task(mut task: FileScanTask) -> Result<Self> {
        validate_file_range(task.file_size_in_bytes, task.start, task.length)?;
        let partition = partition_to_wire(
            task.partition.take(),
            task.partition_spec.as_deref(),
            &task.schema,
        )?;
        Ok(Self {
            file_size_in_bytes: task.file_size_in_bytes,
            start: task.start,
            length: task.length,
            record_count: task.record_count,
            data_file_path: task.data_file_path,
            data_file_format: task.data_file_format,
            deletes: task.deletes,
            partition,
        })
    }

    fn into_task(self, context: &SharedTaskContext) -> Result<FileScanTask> {
        validate_file_range(self.file_size_in_bytes, self.start, self.length)?;
        let partition = partition_from_wire(
            self.partition,
            context.partition_spec.as_deref(),
            &context.schema,
        )?;
        Ok(FileScanTask {
            file_size_in_bytes: self.file_size_in_bytes,
            start: self.start,
            length: self.length,
            record_count: self.record_count,
            data_file_path: self.data_file_path,
            data_file_format: self.data_file_format,
            schema: Arc::clone(&context.schema),
            project_field_ids: context.project_field_ids.clone(),
            predicate: context.predicate.clone(),
            deletes: self.deletes,
            partition,
            partition_spec: context.partition_spec.clone(),
            name_mapping: context.name_mapping.clone(),
            case_sensitive: context.case_sensitive,
        })
    }
}

/// Encodes tasks while defining shared serde context only once per feed.
#[derive(Default)]
pub(crate) struct FileScanTaskEncoder {
    contexts: Vec<SharedTaskContext>,
}

impl FileScanTaskEncoder {
    pub(crate) fn encode(&mut self, task: FileScanTask) -> Result<FileScanTaskMessage> {
        let existing = self
            .contexts
            .iter()
            .position(|context| context.matches(&task));
        let (context_id, context) = match existing {
            Some(index) => (context_id(index)?, None),
            None => (
                context_id(self.contexts.len())?,
                Some(SharedTaskContext::from_task(&task)),
            ),
        };
        let mut wire = FileScanTaskWire {
            context_id,
            context,
            body: FileScanTaskBody::try_from_task(task)?,
        };
        let message = FileScanTaskMessage::from_wire(&wire)?;
        if let Some(context) = wire.context.take() {
            self.contexts.push(context);
        }
        Ok(message)
    }
}

/// Decodes a single feed, retaining shared serde contexts referenced by later tasks.
#[derive(Default)]
pub(crate) struct FileScanTaskDecoder {
    contexts: HashMap<u32, SharedTaskContext>,
}

impl FileScanTaskDecoder {
    pub(crate) fn decode(&mut self, message: FileScanTaskMessage) -> Result<FileScanTask> {
        let wire = message.into_wire()?;
        if wire.context_id == 0 {
            return exec_err!("Iceberg work unit context id must be non-zero");
        }
        let definition = wire.context.map(SharedTaskContext::validate).transpose()?;
        if let (Some(existing), Some(definition)) =
            (self.contexts.get(&wire.context_id), definition.as_ref())
            && existing != definition
        {
            return exec_err!(
                "Iceberg work unit context id {} was redefined",
                wire.context_id
            );
        }
        let Some(context) = definition
            .as_ref()
            .or_else(|| self.contexts.get(&wire.context_id))
        else {
            return exec_err!(
                "Iceberg work unit references undefined context id {}",
                wire.context_id
            );
        };

        let task = wire.body.into_task(context)?;
        if let Some(definition) = definition {
            self.contexts.entry(wire.context_id).or_insert(definition);
        }
        Ok(task)
    }
}

fn context_id(index: usize) -> Result<u32> {
    u32::try_from(index)
        .ok()
        .and_then(|index| index.checked_add(1))
        .ok_or_else(|| exec_datafusion_err!("too many Iceberg task contexts in one feed"))
}

fn validate_file_range(file_size: u64, start: u64, length: u64) -> Result<()> {
    let Some(end) = start.checked_add(length) else {
        return exec_err!("Iceberg work unit file range overflows u64");
    };
    if end > file_size {
        return exec_err!(
            "Iceberg work unit file range {start}..{end} exceeds file size {file_size}"
        );
    }
    Ok(())
}

fn partition_to_wire(
    partition: Option<Struct>,
    spec: Option<&PartitionSpec>,
    schema: &Schema,
) -> Result<Option<Vec<Option<Vec<u8>>>>> {
    // iceberg-rust 0.10 does not propagate the manifest partition spec into planned tasks.
    // Omit values that cannot be interpreted without their matching type metadata.
    let Some(spec) = spec else {
        return Ok(None);
    };
    let Some(partition) = partition else {
        return exec_err!("Iceberg work unit has a partition spec without partition values");
    };
    let partition_type = spec.partition_type(schema).map_err(|error| {
        exec_datafusion_err!("invalid Iceberg work unit partition spec: {error}")
    })?;
    if partition.iter().len() != partition_type.fields().len() {
        return exec_err!(
            "Iceberg work unit has {} partition values for {} partition fields",
            partition.iter().len(),
            partition_type.fields().len()
        );
    }

    partition
        .into_iter()
        .zip(partition_type.fields())
        .enumerate()
        .map(|(index, (value, field))| {
            let Some(value) = value else {
                return Ok(None);
            };
            let Literal::Primitive(value) = value else {
                return exec_err!("Iceberg partition value {index} is not primitive");
            };
            let Type::Primitive(data_type) = field.field_type.as_ref() else {
                return exec_err!("Iceberg partition field {index} is not primitive");
            };
            if !data_type.compatible(&value) {
                return exec_err!(
                    "Iceberg partition value {index} is incompatible with type {data_type}"
                );
            }
            primitive_literal_bytes(value).map(Some)
        })
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn partition_from_wire(
    partition: Option<Vec<Option<Vec<u8>>>>,
    spec: Option<&PartitionSpec>,
    schema: &Schema,
) -> Result<Option<Struct>> {
    let (partition, spec) = match (partition, spec) {
        (None, None) => return Ok(None),
        (Some(partition), Some(spec)) => (partition, spec),
        (Some(_), None) => {
            return exec_err!("Iceberg work unit has partition values without a partition spec");
        }
        (None, Some(_)) => {
            return exec_err!("Iceberg work unit has a partition spec without partition values");
        }
    };
    let partition_type = spec.partition_type(schema).map_err(|error| {
        exec_datafusion_err!("invalid Iceberg work unit partition spec: {error}")
    })?;
    if partition.len() != partition_type.fields().len() {
        return exec_err!(
            "Iceberg work unit has {} partition values for {} partition fields",
            partition.len(),
            partition_type.fields().len()
        );
    }

    partition
        .into_iter()
        .zip(partition_type.fields())
        .enumerate()
        .map(|(index, (value, field))| {
            let Some(value) = value else {
                return Ok(None);
            };
            let Type::Primitive(data_type) = field.field_type.as_ref() else {
                return exec_err!("Iceberg partition field {index} is not primitive");
            };
            if let PrimitiveType::Fixed(length) = data_type
                && usize::try_from(*length).ok() != Some(value.len())
            {
                return exec_err!(
                    "Iceberg partition value {index} has {} bytes for fixed[{length}]",
                    value.len()
                );
            }
            let datum = Datum::try_from_bytes(&value, data_type.clone()).map_err(|error| {
                exec_datafusion_err!("invalid Iceberg partition value {index}: {error}")
            })?;
            let literal = datum.literal().clone();
            if primitive_literal_bytes(literal.clone())? != value {
                return exec_err!("Iceberg partition value {index} has non-canonical bytes");
            }
            Ok(Some(Literal::Primitive(literal)))
        })
        .collect::<Result<Struct>>()
        .map(Some)
}

fn primitive_literal_bytes(literal: PrimitiveLiteral) -> Result<Vec<u8>> {
    Ok(match literal {
        PrimitiveLiteral::Boolean(value) => vec![u8::from(value)],
        PrimitiveLiteral::Int(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::Long(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::Float(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::Double(value) => value.to_le_bytes().to_vec(),
        PrimitiveLiteral::String(value) => value.into_bytes(),
        PrimitiveLiteral::Binary(value) => value,
        PrimitiveLiteral::Int128(value) => value.to_be_bytes().to_vec(),
        PrimitiveLiteral::UInt128(value) => value.to_be_bytes().to_vec(),
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
            return exec_err!("Iceberg partition values cannot be range sentinels");
        }
    })
}

#[cfg(test)]
include!("work_unit_wire_tests.rs");
