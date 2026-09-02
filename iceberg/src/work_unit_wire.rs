use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Not;
use std::sync::Arc;

use bytes::{Buf, BufMut};
use datafusion::common::{Result, exec_datafusion_err, exec_err};
use iceberg::expr::{
    BinaryExpression, Bind, BoundPredicate, Predicate, PredicateOperator, Reference, SetExpression,
    UnaryExpression,
};
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataContentType, DataFileFormat, Datum, ListType, Literal, Map, MapType, MappedField,
    NameMapping, NestedField, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, SchemaRef,
    Struct, StructType, Transform, Type, UnboundPartitionField, UnboundPartitionSpec,
};
use prost::encoding::{DecodeContext, WireType};
use prost::{DecodeError, Message};

use crate::proto::generated as pb;

#[derive(Debug, Clone)]
enum FileScanTaskPayload {
    Native {
        task: FileScanTask,
        context_id: u32,
        include_context: bool,
    },
    Proto(pb::FileScanTask),
}

/// A file scan task that stays in memory until a transport serializes it.
#[derive(Debug, Clone)]
pub struct FileScanTaskMessage {
    payload: FileScanTaskPayload,
}

impl Default for FileScanTaskMessage {
    fn default() -> Self {
        Self {
            payload: FileScanTaskPayload::Proto(pb::FileScanTask::default()),
        }
    }
}

impl FileScanTaskMessage {
    fn new(task: FileScanTask, context_id: u32, include_context: bool) -> Self {
        Self {
            payload: FileScanTaskPayload::Native {
                task,
                context_id,
                include_context,
            },
        }
    }

    fn as_proto(&self) -> Cow<'_, pb::FileScanTask> {
        match &self.payload {
            FileScanTaskPayload::Native {
                task,
                context_id,
                include_context,
            } => Cow::Owned(
                task_to_proto(task, *context_id, *include_context)
                    .expect("FileScanTaskEncoder validates native tasks when constructed"),
            ),
            FileScanTaskPayload::Proto(proto) => Cow::Borrowed(proto),
        }
    }
}

/// Encodes tasks and reuses identical context within one feed.
#[derive(Default)]
pub(crate) struct FileScanTaskEncoder {
    contexts: Vec<TaskContext>,
}

impl FileScanTaskEncoder {
    pub(crate) fn encode(&mut self, task: FileScanTask) -> Result<FileScanTaskMessage> {
        validate_task(&task)?;
        let existing = self
            .contexts
            .iter()
            .position(|context| context.matches(&task));
        let (context_id, include_context) = match existing {
            Some(index) => (context_id(index)?, false),
            None => (context_id(self.contexts.len())?, true),
        };
        if include_context {
            self.contexts.push(TaskContext::from_task(&task));
        }
        Ok(FileScanTaskMessage::new(task, context_id, include_context))
    }
}

/// Decodes one feed while retaining context referenced by later tasks.
#[derive(Default)]
pub(crate) struct FileScanTaskDecoder {
    contexts: HashMap<u32, TaskContext>,
}

impl FileScanTaskDecoder {
    pub(crate) fn decode(&mut self, message: FileScanTaskMessage) -> Result<FileScanTask> {
        let proto = match message.payload {
            FileScanTaskPayload::Native { task, .. } => return Ok(task),
            FileScanTaskPayload::Proto(proto) => proto,
        };
        if proto.context_id == 0 {
            return exec_err!("Iceberg work unit context id must be non-zero");
        }
        let definition = proto.context.map(context_from_proto).transpose()?;
        if let (Some(existing), Some(definition)) =
            (self.contexts.get(&proto.context_id), definition.as_ref())
            && existing != definition
        {
            return exec_err!(
                "Iceberg work unit context id {} was redefined",
                proto.context_id
            );
        }
        let Some(context) = definition
            .as_ref()
            .or_else(|| self.contexts.get(&proto.context_id))
        else {
            return exec_err!(
                "Iceberg work unit references undefined context id {}",
                proto.context_id
            );
        };
        let task = body_from_proto(required(proto.task, "task")?, context)?;
        if let Some(definition) = definition {
            self.contexts.entry(proto.context_id).or_insert(definition);
        }
        Ok(task)
    }
}

impl Message for FileScanTaskMessage {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        self.as_proto().encode_raw(buf);
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> std::result::Result<(), DecodeError> {
        let FileScanTaskPayload::Proto(proto) = &mut self.payload else {
            self.payload = FileScanTaskPayload::Proto(pb::FileScanTask::default());
            let FileScanTaskPayload::Proto(proto) = &mut self.payload else {
                unreachable!()
            };
            return proto.merge_field(tag, wire_type, buf, ctx);
        };
        proto.merge_field(tag, wire_type, buf, ctx)
    }

    fn encoded_len(&self) -> usize {
        self.as_proto().encoded_len()
    }

    fn clear(&mut self) {
        self.payload = FileScanTaskPayload::Proto(pb::FileScanTask::default());
    }
}

fn validate_task(task: &FileScanTask) -> Result<()> {
    validate_file_range(task.file_size_in_bytes, task.start, task.length)?;
    validate_projected_fields(&task.schema, &task.project_field_ids)?;
    partition_to_proto(
        task.partition.as_ref(),
        task.partition_spec.as_deref(),
        &task.schema,
    )?;
    Ok(())
}

fn task_to_proto(
    task: &FileScanTask,
    context_id: u32,
    include_context: bool,
) -> Result<pb::FileScanTask> {
    Ok(pb::FileScanTask {
        context_id,
        context: include_context.then(|| context_to_proto(task)),
        task: Some(pb::FileScanTaskBody {
            file_size_in_bytes: task.file_size_in_bytes,
            start: task.start,
            length: task.length,
            record_count: task.record_count,
            data_file_path: task.data_file_path.clone(),
            data_file_format: data_file_format_to_proto(task.data_file_format) as i32,
            deletes: task.deletes.iter().map(delete_to_proto).collect(),
            partition: partition_to_proto(
                task.partition.as_ref(),
                task.partition_spec.as_deref(),
                &task.schema,
            )?,
        }),
    })
}

#[derive(Debug, PartialEq)]
struct TaskContext {
    schema: SchemaRef,
    projected_field_ids: Vec<i32>,
    predicate: Option<BoundPredicate>,
    partition_spec: Option<Arc<PartitionSpec>>,
    name_mapping: Option<Arc<NameMapping>>,
    case_sensitive: bool,
}

impl TaskContext {
    fn from_task(task: &FileScanTask) -> Self {
        Self {
            schema: Arc::clone(&task.schema),
            projected_field_ids: task.project_field_ids.clone(),
            predicate: task.predicate.clone(),
            partition_spec: task.partition_spec.clone(),
            name_mapping: task.name_mapping.clone(),
            case_sensitive: task.case_sensitive,
        }
    }

    fn matches(&self, task: &FileScanTask) -> bool {
        self.schema == task.schema
            && self.projected_field_ids == task.project_field_ids
            && self.predicate == task.predicate
            && self.partition_spec == task.partition_spec
            && self.name_mapping == task.name_mapping
            && self.case_sensitive == task.case_sensitive
    }
}

fn context_id(index: usize) -> Result<u32> {
    u32::try_from(index)
        .ok()
        .and_then(|index| index.checked_add(1))
        .ok_or_else(|| exec_datafusion_err!("too many Iceberg task contexts in one feed"))
}

fn context_to_proto(task: &FileScanTask) -> pb::FileScanTaskContext {
    pb::FileScanTaskContext {
        schema: Some(schema_to_proto(&task.schema)),
        projected_field_ids: task.project_field_ids.clone(),
        predicate: task.predicate.as_ref().map(predicate_to_proto),
        partition_spec: task.partition_spec.as_deref().map(partition_spec_to_proto),
        name_mapping: task.name_mapping.as_deref().map(name_mapping_to_proto),
        case_sensitive: task.case_sensitive,
    }
}

fn context_from_proto(proto: pb::FileScanTaskContext) -> Result<TaskContext> {
    let schema = Arc::new(schema_from_proto(required(
        proto.schema,
        "context.schema",
    )?)?);
    validate_projected_fields(&schema, &proto.projected_field_ids)?;
    let partition_spec = proto
        .partition_spec
        .map(|spec| partition_spec_from_proto(spec, Arc::clone(&schema)).map(Arc::new))
        .transpose()?;
    let predicate = proto
        .predicate
        .map(|predicate| predicate_from_proto(predicate, Arc::clone(&schema), proto.case_sensitive))
        .transpose()?;
    Ok(TaskContext {
        schema,
        projected_field_ids: proto.projected_field_ids,
        predicate,
        partition_spec,
        name_mapping: proto
            .name_mapping
            .map(name_mapping_from_proto)
            .map(Arc::new),
        case_sensitive: proto.case_sensitive,
    })
}

fn body_from_proto(body: pb::FileScanTaskBody, context: &TaskContext) -> Result<FileScanTask> {
    validate_file_range(body.file_size_in_bytes, body.start, body.length)?;
    Ok(FileScanTask {
        file_size_in_bytes: body.file_size_in_bytes,
        start: body.start,
        length: body.length,
        record_count: body.record_count,
        data_file_path: body.data_file_path,
        data_file_format: data_file_format_from_proto(body.data_file_format)?,
        schema: Arc::clone(&context.schema),
        project_field_ids: context.projected_field_ids.clone(),
        predicate: context.predicate.clone(),
        deletes: body
            .deletes
            .into_iter()
            .map(delete_from_proto)
            .collect::<Result<_>>()?,
        partition: partition_from_proto(
            body.partition,
            context.partition_spec.as_deref(),
            &context.schema,
        )?,
        partition_spec: context.partition_spec.clone(),
        name_mapping: context.name_mapping.clone(),
        case_sensitive: context.case_sensitive,
    })
}

fn validate_projected_fields(schema: &Schema, field_ids: &[i32]) -> Result<()> {
    for field_id in field_ids {
        if schema.field_by_id(*field_id).is_none() {
            return exec_err!("Iceberg work unit projects unknown field id {field_id}");
        }
    }
    Ok(())
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

fn data_file_format_to_proto(format: DataFileFormat) -> pb::DataFileFormat {
    match format {
        DataFileFormat::Avro => pb::DataFileFormat::Avro,
        DataFileFormat::Orc => pb::DataFileFormat::Orc,
        DataFileFormat::Parquet => pb::DataFileFormat::Parquet,
        DataFileFormat::Puffin => pb::DataFileFormat::Puffin,
    }
}

fn data_file_format_from_proto(format: i32) -> Result<DataFileFormat> {
    match pb::DataFileFormat::try_from(format).ok() {
        Some(pb::DataFileFormat::Avro) => Ok(DataFileFormat::Avro),
        Some(pb::DataFileFormat::Orc) => Ok(DataFileFormat::Orc),
        Some(pb::DataFileFormat::Parquet) => Ok(DataFileFormat::Parquet),
        Some(pb::DataFileFormat::Puffin) => Ok(DataFileFormat::Puffin),
        Some(pb::DataFileFormat::Unspecified) | None => {
            exec_err!("Iceberg work unit has invalid data file format {format}")
        }
    }
}

fn delete_to_proto(delete: &FileScanTaskDeleteFile) -> pb::DeleteFile {
    pb::DeleteFile {
        file_path: delete.file_path.clone(),
        file_size_in_bytes: delete.file_size_in_bytes,
        file_type: match delete.file_type {
            DataContentType::Data => pb::DataContentType::Data,
            DataContentType::PositionDeletes => pb::DataContentType::PositionDeletes,
            DataContentType::EqualityDeletes => pb::DataContentType::EqualityDeletes,
        } as i32,
        partition_spec_id: delete.partition_spec_id,
        equality_ids: delete.equality_ids.as_ref().map(|values| pb::EqualityIds {
            values: values.clone(),
        }),
    }
}

fn delete_from_proto(delete: pb::DeleteFile) -> Result<FileScanTaskDeleteFile> {
    let file_type = match pb::DataContentType::try_from(delete.file_type).ok() {
        Some(pb::DataContentType::Data) => DataContentType::Data,
        Some(pb::DataContentType::PositionDeletes) => DataContentType::PositionDeletes,
        Some(pb::DataContentType::EqualityDeletes) => DataContentType::EqualityDeletes,
        Some(pb::DataContentType::Unspecified) | None => {
            return exec_err!(
                "Iceberg work unit has invalid delete file type {}",
                delete.file_type
            );
        }
    };
    Ok(FileScanTaskDeleteFile {
        file_path: delete.file_path,
        file_size_in_bytes: delete.file_size_in_bytes,
        file_type,
        partition_spec_id: delete.partition_spec_id,
        equality_ids: delete.equality_ids.map(|values| values.values),
    })
}

fn partition_to_proto(
    partition: Option<&Struct>,
    spec: Option<&PartitionSpec>,
    schema: &Schema,
) -> Result<Option<pb::PartitionValues>> {
    // iceberg-rust 0.10 does not propagate the manifest partition spec into planned tasks.
    // Values without their matching type metadata cannot be transferred safely.
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

    let values = partition
        .iter()
        .zip(partition_type.fields())
        .enumerate()
        .map(|(index, (value, field))| {
            let Some(value) = value else {
                return Ok(pb::PartitionValue { value: None });
            };
            let Literal::Primitive(value) = value else {
                return exec_err!("Iceberg partition value {index} is not primitive");
            };
            let Type::Primitive(data_type) = field.field_type.as_ref() else {
                return exec_err!("Iceberg partition field {index} is not primitive");
            };
            if !data_type.compatible(value) {
                return exec_err!(
                    "Iceberg partition value {index} is incompatible with type {data_type}"
                );
            }
            Ok(pb::PartitionValue {
                value: Some(partition_value_bytes(value, data_type).map_err(|error| {
                    exec_datafusion_err!("invalid Iceberg partition value {index}: {error}")
                })?),
            })
        })
        .collect::<Result<_>>()?;
    Ok(Some(pb::PartitionValues { values }))
}

fn partition_from_proto(
    partition: Option<pb::PartitionValues>,
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
    if partition.values.len() != partition_type.fields().len() {
        return exec_err!(
            "Iceberg work unit has {} partition values for {} partition fields",
            partition.values.len(),
            partition_type.fields().len()
        );
    }

    partition
        .values
        .into_iter()
        .zip(partition_type.fields())
        .enumerate()
        .map(|(index, (value, field))| {
            let Some(value) = value.value else {
                return Ok(None);
            };
            let Type::Primitive(data_type) = field.field_type.as_ref() else {
                return exec_err!("Iceberg partition field {index} is not primitive");
            };
            let datum = Datum::try_from_bytes(&value, data_type.clone()).map_err(|error| {
                exec_datafusion_err!("invalid Iceberg partition value {index}: {error}")
            })?;
            let canonical = partition_value_bytes(datum.literal(), data_type).map_err(|error| {
                exec_datafusion_err!("invalid Iceberg partition value {index}: {error}")
            })?;
            if canonical != value {
                return exec_err!("Iceberg partition value {index} has non-canonical bytes");
            }
            Ok(Some(Literal::Primitive(datum.literal().clone())))
        })
        .collect::<Result<Struct>>()
        .map(Some)
}

fn schema_to_proto(schema: &Schema) -> pb::Schema {
    pb::Schema {
        schema_id: schema.schema_id(),
        fields: schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| nested_field_to_proto(field))
            .collect(),
        identifier_field_ids: schema.identifier_field_ids().collect(),
    }
}

fn schema_from_proto(schema: pb::Schema) -> Result<Schema> {
    let fields = schema
        .fields
        .into_iter()
        .map(nested_field_from_proto)
        .map(|field| field.map(Arc::new))
        .collect::<Result<Vec<_>>>()?;
    Schema::builder()
        .with_schema_id(schema.schema_id)
        .with_fields(fields)
        .with_identifier_field_ids(schema.identifier_field_ids)
        .build()
        .map_err(|error| exec_datafusion_err!("invalid Iceberg work unit schema: {error}"))
}

fn nested_field_to_proto(field: &NestedField) -> pb::NestedField {
    pb::NestedField {
        id: field.id,
        name: field.name.clone(),
        required: field.required,
        field_type: Some(Box::new(type_to_proto(&field.field_type))),
        doc: field.doc.clone(),
        initial_default: field.initial_default.as_ref().map(literal_to_proto),
        write_default: field.write_default.as_ref().map(literal_to_proto),
    }
}

fn nested_field_from_proto(field: pb::NestedField) -> Result<NestedField> {
    let field_type = type_from_proto(*required(field.field_type, "nested field type")?)?;
    let initial_default = field
        .initial_default
        .map(|literal| literal_from_proto(literal, &field_type))
        .transpose()?;
    let write_default = field
        .write_default
        .map(|literal| literal_from_proto(literal, &field_type))
        .transpose()?;
    let mut nested = NestedField::new(field.id, field.name, field_type, field.required);
    nested.doc = field.doc;
    nested.initial_default = initial_default;
    nested.write_default = write_default;
    Ok(nested)
}

fn type_to_proto(data_type: &Type) -> pb::Type {
    use pb::r#type::Kind;
    let kind = match data_type {
        Type::Primitive(data_type) => Kind::Primitive(primitive_type_to_proto(data_type)),
        Type::Struct(data_type) => Kind::Struct(pb::StructType {
            fields: data_type
                .fields()
                .iter()
                .map(|field| nested_field_to_proto(field))
                .collect(),
        }),
        Type::List(data_type) => Kind::List(Box::new(pb::ListType {
            element: Some(Box::new(nested_field_to_proto(&data_type.element_field))),
        })),
        Type::Map(data_type) => Kind::Map(Box::new(pb::MapType {
            key: Some(Box::new(nested_field_to_proto(&data_type.key_field))),
            value: Some(Box::new(nested_field_to_proto(&data_type.value_field))),
        })),
    };
    pb::Type { kind: Some(kind) }
}

fn type_from_proto(data_type: pb::Type) -> Result<Type> {
    use pb::r#type::Kind;
    match required(data_type.kind, "type kind")? {
        Kind::Primitive(data_type) => Ok(Type::Primitive(primitive_type_from_proto(data_type)?)),
        Kind::Struct(data_type) => data_type
            .fields
            .into_iter()
            .map(nested_field_from_proto)
            .map(|field| field.map(Arc::new))
            .collect::<Result<Vec<_>>>()
            .map(StructType::new)
            .map(Type::Struct),
        Kind::List(data_type) => {
            let field = nested_field_from_proto(*required(data_type.element, "list element")?)?;
            Ok(Type::List(ListType::new(Arc::new(field))))
        }
        Kind::Map(data_type) => {
            let key = nested_field_from_proto(*required(data_type.key, "map key")?)?;
            let value = nested_field_from_proto(*required(data_type.value, "map value")?)?;
            Ok(Type::Map(MapType::new(Arc::new(key), Arc::new(value))))
        }
    }
}

fn primitive_type_to_proto(data_type: &PrimitiveType) -> pb::PrimitiveType {
    use pb::primitive_type::Kind;
    let empty = pb::Empty {};
    let kind = match data_type {
        PrimitiveType::Boolean => Kind::Boolean(empty),
        PrimitiveType::Int => Kind::Int(empty),
        PrimitiveType::Long => Kind::Long(empty),
        PrimitiveType::Float => Kind::Float(empty),
        PrimitiveType::Double => Kind::Double(empty),
        PrimitiveType::Decimal { precision, scale } => Kind::Decimal(pb::DecimalType {
            precision: *precision,
            scale: *scale,
        }),
        PrimitiveType::Date => Kind::Date(empty),
        PrimitiveType::Time => Kind::Time(empty),
        PrimitiveType::Timestamp => Kind::Timestamp(empty),
        PrimitiveType::Timestamptz => Kind::Timestamptz(empty),
        PrimitiveType::TimestampNs => Kind::TimestampNs(empty),
        PrimitiveType::TimestamptzNs => Kind::TimestamptzNs(empty),
        PrimitiveType::String => Kind::String(empty),
        PrimitiveType::Uuid => Kind::Uuid(empty),
        PrimitiveType::Fixed(length) => Kind::Fixed(*length),
        PrimitiveType::Binary => Kind::Binary(empty),
    };
    pb::PrimitiveType { kind: Some(kind) }
}

fn primitive_type_from_proto(data_type: pb::PrimitiveType) -> Result<PrimitiveType> {
    use pb::primitive_type::Kind;
    match required(data_type.kind, "primitive type kind")? {
        Kind::Boolean(_) => Ok(PrimitiveType::Boolean),
        Kind::Int(_) => Ok(PrimitiveType::Int),
        Kind::Long(_) => Ok(PrimitiveType::Long),
        Kind::Float(_) => Ok(PrimitiveType::Float),
        Kind::Double(_) => Ok(PrimitiveType::Double),
        Kind::Decimal(decimal) => match Type::decimal(decimal.precision, decimal.scale) {
            Ok(Type::Primitive(data_type)) => Ok(data_type),
            Ok(_) => unreachable!(),
            Err(error) => exec_err!("invalid Iceberg decimal type: {error}"),
        },
        Kind::Date(_) => Ok(PrimitiveType::Date),
        Kind::Time(_) => Ok(PrimitiveType::Time),
        Kind::Timestamp(_) => Ok(PrimitiveType::Timestamp),
        Kind::Timestamptz(_) => Ok(PrimitiveType::Timestamptz),
        Kind::TimestampNs(_) => Ok(PrimitiveType::TimestampNs),
        Kind::TimestamptzNs(_) => Ok(PrimitiveType::TimestamptzNs),
        Kind::String(_) => Ok(PrimitiveType::String),
        Kind::Uuid(_) => Ok(PrimitiveType::Uuid),
        Kind::Fixed(length) => Ok(PrimitiveType::Fixed(length)),
        Kind::Binary(_) => Ok(PrimitiveType::Binary),
    }
}

fn literal_to_proto(literal: &Literal) -> pb::Literal {
    use pb::literal::Value;
    let value = match literal {
        Literal::Primitive(literal) => Value::Primitive(primitive_literal_to_proto(literal)),
        Literal::Struct(values) => Value::Struct(literal_values_to_proto(values.iter())),
        Literal::List(values) => Value::List(literal_values_to_proto(
            values.iter().map(|value| value.as_ref()),
        )),
        Literal::Map(values) => Value::Map(pb::MapLiteral {
            entries: values
                .clone()
                .into_iter()
                .map(|(key, value)| pb::MapEntry {
                    key: Some(literal_to_proto(&key)),
                    value: value.as_ref().map(literal_to_proto),
                })
                .collect(),
        }),
    };
    pb::Literal { value: Some(value) }
}

fn literal_values_to_proto<'a>(
    values: impl IntoIterator<Item = Option<&'a Literal>>,
) -> pb::LiteralValues {
    pb::LiteralValues {
        values: values
            .into_iter()
            .map(|value| pb::LiteralValue {
                value: value.map(literal_to_proto),
            })
            .collect(),
    }
}

fn literal_from_proto(literal: pb::Literal, data_type: &Type) -> Result<Literal> {
    use pb::literal::Value;
    match (required(literal.value, "literal value")?, data_type) {
        (Value::Primitive(literal), Type::Primitive(data_type)) => {
            let literal = primitive_literal_from_proto(literal)?;
            if !data_type.compatible(&literal) {
                return exec_err!("Iceberg literal is incompatible with type {data_type}");
            }
            Ok(Literal::Primitive(literal))
        }
        (Value::Struct(values), Type::Struct(data_type)) => {
            literal_values_from_proto(values, data_type.fields()).map(Literal::Struct)
        }
        (Value::List(values), Type::List(data_type)) => values
            .values
            .into_iter()
            .map(|value| {
                value
                    .value
                    .map(|value| literal_from_proto(value, &data_type.element_field.field_type))
                    .transpose()
            })
            .collect::<Result<Vec<_>>>()
            .map(Literal::List),
        (Value::Map(map), Type::Map(data_type)) => map
            .entries
            .into_iter()
            .map(|entry| {
                let key = literal_from_proto(
                    required(entry.key, "map literal key")?,
                    &data_type.key_field.field_type,
                )?;
                let value = entry
                    .value
                    .map(|value| literal_from_proto(value, &data_type.value_field.field_type))
                    .transpose()?;
                Ok((key, value))
            })
            .collect::<Result<Map>>()
            .map(Literal::Map),
        _ => exec_err!("Iceberg literal does not match its declared type"),
    }
}

fn literal_values_from_proto(
    values: pb::LiteralValues,
    fields: &[Arc<NestedField>],
) -> Result<Struct> {
    if values.values.len() != fields.len() {
        return exec_err!(
            "Iceberg literal has {} values for {} fields",
            values.values.len(),
            fields.len()
        );
    }
    values
        .values
        .into_iter()
        .zip(fields)
        .map(|(value, field)| {
            value
                .value
                .map(|value| literal_from_proto(value, &field.field_type))
                .transpose()
        })
        .collect()
}

fn primitive_literal_to_proto(literal: &PrimitiveLiteral) -> pb::PrimitiveLiteral {
    use pb::primitive_literal::Value;
    let value = match literal {
        PrimitiveLiteral::Boolean(value) => Value::Boolean(*value),
        PrimitiveLiteral::Int(value) => Value::Int(*value),
        PrimitiveLiteral::Long(value) => Value::Long(*value),
        PrimitiveLiteral::Float(value) => Value::FloatBits(value.to_bits()),
        PrimitiveLiteral::Double(value) => Value::DoubleBits(value.to_bits()),
        PrimitiveLiteral::String(value) => Value::String(value.clone()),
        PrimitiveLiteral::Binary(value) => Value::Binary(value.clone()),
        PrimitiveLiteral::Int128(value) => Value::Int128(value.to_be_bytes().to_vec()),
        PrimitiveLiteral::UInt128(value) => Value::Uint128(value.to_be_bytes().to_vec()),
        PrimitiveLiteral::AboveMax => Value::AboveMax(pb::Empty {}),
        PrimitiveLiteral::BelowMin => Value::BelowMin(pb::Empty {}),
    };
    pb::PrimitiveLiteral { value: Some(value) }
}

fn primitive_literal_from_proto(literal: pb::PrimitiveLiteral) -> Result<PrimitiveLiteral> {
    use pb::primitive_literal::Value;
    match required(literal.value, "primitive literal value")? {
        Value::Boolean(value) => Ok(PrimitiveLiteral::Boolean(value)),
        Value::Int(value) => Ok(PrimitiveLiteral::Int(value)),
        Value::Long(value) => Ok(PrimitiveLiteral::Long(value)),
        Value::FloatBits(value) => Ok(PrimitiveLiteral::Float(f32::from_bits(value).into())),
        Value::DoubleBits(value) => Ok(PrimitiveLiteral::Double(f64::from_bits(value).into())),
        Value::String(value) => Ok(PrimitiveLiteral::String(value)),
        Value::Binary(value) => Ok(PrimitiveLiteral::Binary(value)),
        Value::Int128(value) => value
            .try_into()
            .map(i128::from_be_bytes)
            .map(PrimitiveLiteral::Int128)
            .map_err(|value: Vec<u8>| {
                exec_datafusion_err!(
                    "Iceberg int128 literal has {} bytes instead of 16",
                    value.len()
                )
            }),
        Value::Uint128(value) => value
            .try_into()
            .map(u128::from_be_bytes)
            .map(PrimitiveLiteral::UInt128)
            .map_err(|value: Vec<u8>| {
                exec_datafusion_err!(
                    "Iceberg uint128 literal has {} bytes instead of 16",
                    value.len()
                )
            }),
        Value::AboveMax(_) => Ok(PrimitiveLiteral::AboveMax),
        Value::BelowMin(_) => Ok(PrimitiveLiteral::BelowMin),
    }
}

fn predicate_to_proto(predicate: &BoundPredicate) -> pb::BoundPredicate {
    use pb::bound_predicate::Expression;
    let expression = match predicate {
        BoundPredicate::AlwaysTrue => Expression::AlwaysTrue(pb::Empty {}),
        BoundPredicate::AlwaysFalse => Expression::AlwaysFalse(pb::Empty {}),
        BoundPredicate::And(predicate) => {
            let [left, right] = predicate.inputs();
            Expression::And(Box::new(pb::BinaryLogicalPredicate {
                left: Some(Box::new(predicate_to_proto(left))),
                right: Some(Box::new(predicate_to_proto(right))),
            }))
        }
        BoundPredicate::Or(predicate) => {
            let [left, right] = predicate.inputs();
            Expression::Or(Box::new(pb::BinaryLogicalPredicate {
                left: Some(Box::new(predicate_to_proto(left))),
                right: Some(Box::new(predicate_to_proto(right))),
            }))
        }
        BoundPredicate::Not(predicate) => {
            let [predicate] = predicate.inputs();
            Expression::Not(Box::new(predicate_to_proto(predicate)))
        }
        BoundPredicate::Unary(predicate) => Expression::Unary(pb::UnaryPredicate {
            op: predicate_operator_to_proto(predicate.op()) as i32,
            term: Some(reference_to_proto(predicate.term())),
        }),
        BoundPredicate::Binary(predicate) => Expression::Binary(pb::BinaryPredicate {
            op: predicate_operator_to_proto(predicate.op()) as i32,
            term: Some(reference_to_proto(predicate.term())),
            literal: Some(datum_to_proto(predicate.literal())),
        }),
        BoundPredicate::Set(predicate) => Expression::Set(pb::SetPredicate {
            op: predicate_operator_to_proto(predicate.op()) as i32,
            term: Some(reference_to_proto(predicate.term())),
            literals: predicate.literals().iter().map(datum_to_proto).collect(),
        }),
    };
    pb::BoundPredicate {
        expression: Some(expression),
    }
}

fn predicate_from_proto(
    predicate: pb::BoundPredicate,
    schema: SchemaRef,
    case_sensitive: bool,
) -> Result<BoundPredicate> {
    let predicate = unbound_predicate_from_proto(predicate, &schema, case_sensitive)?;
    predicate
        .bind(schema, case_sensitive)
        .map_err(|error| exec_datafusion_err!("invalid Iceberg work unit predicate: {error}"))
}

fn unbound_predicate_from_proto(
    predicate: pb::BoundPredicate,
    schema: &Schema,
    case_sensitive: bool,
) -> Result<Predicate> {
    use pb::bound_predicate::Expression;
    match required(predicate.expression, "predicate expression")? {
        Expression::AlwaysTrue(_) => Ok(Predicate::AlwaysTrue),
        Expression::AlwaysFalse(_) => Ok(Predicate::AlwaysFalse),
        Expression::And(predicate) => {
            let left = unbound_predicate_from_proto(
                *required(predicate.left, "and.left")?,
                schema,
                case_sensitive,
            )?;
            let right = unbound_predicate_from_proto(
                *required(predicate.right, "and.right")?,
                schema,
                case_sensitive,
            )?;
            Ok(left.and(right))
        }
        Expression::Or(predicate) => {
            let left = unbound_predicate_from_proto(
                *required(predicate.left, "or.left")?,
                schema,
                case_sensitive,
            )?;
            let right = unbound_predicate_from_proto(
                *required(predicate.right, "or.right")?,
                schema,
                case_sensitive,
            )?;
            Ok(left.or(right))
        }
        Expression::Not(predicate) => {
            Ok(unbound_predicate_from_proto(*predicate, schema, case_sensitive)?.not())
        }
        Expression::Unary(predicate) => {
            let op = predicate_operator_from_proto(predicate.op)?;
            if !op.is_unary() {
                return exec_err!("Iceberg unary predicate has non-unary operator {op}");
            }
            Ok(Predicate::Unary(UnaryExpression::new(
                op,
                reference_from_proto(
                    required(predicate.term, "unary predicate term")?,
                    schema,
                    case_sensitive,
                )?,
            )))
        }
        Expression::Binary(predicate) => {
            let op = predicate_operator_from_proto(predicate.op)?;
            if !op.is_binary() {
                return exec_err!("Iceberg binary predicate has non-binary operator {op}");
            }
            Ok(Predicate::Binary(BinaryExpression::new(
                op,
                reference_from_proto(
                    required(predicate.term, "binary predicate term")?,
                    schema,
                    case_sensitive,
                )?,
                datum_from_proto(required(predicate.literal, "binary predicate literal")?)?,
            )))
        }
        Expression::Set(predicate) => {
            let op = predicate_operator_from_proto(predicate.op)?;
            if !op.is_set() {
                return exec_err!("Iceberg set predicate has non-set operator {op}");
            }
            let literals = predicate
                .literals
                .into_iter()
                .map(datum_from_proto)
                .collect::<Result<Vec<_>>>()?;
            Ok(Predicate::Set(SetExpression::new(
                op,
                reference_from_proto(
                    required(predicate.term, "set predicate term")?,
                    schema,
                    case_sensitive,
                )?,
                literals.into_iter().collect(),
            )))
        }
    }
}

fn reference_to_proto(reference: &iceberg::expr::BoundReference) -> pb::BoundReference {
    pb::BoundReference {
        column_name: reference.to_string(),
        field_id: reference.field().id,
    }
}

fn reference_from_proto(
    reference: pb::BoundReference,
    schema: &Schema,
    case_sensitive: bool,
) -> Result<Reference> {
    let field = if case_sensitive {
        schema.field_by_name(&reference.column_name)
    } else {
        schema.field_by_name_case_insensitive(&reference.column_name)
    };
    if field.map(|field| field.id) != Some(reference.field_id) {
        return exec_err!(
            "Iceberg predicate reference {} does not identify field id {}",
            reference.column_name,
            reference.field_id
        );
    }
    Ok(Reference::new(reference.column_name))
}

fn predicate_operator_to_proto(operator: PredicateOperator) -> pb::PredicateOperator {
    match operator as u16 {
        101 => pb::PredicateOperator::IsNull,
        102 => pb::PredicateOperator::NotNull,
        103 => pb::PredicateOperator::IsNan,
        104 => pb::PredicateOperator::NotNan,
        201 => pb::PredicateOperator::LessThan,
        202 => pb::PredicateOperator::LessThanOrEqual,
        203 => pb::PredicateOperator::GreaterThan,
        204 => pb::PredicateOperator::GreaterThanOrEqual,
        205 => pb::PredicateOperator::Equal,
        206 => pb::PredicateOperator::NotEqual,
        207 => pb::PredicateOperator::StartsWith,
        208 => pb::PredicateOperator::NotStartsWith,
        301 => pb::PredicateOperator::In,
        302 => pb::PredicateOperator::NotIn,
        _ => pb::PredicateOperator::Unspecified,
    }
}

fn predicate_operator_from_proto(operator: i32) -> Result<PredicateOperator> {
    match pb::PredicateOperator::try_from(operator).ok() {
        Some(pb::PredicateOperator::IsNull) => Ok(PredicateOperator::IsNull),
        Some(pb::PredicateOperator::NotNull) => Ok(PredicateOperator::NotNull),
        Some(pb::PredicateOperator::IsNan) => Ok(PredicateOperator::IsNan),
        Some(pb::PredicateOperator::NotNan) => Ok(PredicateOperator::NotNan),
        Some(pb::PredicateOperator::LessThan) => Ok(PredicateOperator::LessThan),
        Some(pb::PredicateOperator::LessThanOrEqual) => Ok(PredicateOperator::LessThanOrEq),
        Some(pb::PredicateOperator::GreaterThan) => Ok(PredicateOperator::GreaterThan),
        Some(pb::PredicateOperator::GreaterThanOrEqual) => Ok(PredicateOperator::GreaterThanOrEq),
        Some(pb::PredicateOperator::Equal) => Ok(PredicateOperator::Eq),
        Some(pb::PredicateOperator::NotEqual) => Ok(PredicateOperator::NotEq),
        Some(pb::PredicateOperator::StartsWith) => Ok(PredicateOperator::StartsWith),
        Some(pb::PredicateOperator::NotStartsWith) => Ok(PredicateOperator::NotStartsWith),
        Some(pb::PredicateOperator::In) => Ok(PredicateOperator::In),
        Some(pb::PredicateOperator::NotIn) => Ok(PredicateOperator::NotIn),
        Some(pb::PredicateOperator::Unspecified) | None => {
            exec_err!("Iceberg predicate has invalid operator {operator}")
        }
    }
}

fn datum_to_proto(datum: &Datum) -> pb::Datum {
    pb::Datum {
        data_type: Some(primitive_type_to_proto(datum.data_type())),
        literal: Some(primitive_literal_to_proto(datum.literal())),
    }
}

fn datum_from_proto(datum: pb::Datum) -> Result<Datum> {
    let data_type = primitive_type_from_proto(required(datum.data_type, "datum type")?)?;
    let literal = primitive_literal_from_proto(required(datum.literal, "datum literal")?)?;
    if !data_type.compatible(&literal) {
        return exec_err!("Iceberg datum literal is incompatible with type {data_type}");
    }
    let bytes = primitive_literal_bytes(literal)?;
    Datum::try_from_bytes(&bytes, data_type)
        .map_err(|error| exec_datafusion_err!("invalid Iceberg datum: {error}"))
}

fn partition_spec_to_proto(spec: &PartitionSpec) -> pb::PartitionSpec {
    pb::PartitionSpec {
        spec_id: spec.spec_id(),
        fields: spec
            .fields()
            .iter()
            .map(|field| pb::PartitionField {
                source_id: field.source_id,
                field_id: field.field_id,
                name: field.name.clone(),
                transform: Some(transform_to_proto(field.transform)),
            })
            .collect(),
    }
}

fn partition_spec_from_proto(proto: pb::PartitionSpec, schema: SchemaRef) -> Result<PartitionSpec> {
    let fields = proto
        .fields
        .into_iter()
        .map(|field| {
            Ok(UnboundPartitionField {
                source_id: field.source_id,
                field_id: Some(field.field_id),
                name: field.name,
                transform: transform_from_proto(required(field.transform, "partition transform")?)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    UnboundPartitionSpec::builder()
        .with_spec_id(proto.spec_id)
        .add_partition_fields(fields)
        .and_then(|builder| builder.build().bind(schema))
        .map_err(|error| exec_datafusion_err!("invalid Iceberg work unit partition spec: {error}"))
}

fn transform_to_proto(transform: Transform) -> pb::Transform {
    use pb::transform::Kind;
    let empty = pb::Empty {};
    let kind = match transform {
        Transform::Identity => Kind::Identity(empty),
        Transform::Bucket(value) => Kind::Bucket(value),
        Transform::Truncate(value) => Kind::Truncate(value),
        Transform::Year => Kind::Year(empty),
        Transform::Month => Kind::Month(empty),
        Transform::Day => Kind::Day(empty),
        Transform::Hour => Kind::Hour(empty),
        Transform::Void => Kind::Void(empty),
        Transform::Unknown => Kind::Unknown(empty),
    };
    pb::Transform { kind: Some(kind) }
}

fn transform_from_proto(transform: pb::Transform) -> Result<Transform> {
    use pb::transform::Kind;
    match required(transform.kind, "transform kind")? {
        Kind::Identity(_) => Ok(Transform::Identity),
        Kind::Bucket(value) => Ok(Transform::Bucket(value)),
        Kind::Truncate(value) => Ok(Transform::Truncate(value)),
        Kind::Year(_) => Ok(Transform::Year),
        Kind::Month(_) => Ok(Transform::Month),
        Kind::Day(_) => Ok(Transform::Day),
        Kind::Hour(_) => Ok(Transform::Hour),
        Kind::Void(_) => Ok(Transform::Void),
        Kind::Unknown(_) => Ok(Transform::Unknown),
    }
}

fn name_mapping_to_proto(mapping: &NameMapping) -> pb::NameMapping {
    pb::NameMapping {
        fields: mapping.fields().iter().map(mapped_field_to_proto).collect(),
    }
}

fn mapped_field_to_proto(field: &MappedField) -> pb::MappedField {
    pb::MappedField {
        field_id: field.field_id(),
        names: field.names().to_vec(),
        fields: field
            .fields()
            .iter()
            .map(|field| mapped_field_to_proto(field))
            .collect(),
    }
}

fn name_mapping_from_proto(mapping: pb::NameMapping) -> NameMapping {
    NameMapping::new(
        mapping
            .fields
            .into_iter()
            .map(mapped_field_from_proto)
            .collect(),
    )
}

fn mapped_field_from_proto(field: pb::MappedField) -> MappedField {
    MappedField::new(
        field.field_id,
        field.names,
        field
            .fields
            .into_iter()
            .map(mapped_field_from_proto)
            .collect(),
    )
}

fn partition_value_bytes(literal: &PrimitiveLiteral, data_type: &PrimitiveType) -> Result<Vec<u8>> {
    if let (PrimitiveType::Fixed(length), PrimitiveLiteral::Binary(value)) = (data_type, literal)
        && usize::try_from(*length).ok() != Some(value.len())
    {
        return exec_err!(
            "partition value has {} bytes for fixed[{length}]",
            value.len()
        );
    }
    if let (PrimitiveType::Decimal { precision, .. }, PrimitiveLiteral::Int128(value)) =
        (data_type, literal)
        && value
            .unsigned_abs()
            .checked_ilog10()
            .map_or(1, |digits| digits + 1)
            > *precision
    {
        return exec_err!("decimal partition value exceeds precision {precision}");
    }
    let bytes = primitive_literal_bytes(literal.clone())?;
    let datum = Datum::try_from_bytes(&bytes, data_type.clone())
        .map_err(|error| exec_datafusion_err!("{error}"))?;
    datum
        .to_bytes()
        .map(|bytes| bytes.into_vec())
        .map_err(|error| exec_datafusion_err!("{error}"))
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
            return exec_err!("Iceberg values cannot serialize range sentinels");
        }
    })
}

fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| exec_datafusion_err!("Iceberg work unit is missing {field}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::expr::Reference;

    #[test]
    fn local_message_keeps_native_task() {
        let task = sample_task("file.parquet", 10);
        let schema = Arc::clone(&task.schema);
        let partition_spec = Arc::clone(task.partition_spec.as_ref().unwrap());

        let message = FileScanTaskEncoder::default().encode(task).unwrap();
        let decoded = FileScanTaskDecoder::default().decode(message).unwrap();

        assert!(Arc::ptr_eq(&schema, &decoded.schema));
        assert!(Arc::ptr_eq(
            &partition_spec,
            decoded.partition_spec.as_ref().unwrap()
        ));
    }

    #[test]
    fn protobuf_roundtrips_complete_task() {
        let task = sample_task("file.parquet", 10);

        assert_eq!(protobuf_roundtrip(task.clone()).unwrap(), task);
    }

    #[test]
    fn reuses_context_within_each_feed() {
        let mut encoder = FileScanTaskEncoder::default();
        let first = encoder.encode(sample_task("first.parquet", 10)).unwrap();
        let second = encoder.encode(sample_task("second.parquet", 20)).unwrap();
        let mut changed = sample_task("third.parquet", 30);
        changed.case_sensitive = false;
        let changed = encoder.encode(changed).unwrap();

        assert_eq!(context_state(&first), (1, true));
        assert_eq!(context_state(&second), (1, false));
        assert_eq!(context_state(&changed), (2, true));

        let mut decoder = FileScanTaskDecoder::default();
        let first = decoder.decode(protobuf_message(first)).unwrap();
        let second = decoder.decode(protobuf_message(second)).unwrap();
        assert_eq!(first.data_file_path, "first.parquet");
        assert_eq!(second.data_file_path, "second.parquet");
        assert!(Arc::ptr_eq(&first.schema, &second.schema));
        assert!(
            !decoder
                .decode(protobuf_message(changed))
                .unwrap()
                .case_sensitive
        );
    }

    #[test]
    fn rejects_undefined_and_redefined_context() {
        let proto = encoded_proto(sample_task("file.parquet", 10));
        let mut undefined = proto.clone();
        undefined.context = None;
        let error = FileScanTaskDecoder::default()
            .decode(message_from_proto(undefined))
            .unwrap_err();
        assert!(error.to_string().contains("undefined context id 1"));

        let mut decoder = FileScanTaskDecoder::default();
        decoder.decode(message_from_proto(proto.clone())).unwrap();
        let mut redefined = proto;
        redefined.context.as_mut().unwrap().case_sensitive = false;
        let error = decoder.decode(message_from_proto(redefined)).unwrap_err();
        assert!(error.to_string().contains("context id 1 was redefined"));
    }

    #[test]
    fn failed_task_does_not_define_context() {
        let mut invalid = encoded_proto(sample_task("file.parquet", 10));
        let mut reference = invalid.clone();
        reference.context = None;
        let task = invalid.task.as_mut().unwrap();
        task.start = u64::MAX;
        task.length = 2;
        task.file_size_in_bytes = u64::MAX;
        let mut decoder = FileScanTaskDecoder::default();

        assert!(decoder.decode(message_from_proto(invalid)).is_err());
        let error = decoder.decode(message_from_proto(reference)).unwrap_err();

        assert!(error.to_string().contains("undefined context id 1"));
    }

    #[test]
    fn omits_untyped_partition_values_from_protobuf() {
        let mut task = sample_task("file.parquet", 10);
        task.partition_spec = None;

        let decoded = protobuf_roundtrip(task).unwrap();

        assert!(decoded.partition.is_none());
        assert!(decoded.partition_spec.is_none());
    }

    #[test]
    fn rejects_invalid_native_task() {
        let mut task = sample_task("file.parquet", 10);
        task.partition = Some(
            [Some(Literal::List(vec![Some(Literal::int(10))]))]
                .into_iter()
                .collect(),
        );

        let error = FileScanTaskEncoder::default().encode(task).unwrap_err();

        assert!(error.to_string().contains("not primitive"));
    }

    #[test]
    fn rejects_invalid_file_range_and_projection() {
        assert_proto_error(
            |proto| {
                let task = proto.task.as_mut().unwrap();
                task.start = u64::MAX;
                task.length = 2;
                task.file_size_in_bytes = u64::MAX;
            },
            "overflows",
        );
        assert_proto_error(
            |proto| {
                proto.context.as_mut().unwrap().projected_field_ids.push(99);
            },
            "projects unknown field id 99",
        );
    }

    #[test]
    fn rejects_invalid_partition_metadata() {
        assert_proto_error(
            |proto| proto.task.as_mut().unwrap().partition = None,
            "partition spec without partition values",
        );
        assert_proto_error(
            |proto| proto.context.as_mut().unwrap().partition_spec = None,
            "partition values without a partition spec",
        );
        assert_proto_error(
            |proto| {
                proto
                    .task
                    .as_mut()
                    .unwrap()
                    .partition
                    .as_mut()
                    .unwrap()
                    .values
                    .clear();
            },
            "partition values for 1 partition fields",
        );
    }

    #[test]
    fn rejects_non_canonical_partition_bytes() {
        let mut proto = encoded_proto(boolean_partition_task());
        proto
            .task
            .as_mut()
            .unwrap()
            .partition
            .as_mut()
            .unwrap()
            .values[0]
            .value = Some(vec![2]);

        let error = decode_proto(proto).unwrap_err();

        assert!(error.to_string().contains("non-canonical bytes"), "{error}");
    }

    #[test]
    fn validates_decimal_and_fixed_partition_bytes() {
        let mut proto = encoded_proto(typed_partition_task(
            PrimitiveType::Decimal {
                precision: 9,
                scale: 2,
            },
            Literal::decimal(123),
        ));
        let value = &mut proto
            .task
            .as_mut()
            .unwrap()
            .partition
            .as_mut()
            .unwrap()
            .values[0]
            .value;
        assert_eq!(value.as_deref(), Some([123].as_slice()));
        *value = Some([vec![0; 15], vec![123]].concat());
        let error = decode_proto(proto).unwrap_err();
        assert!(error.to_string().contains("non-canonical bytes"), "{error}");

        let task = typed_partition_task(PrimitiveType::Fixed(2), Literal::binary([1]));
        let error = FileScanTaskEncoder::default().encode(task).unwrap_err();
        assert!(
            error.to_string().contains("1 bytes for fixed[2]"),
            "{error}"
        );
    }

    fn protobuf_roundtrip(task: FileScanTask) -> Result<FileScanTask> {
        decode_proto(encoded_proto(task))
    }

    fn encoded_proto(task: FileScanTask) -> pb::FileScanTask {
        let message = FileScanTaskEncoder::default().encode(task).unwrap();
        pb::FileScanTask::decode(message.encode_to_vec().as_slice()).unwrap()
    }

    fn decode_proto(proto: pb::FileScanTask) -> Result<FileScanTask> {
        FileScanTaskDecoder::default().decode(message_from_proto(proto))
    }

    fn message_from_proto(proto: pb::FileScanTask) -> FileScanTaskMessage {
        FileScanTaskMessage::decode(proto.encode_to_vec().as_slice()).unwrap()
    }

    fn protobuf_message(message: FileScanTaskMessage) -> FileScanTaskMessage {
        FileScanTaskMessage::decode(message.encode_to_vec().as_slice()).unwrap()
    }

    fn context_state(message: &FileScanTaskMessage) -> (u32, bool) {
        let proto = pb::FileScanTask::decode(message.encode_to_vec().as_slice()).unwrap();
        (proto.context_id, proto.context.is_some())
    }

    #[track_caller]
    fn assert_proto_error(edit: impl FnOnce(&mut pb::FileScanTask), expected: &str) {
        let mut proto = encoded_proto(sample_task("file.parquet", 10));
        edit(&mut proto);
        let error = decode_proto(proto).unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    fn sample_task(path: &str, partition_value: i32) -> FileScanTask {
        let schema = test_schema(PrimitiveType::Int);
        let partition_spec = Arc::new(
            PartitionSpec::builder(Arc::clone(&schema))
                .with_spec_id(7)
                .add_partition_field("id", "id", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        let predicate = Reference::new("id")
            .greater_than(Datum::int(5))
            .bind(Arc::clone(&schema), true)
            .unwrap();
        FileScanTask {
            file_size_in_bytes: 100,
            start: 10,
            length: 80,
            record_count: Some(12),
            data_file_path: path.to_owned(),
            data_file_format: DataFileFormat::Parquet,
            schema,
            project_field_ids: vec![1],
            predicate: Some(predicate),
            deletes: vec![FileScanTaskDeleteFile {
                file_path: "delete.parquet".to_owned(),
                file_size_in_bytes: 20,
                file_type: DataContentType::EqualityDeletes,
                partition_spec_id: 7,
                equality_ids: Some(vec![1]),
            }],
            partition: Some([Some(Literal::int(partition_value))].into_iter().collect()),
            partition_spec: Some(partition_spec),
            name_mapping: Some(Arc::new(NameMapping::new(vec![MappedField::new(
                Some(1),
                vec!["id".to_owned(), "record_id".to_owned()],
                vec![],
            )]))),
            case_sensitive: true,
        }
    }

    fn boolean_partition_task() -> FileScanTask {
        typed_partition_task(PrimitiveType::Boolean, Literal::bool(true))
    }

    fn typed_partition_task(data_type: PrimitiveType, value: Literal) -> FileScanTask {
        let schema = test_schema(data_type);
        let partition_spec = Arc::new(
            PartitionSpec::builder(Arc::clone(&schema))
                .add_partition_field("id", "id", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        );
        FileScanTask {
            file_size_in_bytes: 1,
            start: 0,
            length: 1,
            record_count: None,
            data_file_path: "typed.parquet".to_owned(),
            data_file_format: DataFileFormat::Parquet,
            schema,
            project_field_ids: vec![1],
            predicate: None,
            deletes: vec![],
            partition: Some([Some(value)].into_iter().collect()),
            partition_spec: Some(partition_spec),
            name_mapping: None,
            case_sensitive: true,
        }
    }

    fn test_schema(data_type: PrimitiveType) -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(3)
                .with_fields([Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(data_type),
                ))])
                .with_identifier_field_ids([1])
                .build()
                .unwrap(),
        )
    }
}
