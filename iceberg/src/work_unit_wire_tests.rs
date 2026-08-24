#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::expr::{Bind, Reference};
    use iceberg::spec::{Datum, NestedField, PrimitiveType, Transform};

    #[test]
    fn roundtrips_complete_partitioned_task() {
        let task = sample_task("file.parquet", 10);

        assert_eq!(roundtrip(task.clone()).unwrap(), task);
    }

    #[test]
    fn defines_reuses_and_replaces_shared_context() {
        let mut encoder = FileScanTaskEncoder::default();
        let first = encoder.encode(sample_task("first.parquet", 10)).unwrap();
        let second = encoder.encode(sample_task("second.parquet", 20)).unwrap();
        let mut changed_task = sample_task("third.parquet", 30);
        changed_task.case_sensitive = false;
        let changed = encoder.encode(changed_task).unwrap();

        assert_eq!(context_state(&first), (1, true));
        assert_eq!(context_state(&second), (1, false));
        assert_eq!(context_state(&changed), (2, true));

        let mut decoder = FileScanTaskDecoder::default();
        assert_eq!(decode(&mut decoder, first).unwrap().data_file_path, "first.parquet");
        assert_eq!(decode(&mut decoder, second).unwrap().data_file_path, "second.parquet");
        assert!(!decode(&mut decoder, changed).unwrap().case_sensitive);
    }

    #[test]
    fn omits_partition_values_when_dependency_omits_their_spec() {
        let mut task = sample_task("file.parquet", 10);
        task.partition_spec = None;

        let decoded = roundtrip(task).unwrap();

        assert!(decoded.partition.is_none());
        assert!(decoded.partition_spec.is_none());
    }

    #[test]
    fn roundtrips_iceberg_primitive_value_bytes() {
        let mut datums = vec![
            Datum::bool(true),
            Datum::int(-12),
            Datum::long(i64::MIN + 1),
            Datum::float(f32::from_bits(0x8000_0000)),
            Datum::double(f64::from_bits(0x7ff8_0000_0000_0001)),
            Datum::string("iceberg"),
            Datum::binary([0, 1, 255]),
            Datum::fixed([0, 1, 255]),
            Datum::date(1),
            Datum::timestamp_micros(1),
        ];
        datums.push(
            Datum::try_from_bytes(&i128::MIN.to_be_bytes(), PrimitiveType::Decimal {
                precision: 38,
                scale: 0,
            })
            .unwrap(),
        );
        datums.push(Datum::try_from_bytes(&u128::MAX.to_be_bytes(), PrimitiveType::Uuid).unwrap());

        for datum in datums {
            let bytes = primitive_literal_bytes(datum.literal().clone()).unwrap();
            let decoded = Datum::try_from_bytes(&bytes, datum.data_type().clone()).unwrap();
            assert_eq!(decoded, datum);
        }
        for sentinel in [PrimitiveLiteral::AboveMax, PrimitiveLiteral::BelowMin] {
            assert!(primitive_literal_bytes(sentinel).is_err());
        }
    }

    #[test]
    fn rejects_non_primitive_partition_values() {
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
    fn rejects_invalid_decoded_tasks() {
        assert_rejected(
            |wire| {
                wire.body.start = u64::MAX;
                wire.body.length = 2;
                wire.body.file_size_in_bytes = u64::MAX;
            },
            "overflows",
        );
        assert_rejected(
            |wire| wire.body.partition.as_mut().unwrap().clear(),
            "partition values for 1 partition fields",
        );
        assert_rejected(
            |wire| wire.body.partition = Some(vec![Some(vec![0; 3])]),
            "invalid Iceberg partition value 0",
        );
        assert_rejected(
            |wire| wire.context.as_mut().unwrap().project_field_ids.push(99),
            "projects unknown field id 99",
        );
    }

    #[test]
    fn failed_task_does_not_cache_its_context() {
        let valid = encoded_task();
        let context_id = wire(&valid).context_id;
        let invalid = edit(valid.clone(), |wire| {
            wire.body.start = u64::MAX;
            wire.body.length = 2;
            wire.body.file_size_in_bytes = u64::MAX;
        });
        let reference = edit(valid, |wire| wire.context = None);
        let mut decoder = FileScanTaskDecoder::default();

        assert!(decoder.decode(invalid).is_err());
        let error = decoder.decode(reference).unwrap_err();

        assert!(error
            .to_string()
            .contains(&format!("undefined context id {context_id}")));
    }

    #[test]
    fn rebinds_partition_spec_even_when_partition_value_is_null() {
        let message = edit(encoded_task(), |wire| {
            let context = wire.context.as_mut().unwrap();
            context.schema = mismatched_schema();
            context.project_field_ids = vec![2];
            context.predicate = None;
            wire.body.partition = Some(vec![None]);
        });

        assert_decode_error(message, "Cannot find partition source field with id `1`");
    }

    fn roundtrip(task: FileScanTask) -> Result<FileScanTask> {
        let message = FileScanTaskEncoder::default().encode(task)?;
        decode(&mut FileScanTaskDecoder::default(), message)
    }

    fn decode(
        decoder: &mut FileScanTaskDecoder,
        message: FileScanTaskMessage,
    ) -> Result<FileScanTask> {
        decoder.decode(prost_roundtrip(message))
    }

    fn context_state(message: &FileScanTaskMessage) -> (u32, bool) {
        let wire = wire(message);
        (wire.context_id, wire.context.is_some())
    }

    fn encoded_task() -> FileScanTaskMessage {
        FileScanTaskEncoder::default()
            .encode(sample_task("file.parquet", 10))
            .unwrap()
    }

    fn edit(
        message: FileScanTaskMessage,
        edit: impl FnOnce(&mut FileScanTaskWire),
    ) -> FileScanTaskMessage {
        let mut wire = message.into_wire().unwrap();
        edit(&mut wire);
        FileScanTaskMessage::from_wire(&wire).unwrap()
    }

    fn wire(message: &FileScanTaskMessage) -> FileScanTaskWire {
        message.clone().into_wire().unwrap()
    }

    fn prost_roundtrip(message: FileScanTaskMessage) -> FileScanTaskMessage {
        FileScanTaskMessage::decode(message.encode_to_vec().as_slice()).unwrap()
    }

    #[track_caller]
    fn assert_rejected(edit_wire: impl FnOnce(&mut FileScanTaskWire), expected: &str) {
        assert_decode_error(edit(encoded_task(), edit_wire), expected);
    }

    #[track_caller]
    fn assert_decode_error(message: FileScanTaskMessage, expected: &str) {
        let error = FileScanTaskDecoder::default()
            .decode(prost_roundtrip(message))
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }

    fn sample_task(path: &str, partition_value: i32) -> FileScanTask {
        let schema = test_schema();
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
            deletes: vec![],
            partition: Some([Some(Literal::int(partition_value))].into_iter().collect()),
            partition_spec: Some(partition_spec),
            name_mapping: None,
            case_sensitive: true,
        }
    }

    fn test_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_schema_id(3)
                .with_fields([Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .with_identifier_field_ids([1])
                .build()
                .unwrap(),
        )
    }

    fn mismatched_schema() -> SchemaRef {
        Arc::new(
            Schema::builder()
                .with_fields([Arc::new(NestedField::optional(
                    2,
                    "other",
                    Type::Primitive(PrimitiveType::Int),
                ))])
                .build()
                .unwrap(),
        )
    }
}
