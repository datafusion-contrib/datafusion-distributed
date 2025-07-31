use datafusion::common::{Column, SchemaError, TableReference};

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaErrorProto {
    #[prost(oneof = "SchemaErrorInnerProto", tags = "1")]
    pub inner: Option<SchemaErrorInnerProto>,
    #[prost(string, optional, tag = "2")]
    pub backtrace: Option<String>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum SchemaErrorInnerProto {
    #[prost(message, tag = "1")]
    AmbiguousReference(AmbiguousReferenceProto),
    #[prost(message, tag = "2")]
    DuplicateQualifiedField(DuplicateQualifiedFieldProto),
    #[prost(message, tag = "3")]
    DuplicateUnqualifiedField(DuplicateUnqualifiedFieldProto),
    #[prost(message, tag = "4")]
    FieldNotFound(FieldNotFoundProto),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AmbiguousReferenceProto {
    #[prost(message, tag = "1")]
    field: Option<ColumnProto>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DuplicateQualifiedFieldProto {
    #[prost(message, tag = "1")]
    qualifier: Option<TableReferenceProto>,
    #[prost(string, tag = "2")]
    name: String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DuplicateUnqualifiedFieldProto {
    #[prost(string, tag = "1")]
    name: String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FieldNotFoundProto {
    #[prost(message, boxed, tag = "1")]
    field: Option<Box<ColumnProto>>,
    #[prost(message, repeated, tag = "2")]
    valid_fields: Vec<ColumnProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnProto {
    #[prost(message, tag = "1")]
    pub relation: Option<TableReferenceProto>,
    #[prost(string, tag = "2")]
    pub name: String,
    // No spans
}

impl ColumnProto {
    pub fn from_column(v: &Column) -> Self {
        ColumnProto {
            relation: v
                .relation
                .as_ref()
                .map(TableReferenceProto::from_table_reference),
            name: v.name.to_string(),
        }
    }

    pub fn to_column(&self) -> Column {
        Column::new(
            self.relation.as_ref().map(|v| v.to_table_reference()),
            self.name.clone(),
        )
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableReferenceProto {
    #[prost(oneof = "TableReferenceInnerProto", tags = "1")]
    pub inner: Option<TableReferenceInnerProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum TableReferenceInnerProto {
    #[prost(message, tag = "1")]
    Bare(TableReferenceBareProto),
    #[prost(message, tag = "2")]
    Partial(TableReferencePartialProto),
    #[prost(message, tag = "3")]
    Full(TableReferenceFullProto),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableReferenceBareProto {
    #[prost(string, tag = "1")]
    pub table: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableReferencePartialProto {
    #[prost(string, tag = "1")]
    pub schema: String,
    #[prost(string, tag = "2")]
    pub table: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableReferenceFullProto {
    #[prost(string, tag = "1")]
    pub catalog: String,
    #[prost(string, tag = "2")]
    pub schema: String,
    #[prost(string, tag = "3")]
    pub table: String,
}

impl TableReferenceProto {
    pub fn from_table_reference(v: &TableReference) -> Self {
        match v {
            TableReference::Bare { table } => TableReferenceProto {
                inner: Some(TableReferenceInnerProto::Bare(TableReferenceBareProto {
                    table: table.to_string(),
                })),
            },
            TableReference::Partial { schema, table } => TableReferenceProto {
                inner: Some(TableReferenceInnerProto::Partial(
                    TableReferencePartialProto {
                        schema: schema.to_string(),
                        table: table.to_string(),
                    },
                )),
            },
            TableReference::Full {
                catalog,
                schema,
                table,
            } => TableReferenceProto {
                inner: Some(TableReferenceInnerProto::Full(TableReferenceFullProto {
                    catalog: catalog.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                })),
            },
        }
    }

    pub fn to_table_reference(&self) -> TableReference {
        let Some(ref inner) = self.inner else {
            return TableReference::bare("");
        };

        match inner {
            TableReferenceInnerProto::Bare(msg) => TableReference::Bare {
                table: msg.table.clone().into(),
            },
            TableReferenceInnerProto::Partial(msg) => TableReference::Partial {
                schema: msg.schema.clone().into(),
                table: msg.table.clone().into(),
            },
            TableReferenceInnerProto::Full(msg) => TableReference::Full {
                catalog: msg.catalog.clone().into(),
                schema: msg.schema.clone().into(),
                table: msg.table.clone().into(),
            },
        }
    }
}

impl SchemaErrorProto {
    pub fn from_schema_error(err: &SchemaError, backtrace: Option<&String>) -> Self {
        match err {
            SchemaError::AmbiguousReference { ref field } => SchemaErrorProto {
                inner: Some(SchemaErrorInnerProto::AmbiguousReference(
                    AmbiguousReferenceProto {
                        field: Some(ColumnProto::from_column(field)),
                    },
                )),
                backtrace: backtrace.cloned(),
            },
            SchemaError::DuplicateQualifiedField { qualifier, name } => SchemaErrorProto {
                inner: Some(SchemaErrorInnerProto::DuplicateQualifiedField(
                    DuplicateQualifiedFieldProto {
                        qualifier: Some(TableReferenceProto::from_table_reference(qualifier)),
                        name: name.to_string(),
                    },
                )),
                backtrace: backtrace.cloned(),
            },
            SchemaError::DuplicateUnqualifiedField { name } => SchemaErrorProto {
                inner: Some(SchemaErrorInnerProto::DuplicateUnqualifiedField(
                    DuplicateUnqualifiedFieldProto {
                        name: name.to_string(),
                    },
                )),
                backtrace: backtrace.cloned(),
            },
            SchemaError::FieldNotFound {
                field,
                valid_fields,
            } => SchemaErrorProto {
                inner: Some(SchemaErrorInnerProto::FieldNotFound(FieldNotFoundProto {
                    field: Some(Box::new(ColumnProto::from_column(&field))),
                    valid_fields: valid_fields.iter().map(ColumnProto::from_column).collect(),
                })),
                backtrace: backtrace.cloned(),
            },
        }
    }

    pub fn to_schema_error(&self) -> (SchemaError, Option<String>) {
        let Some(ref inner) = self.inner else {
            // Found no better default.
            return (
                SchemaError::FieldNotFound {
                    field: Box::new(Column::new_unqualified("".to_string())),
                    valid_fields: vec![],
                },
                None,
            );
        };

        let err = match inner {
            SchemaErrorInnerProto::AmbiguousReference(err) => SchemaError::AmbiguousReference {
                field: err
                    .field
                    .as_ref()
                    .map(|v| v.to_column())
                    .unwrap_or(Column::new_unqualified("".to_string())),
            },
            SchemaErrorInnerProto::DuplicateQualifiedField(err) => {
                SchemaError::DuplicateQualifiedField {
                    qualifier: Box::new(
                        err.qualifier
                            .as_ref()
                            .map(|v| v.to_table_reference())
                            .unwrap_or(TableReference::Bare { table: "".into() }),
                    ),
                    name: err.name.clone(),
                }
            }
            SchemaErrorInnerProto::DuplicateUnqualifiedField(err) => {
                SchemaError::DuplicateUnqualifiedField {
                    name: err.name.clone(),
                }
            }
            SchemaErrorInnerProto::FieldNotFound(err) => SchemaError::FieldNotFound {
                field: Box::new(
                    err.field
                        .as_ref()
                        .map(|v| v.to_column())
                        .unwrap_or(Column::new_unqualified("".to_string())),
                ),
                valid_fields: err.valid_fields.iter().map(|v| v.to_column()).collect(),
            },
        };
        (err, self.backtrace.clone())
    }
}
