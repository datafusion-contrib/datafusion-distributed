use datafusion::error::DataFusionError;
use iceberg::{Error, ErrorKind};

/// Converts a datafusion error into an iceberg error.
pub fn iceberg_err(error: DataFusionError) -> Error {
    let fallback_message = error.to_string();
    let DataFusionError::Context(ctx, err) = error else {
        return Error::new(
            ErrorKind::Unexpected,
            format!("DataFusion execution failed: {fallback_message}"),
        );
    };

    let Some(kind) = parse_iceberg_error_kind(&ctx) else {
        return Error::new(
            ErrorKind::Unexpected,
            format!("DataFusion execution failed: {fallback_message}"),
        );
    };
    let DataFusionError::Execution(message) = err.as_ref() else {
        return Error::new(
            ErrorKind::Unexpected,
            format!("DataFusion execution failed: {fallback_message}"),
        );
    };

    Error::new(kind, strip_error_kind(kind, message))
}

/// Converts an Iceberg error into a DataFusion error.
pub fn df_err(error: Error) -> DataFusionError {
    DataFusionError::Context(
        format!("IcebergError({})", error.kind()),
        Box::new(DataFusionError::Execution(error.to_string())),
    )
}

fn parse_iceberg_error_kind(context: &str) -> Option<ErrorKind> {
    let kind = context.strip_prefix("IcebergError(")?.strip_suffix(')')?;

    match kind {
        "PreconditionFailed" => Some(ErrorKind::PreconditionFailed),
        "Unexpected" => Some(ErrorKind::Unexpected),
        "DataInvalid" => Some(ErrorKind::DataInvalid),
        "NamespaceAlreadyExists" => Some(ErrorKind::NamespaceAlreadyExists),
        "TableAlreadyExists" => Some(ErrorKind::TableAlreadyExists),
        "NamespaceNotFound" => Some(ErrorKind::NamespaceNotFound),
        "TableNotFound" => Some(ErrorKind::TableNotFound),
        "FeatureUnsupported" => Some(ErrorKind::FeatureUnsupported),
        "CatalogCommitConflicts" => Some(ErrorKind::CatalogCommitConflicts),
        _ => None,
    }
}

fn strip_error_kind(kind: ErrorKind, message: &str) -> String {
    let kind = kind.into_static();
    if message == kind {
        String::new()
    } else {
        message
            .strip_prefix(&format!("{kind} => "))
            .unwrap_or(message)
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::error::DataFusionError;
    use iceberg::{Error, ErrorKind};

    use super::{df_err, iceberg_err};

    #[test]
    fn roundtrips_iceberg_error_kind_and_message() {
        let error = Error::new(ErrorKind::DataInvalid, "invalid manifest");
        let roundtripped = iceberg_err(df_err(error));

        assert_eq!(roundtripped.kind(), ErrorKind::DataInvalid);
        assert_eq!(roundtripped.to_string(), "DataInvalid => invalid manifest");
    }

    #[test]
    fn encodes_iceberg_errors_with_native_datafusion_variants() {
        let error = df_err(Error::new(ErrorKind::DataInvalid, "invalid manifest"));

        assert!(matches!(
            error,
            DataFusionError::Context(context, inner)
                if context == "IcebergError(DataInvalid)"
                    && matches!(inner.as_ref(), DataFusionError::Execution(message) if message == "DataInvalid => invalid manifest")
        ));
    }

    #[test]
    fn maps_non_iceberg_datafusion_errors_to_unexpected() {
        let error = iceberg_err(DataFusionError::Execution("worker failed".to_string()));

        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert_eq!(
            error.to_string(),
            "Unexpected => DataFusion execution failed: Execution error: worker failed"
        );
    }
}
