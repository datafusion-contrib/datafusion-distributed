use datafusion::common::DataFusionError;

pub(crate) enum RetryOutcome {
    SameUrl,
    OtherUrl,
}

impl RetryOutcome {
    pub(crate) fn try_from_err(err: &DataFusionError) -> Option<RetryOutcome> {
        match err {
            DataFusionError::Context(info, _) => match info.as_str() {
                "RetryOutcome::SameUrl" => Some(RetryOutcome::SameUrl),
                "RetryOutcome::OtherUrl" => Some(RetryOutcome::OtherUrl),
                _ => None,
            },
            _ => None,
        }
    }

    pub(crate) fn tag(self, err: DataFusionError) -> DataFusionError {
        let ctx = match self {
            RetryOutcome::SameUrl => "RetryOutcome::SameUrl",
            RetryOutcome::OtherUrl => "RetryOutcome::OtherUrl",
        };
        DataFusionError::Context(ctx.to_string(), Box::new(err))
    }
}
