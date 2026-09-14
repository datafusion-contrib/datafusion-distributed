use datafusion::common::DataFusionError;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RetryOutcome {
    SameUrl,
    OtherUrl,
}

impl RetryOutcome {
    pub(crate) fn try_from_err(err: &DataFusionError) -> Option<RetryOutcome> {
        match err {
            DataFusionError::External(err) => {
                err.downcast_ref::<RetryableError>().map(|err| err.outcome)
            }
            _ => None,
        }
    }

    pub(crate) fn tag(self, err: DataFusionError) -> DataFusionError {
        DataFusionError::External(Box::new(RetryableError {
            outcome: self,
            source: err,
        }))
    }
}

#[derive(Debug)]
struct RetryableError {
    outcome: RetryOutcome,
    source: DataFusionError,
}

impl Display for RetryableError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, f)
    }
}

impl Error for RetryableError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_outcome_is_typed_not_parsed_from_error_text() {
        let misleading = DataFusionError::Execution("RetryOutcome::OtherUrl".to_string());
        assert_eq!(RetryOutcome::try_from_err(&misleading), None);

        let tagged = RetryOutcome::SameUrl.tag(misleading);
        assert_eq!(
            RetryOutcome::try_from_err(&tagged),
            Some(RetryOutcome::SameUrl)
        );
    }
}
