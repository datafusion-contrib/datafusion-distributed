use datafusion::sql::sqlparser::parser::ParserError;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParserErrorProto {
    #[prost(oneof = "ParserErrorInnerProto", tags = "1,2,3")]
    pub inner: Option<ParserErrorInnerProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ParserErrorInnerProto {
    #[prost(string, tag = "1")]
    TokenizerError(String),
    #[prost(string, tag = "2")]
    ParserError(String),
    #[prost(bool, tag = "3")]
    RecursionLimitExceeded(bool),
}

impl ParserErrorProto {
    pub fn from_parser_error(err: &ParserError) -> Self {
        match err {
            ParserError::TokenizerError(msg) => ParserErrorProto {
                inner: Some(ParserErrorInnerProto::TokenizerError(msg.to_string())),
            },
            ParserError::ParserError(msg) => ParserErrorProto {
                inner: Some(ParserErrorInnerProto::ParserError(msg.to_string())),
            },
            ParserError::RecursionLimitExceeded => ParserErrorProto {
                inner: Some(ParserErrorInnerProto::RecursionLimitExceeded(true)),
            },
        }
    }

    pub fn to_parser_error(&self) -> ParserError {
        let Some(ref inner) = self.inner else {
            return ParserError::ParserError("Malformed protobuf message".to_string());
        };

        match inner {
            ParserErrorInnerProto::TokenizerError(msg) => {
                ParserError::TokenizerError(msg.to_string())
            }
            ParserErrorInnerProto::ParserError(msg) => ParserError::ParserError(msg.to_string()),
            ParserErrorInnerProto::RecursionLimitExceeded(_) => ParserError::RecursionLimitExceeded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::parser::ParserError;
    use prost::Message;

    #[test]
    fn test_parser_error_roundtrip() {
        let test_cases = vec![
            ParserError::ParserError("syntax error".to_string()),
            ParserError::TokenizerError("tokenizer error".to_string()),
            ParserError::RecursionLimitExceeded,
        ];

        for original_error in test_cases {
            let proto = ParserErrorProto::from_parser_error(&original_error);
            let proto = ParserErrorProto::decode(proto.encode_to_vec().as_ref()).unwrap();
            let recovered_error = proto.to_parser_error();

            assert_eq!(original_error.to_string(), recovered_error.to_string());
        }
    }

    #[test]
    fn test_malformed_protobuf_message() {
        let malformed_proto = ParserErrorProto { inner: None };
        let recovered_error = malformed_proto.to_parser_error();
        assert!(matches!(recovered_error, ParserError::ParserError(_)));
    }
}
