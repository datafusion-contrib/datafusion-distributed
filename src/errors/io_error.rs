use std::io::ErrorKind;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IoErrorProto {
    #[prost(string, tag = "1")]
    pub msg: String,
    #[prost(int32, tag = "2")]
    pub code: i32,
    #[prost(string, tag = "3")]
    pub err: String,
}

impl IoErrorProto {
    pub(crate) fn from_io_error(msg: &str, err: &std::io::Error) -> Self {
        Self {
            msg: msg.to_string(),
            code: match err.kind() {
                ErrorKind::NotFound => 0,
                ErrorKind::PermissionDenied => 1,
                ErrorKind::ConnectionRefused => 2,
                ErrorKind::ConnectionReset => 3,
                ErrorKind::HostUnreachable => 4,
                ErrorKind::NetworkUnreachable => 5,
                ErrorKind::ConnectionAborted => 6,
                ErrorKind::NotConnected => 7,
                ErrorKind::AddrInUse => 8,
                ErrorKind::AddrNotAvailable => 9,
                ErrorKind::NetworkDown => 10,
                ErrorKind::BrokenPipe => 11,
                ErrorKind::AlreadyExists => 12,
                ErrorKind::WouldBlock => 13,
                ErrorKind::NotADirectory => 14,
                ErrorKind::IsADirectory => 15,
                ErrorKind::DirectoryNotEmpty => 16,
                ErrorKind::ReadOnlyFilesystem => 17,
                ErrorKind::StaleNetworkFileHandle => 18,
                ErrorKind::InvalidInput => 19,
                ErrorKind::InvalidData => 20,
                ErrorKind::TimedOut => 21,
                ErrorKind::WriteZero => 22,
                ErrorKind::StorageFull => 23,
                ErrorKind::NotSeekable => 24,
                ErrorKind::QuotaExceeded => 25,
                ErrorKind::FileTooLarge => 26,
                ErrorKind::ResourceBusy => 27,
                ErrorKind::ExecutableFileBusy => 28,
                ErrorKind::Deadlock => 29,
                ErrorKind::CrossesDevices => 30,
                ErrorKind::TooManyLinks => 31,
                ErrorKind::ArgumentListTooLong => 32,
                ErrorKind::Interrupted => 33,
                ErrorKind::Unsupported => 34,
                ErrorKind::UnexpectedEof => 35,
                ErrorKind::OutOfMemory => 36,
                ErrorKind::Other => 37,
                _ => -1,
            },
            err: err.to_string(),
        }
    }

    pub(crate) fn to_io_error(&self) -> (std::io::Error, String) {
        let kind = match self.code {
            0 => ErrorKind::NotFound,
            1 => ErrorKind::PermissionDenied,
            2 => ErrorKind::ConnectionRefused,
            3 => ErrorKind::ConnectionReset,
            4 => ErrorKind::HostUnreachable,
            5 => ErrorKind::NetworkUnreachable,
            6 => ErrorKind::ConnectionAborted,
            7 => ErrorKind::NotConnected,
            8 => ErrorKind::AddrInUse,
            9 => ErrorKind::AddrNotAvailable,
            10 => ErrorKind::NetworkDown,
            11 => ErrorKind::BrokenPipe,
            12 => ErrorKind::AlreadyExists,
            13 => ErrorKind::WouldBlock,
            14 => ErrorKind::NotADirectory,
            15 => ErrorKind::IsADirectory,
            16 => ErrorKind::DirectoryNotEmpty,
            17 => ErrorKind::ReadOnlyFilesystem,
            18 => ErrorKind::StaleNetworkFileHandle,
            19 => ErrorKind::InvalidInput,
            20 => ErrorKind::InvalidData,
            21 => ErrorKind::TimedOut,
            22 => ErrorKind::WriteZero,
            23 => ErrorKind::StorageFull,
            24 => ErrorKind::NotSeekable,
            25 => ErrorKind::QuotaExceeded,
            26 => ErrorKind::FileTooLarge,
            27 => ErrorKind::ResourceBusy,
            28 => ErrorKind::ExecutableFileBusy,
            29 => ErrorKind::Deadlock,
            30 => ErrorKind::CrossesDevices,
            31 => ErrorKind::TooManyLinks,
            32 => ErrorKind::ArgumentListTooLong,
            33 => ErrorKind::Interrupted,
            34 => ErrorKind::Unsupported,
            35 => ErrorKind::UnexpectedEof,
            36 => ErrorKind::OutOfMemory,
            37 => ErrorKind::Other,
            _ => ErrorKind::Other,
        };
        (
            std::io::Error::new(kind, self.err.clone()),
            self.msg.clone(),
        )
    }
}
