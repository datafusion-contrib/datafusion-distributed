use core::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use datafusion::common::internal_datafusion_err;
use datafusion::error::Result;

use url::Url;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExecutionTask {
    /// The url of the worker that will execute this task.  A None value is interpreted as
    /// unassinged.
    #[prost(string, optional, tag = "1")]
    pub url_str: Option<String>,
    /// The partitions that we can execute from this plan
    #[prost(uint64, repeated, tag = "2")]
    pub partition_group: Vec<u64>,
}

impl ExecutionTask {
    pub fn new(partition_group: Vec<u64>) -> Self {
        ExecutionTask {
            url_str: None,
            partition_group,
        }
    }

    pub fn with_assignment(mut self, url: &Url) -> Self {
        self.url_str = Some(format!("{url}"));
        self
    }

    /// Returns the url of this worker, a None is unassigned
    pub fn url(&self) -> Result<Option<Url>> {
        self.url_str
            .as_ref()
            .map(|u| Url::parse(u).map_err(|_| internal_datafusion_err!("Invalid URL: {}", u)))
            .transpose()
    }
}

impl Display for ExecutionTask {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Task: partitions: {},{}]",
            format_pg(&self.partition_group),
            self.url()
                .map_err(|_| std::fmt::Error {})?
                .map(|u| u.to_string())
                .unwrap_or("unassigned".to_string())
        )
    }
}

pub(crate) fn format_pg(partition_group: &[u64]) -> String {
    if partition_group.len() > 2 {
        format!(
            "{}..{}",
            partition_group[0],
            partition_group[partition_group.len() - 1]
        )
    } else {
        partition_group
            .iter()
            .map(|pg| format!("{pg}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}
