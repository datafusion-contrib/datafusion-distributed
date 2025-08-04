use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct StageContext {
    /// Unique identifier of the Stage.
    pub id: Uuid,
    /// Number of tasks involved in the query.
    pub n_tasks: usize,
    /// Unique identifier of the input Stage.
    pub input_id: Uuid,
    /// Urls from which the current stage will need to read data.
    pub input_urls: Vec<Url>,
}

#[derive(Debug, Clone)]
pub struct StageTaskContext {
    /// Index of the current task in a stage
    pub task_idx: usize,
}
