use datafusion::common::{Result, exec_datafusion_err};
use indicatif::{ProgressBar, ProgressStyle};

/// A progress bar on stderr over the `len` tables or files of a dataset, hidden when stderr is
/// not a terminal so that it never mixes with the dataset path printed on stdout.
pub(crate) fn dataset_progress(len: usize, unit: &str) -> Result<ProgressBar> {
    let style = ProgressStyle::with_template(&format!(
        "[{{elapsed_precise}}] {{bar:30}} {{pos}}/{{len}} {unit}  {{msg}}"
    ))
    .map_err(|e| exec_datafusion_err!("Invalid progress bar template: {e}"))?;
    Ok(ProgressBar::new(len as u64).with_style(style))
}
