use ratatui::style::Color;
use std::time::Duration;

/// Format a byte count as a human-readable string.
/// Returns `"--"` for zero bytes.
pub fn format_bytes(bytes: u64) -> String {
    if bytes == 0 {
        "--".to_string()
    } else if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1_024 {
        format!("{:.1} KB", bytes as f64 / 1_024.0)
    } else {
        format!("{bytes} B")
    }
}

/// Format a duration as a compact human-readable string.
pub fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let millis = d.subsec_millis();
    if secs == 0 {
        format!("{millis}ms")
    } else if secs < 60 {
        format!("{secs}.{:01}s", millis / 100)
    } else {
        format!("{}m {}s", secs / 60, secs % 60)
    }
}

/// Format a row count with K/M suffixes.
/// Returns `"--"` for zero rows.
pub fn format_row_count(rows: u64) -> String {
    if rows == 0 {
        "--".to_string()
    } else if rows >= 1_000_000 {
        format!("{:.1}M", rows as f64 / 1_000_000.0)
    } else if rows >= 1_000 {
        format!("{:.1}K", rows as f64 / 1_000.0)
    } else {
        rows.to_string()
    }
}

/// Format a rows-per-second throughput value.
pub fn format_rows_throughput(rows_per_sec: f64) -> String {
    if rows_per_sec >= 1_000_000.0 {
        format!("{:.1}M rows out/s", rows_per_sec / 1_000_000.0)
    } else if rows_per_sec >= 1_000.0 {
        format!("{:.1}K rows out/s", rows_per_sec / 1_000.0)
    } else {
        format!("{rows_per_sec:.0} rows out/s")
    }
}

/// Return a color for CPU usage percentage.
pub fn cpu_color(pct: f64) -> Color {
    if pct > 95.0 {
        Color::Red
    } else if pct > 80.0 {
        Color::Yellow
    } else {
        Color::Green
    }
}
