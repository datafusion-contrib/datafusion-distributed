use std::env;

use insta::Snapshot;
use insta::comparator::{Comparator, DefaultComparator};

pub use insta;

#[macro_export]
macro_rules! assert_snapshot {
    ($($arg:tt)*) => {
        $crate::test_utils::insta::settings().bind(|| {
            $crate::test_utils::insta::insta::assert_snapshot!($($arg)*);
        })
    };
}

/// DataFusion may append this when a dynamic filter is eligible for parquet
/// row-group pruning. Eligibility is statistics-dependent and unrelated to DFD
/// rewrites.
///
/// Insta filters run only on the generated value, not the stored inline
/// snapshot. Linux CI still diffs against stored lines that carry this suffix,
/// so comparison also strips it from both sides.
const DYNAMIC_RG_PRUNING: &str = ", dynamic_rg_pruning=";

fn strip_dynamic_rg_pruning(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(idx) = rest.find(DYNAMIC_RG_PRUNING) {
        out.push_str(&rest[..idx]);
        rest = &rest[idx + DYNAMIC_RG_PRUNING.len()..];
        let value_len = rest
            .find(|c: char| c.is_whitespace() || c == ',')
            .unwrap_or(rest.len());
        rest = &rest[value_len..];
    }
    out.push_str(rest);
    out
}

struct StripDynamicRgPruning;

impl Comparator for StripDynamicRgPruning {
    fn matches(&self, reference: &Snapshot, test: &Snapshot) -> bool {
        match (reference.as_text(), test.as_text()) {
            (Some(a), Some(b)) => {
                strip_dynamic_rg_pruning(&a.to_string()) == strip_dynamic_rg_pruning(&b.to_string())
            }
            _ => DefaultComparator.matches(reference, test),
        }
    }

    fn dyn_clone(&self) -> Box<dyn Comparator> {
        Box::new(Self)
    }
}

pub fn settings() -> insta::Settings {
    // Safety: this is only used in tests, it may panic if used in parallel with other tests.
    unsafe { env::set_var("INSTA_WORKSPACE_ROOT", env!("CARGO_MANIFEST_DIR")) };
    let mut settings = insta::Settings::clone_current();
    let cwd = env::current_dir().unwrap();
    let cwd = cwd.to_str().unwrap();
    settings.add_filter(cwd.trim_start_matches("/"), "");
    settings.add_filter(
        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
        "UUID",
    );
    settings.add_filter(r"\d+\.\.\d+", "<int>..<int>");
    // A hash join pushes its build-side values into a DynamicFilter's IN-list in whatever
    // order the build collected them, which is scheduler-dependent. Only redact inside
    // DynamicFilter: static IN (SET) predicates are deterministic and stay asserted.
    settings.add_filter(
        r"(DynamicFilter \[[^\[\]]*IN \(SET\) \(\[)[^\]]*(\]\))",
        "${1}<values>${2}",
    );
    // Do not use `\S+`: that swallows the comma before a following field such as
    // `pruning_predicate=`.
    settings.add_filter(r", dynamic_rg_pruning=[^\s,]+", "");
    settings.set_comparator(Box::new(StripDynamicRgPruning));
    settings
}

#[cfg(test)]
mod tests {
    use super::strip_dynamic_rg_pruning;

    #[test]
    fn strip_rg_pruning_at_eol_and_before_next_field() {
        assert_eq!(
            strip_dynamic_rg_pruning("pred, dynamic_rg_pruning=eligible"),
            "pred"
        );
        assert_eq!(
            strip_dynamic_rg_pruning("pred, dynamic_rg_pruning=eligible, pruning_predicate=x"),
            "pred, pruning_predicate=x"
        );
    }
}
