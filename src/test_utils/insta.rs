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

/// LIMIT pushdown may keep a subset of generated parquet parts, and which
/// `part-N.parquet` files remain is not stable across platforms (part-0..2
/// locally vs part-1..3 on Linux CI). Grouping structure stays asserted.
fn redact_part_parquet(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(idx) = rest.find("part-") {
        out.push_str(&rest[..idx]);
        let after = &rest[idx + "part-".len()..];
        let digit_end = after
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(after.len());
        if digit_end > 0 && after[digit_end..].starts_with(".parquet") {
            out.push_str("part-<n>.parquet");
            rest = &after[digit_end + ".parquet".len()..];
        } else {
            out.push_str("part-");
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

fn normalize_display_noise(s: &str) -> String {
    redact_part_parquet(&strip_dynamic_rg_pruning(s))
}

struct NormalizeDisplayNoise;

impl Comparator for NormalizeDisplayNoise {
    fn matches(&self, reference: &Snapshot, test: &Snapshot) -> bool {
        match (reference.as_text(), test.as_text()) {
            (Some(a), Some(b)) => {
                normalize_display_noise(&a.to_string()) == normalize_display_noise(&b.to_string())
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
    settings.add_filter(r"part-\d+\.parquet", "part-<n>.parquet");
    settings.set_comparator(Box::new(NormalizeDisplayNoise));
    settings
}

#[cfg(test)]
mod tests {
    use super::{normalize_display_noise, redact_part_parquet, strip_dynamic_rg_pruning};

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

    #[test]
    fn redact_part_numbers_without_changing_grouping() {
        assert_eq!(
            redact_part_parquet(
                "[[/build_side/part-0.parquet:<int>..<int>], [/build_side/part-1.parquet:<int>..<int>]]"
            ),
            "[[/build_side/part-<n>.parquet:<int>..<int>], [/build_side/part-<n>.parquet:<int>..<int>]]"
        );
        assert_eq!(
            redact_part_parquet(
                "[[/build_side/part-1.parquet:<int>..<int>], [/build_side/part-2.parquet:<int>..<int>]]"
            ),
            "[[/build_side/part-<n>.parquet:<int>..<int>], [/build_side/part-<n>.parquet:<int>..<int>]]"
        );
        assert_eq!(redact_part_parquet("part-<n>.parquet"), "part-<n>.parquet");
        assert_eq!(
            normalize_display_noise(
                "part-1.parquet, dynamic_rg_pruning=eligible, pruning_predicate=x"
            ),
            "part-<n>.parquet, pruning_predicate=x"
        );
    }
}
