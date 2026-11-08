use datafusion::common::Statistics;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone, PartialEq)]
pub(super) enum Complexity {
    /// Constant complexity
    Constant(f32),
    /// Linear with a specific column from a specific child.
    Linear(LinearComplexity),
    /// NLogM
    Log(Box<Complexity>, Box<Complexity>),
    /// N+M
    Plus(Box<Complexity>, Box<Complexity>),
    /// N*M
    Multiply(Box<Complexity>, Box<Complexity>),
}

#[derive(Clone, PartialEq, Eq)]
pub(super) enum LinearComplexity {
    /// Depends on linearly with the input column with the provided index
    Column(usize),
    /// Depends on linearly with the all the input columns
    AllColumns,
    /// Depends on linearly with the input column with the provided index from the left child
    ColumnFromLeft(usize),
    /// Depends on linearly with the all the input columns from the left child
    AllColumnsFromLeft,
    /// Depends on linearly with the input column with the provided index from the right child
    ColumnFromRight(usize),
    /// Depends on linearly with the all the input columns from the right child
    AllColumnsFromRight,
    /// Depends on linearly with the all the output columns
    AllOutputColumns,
}

impl Complexity {
    pub(super) fn log(self, other: Self) -> Self {
        match (self, other) {
            (Self::Constant(n), Self::Constant(m)) => Self::Constant(n * m.log2()),
            (s, o) => Self::Log(Box::new(s), Box::new(o)),
        }
    }

    pub(super) fn plus(self, other: Self) -> Self {
        match (self, other) {
            (Self::Constant(n), Self::Constant(m)) => Self::Constant(n + m),
            // (A + k1) + k2 = A + (k1 + k2): bubble constants rightward so they can fold
            (Self::Plus(a, b), Self::Constant(m)) if matches!(*b, Self::Constant(_)) => {
                (*a).plus((*b).plus(Self::Constant(m)))
            }
            (s, o) if s == o => Self::Constant(2.).multiply(s),
            (s, o) => Self::Plus(Box::new(s), Box::new(o)),
        }
    }

    pub(super) fn multiply(self, other: Self) -> Self {
        match (self, other) {
            (Self::Constant(n), Self::Constant(m)) => Self::Constant(n * m),
            (s, o) => Self::Multiply(Box::new(s), Box::new(o)),
        }
    }

    /// Computes the total bytes processed given per-child row counts.
    /// Returns None if statistics are unavailable for any required input.
    pub(super) fn cost(
        &self,
        output_stat: &Arc<Statistics>,
        input_stats: &[Arc<Statistics>],
    ) -> Option<usize> {
        Some(match self {
            Self::Constant(v) => *v as usize,
            Self::Linear(linear) => match linear {
                LinearComplexity::Column(i) => {
                    let col_stats = &input_stats.first()?.column_statistics;
                    *col_stats.get(*i)?.byte_size.get_value()?
                }
                LinearComplexity::AllColumns => {
                    *input_stats.first()?.total_byte_size.get_value()?
                }
                LinearComplexity::ColumnFromLeft(i) => {
                    let col_stats = &input_stats.first()?.column_statistics;
                    *col_stats.get(*i)?.byte_size.get_value()?
                }
                LinearComplexity::AllColumnsFromLeft => {
                    *input_stats.first()?.total_byte_size.get_value()?
                }
                LinearComplexity::ColumnFromRight(i) => {
                    let col_stats = &input_stats.last()?.column_statistics;
                    *col_stats.get(*i)?.byte_size.get_value()?
                }
                LinearComplexity::AllColumnsFromRight => {
                    *input_stats.last()?.total_byte_size.get_value()?
                }
                LinearComplexity::AllOutputColumns => *output_stat.total_byte_size.get_value()?,
            },
            Self::Log(n, m) => {
                let n = n.cost(output_stat, input_stats)?;
                let m = m.cost(output_stat, input_stats)?;
                // `ilog2` panics on 0, which happens whenever the logged input has zero estimated
                // bytes/rows (e.g. an empty or fully-pruned relation). Flooring at 1 makes log2
                // contribute 0 there, i.e. sorting/merging nothing costs nothing.
                n * m.checked_ilog2().unwrap_or(0) as usize
            }
            Self::Plus(n, m) => n
                .cost(output_stat, input_stats)?
                .saturating_add(m.cost(output_stat, input_stats)?),
            Self::Multiply(n, m) => n
                .cost(output_stat, input_stats)?
                .saturating_mul(m.cost(output_stat, input_stats)?),
        })
    }
}

impl Debug for Complexity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn trim_parenthesis(dbg: &Complexity) -> String {
            let s = format!("{dbg:?}");
            if s.starts_with('(') && s.ends_with(')') {
                s[1..s.len() - 1].to_string()
            } else {
                s
            }
        }
        match self {
            Self::Constant(v) => write!(f, "{v}"),
            Self::Linear(linear) => match linear {
                LinearComplexity::Column(i) => write!(f, "Col{i}"),
                LinearComplexity::AllColumns => write!(f, "Cols"),
                LinearComplexity::ColumnFromLeft(i) => write!(f, "left_Col{i}"),
                LinearComplexity::AllColumnsFromLeft => write!(f, "left_Cols"),
                LinearComplexity::ColumnFromRight(i) => write!(f, "right_Col{i}"),
                LinearComplexity::AllColumnsFromRight => write!(f, "right_Cols"),
                LinearComplexity::AllOutputColumns => write!(f, "out_Cols"),
            },
            Self::Log(n, m) => write!(f, "{n:?}*Log({m:?})"),
            Self::Plus(n, m) => {
                if matches!(n.as_ref(), &Self::Plus(_, _)) {
                    write!(f, "({}+{m:?})", trim_parenthesis(n))
                } else {
                    write!(f, "({n:?}+{m:?})")
                }
            }
            Self::Multiply(n, m) => {
                if matches!(n.as_ref(), &Self::Multiply(_, _)) {
                    write!(f, "({}*{m:?})", trim_parenthesis(n))
                } else {
                    write!(f, "({n:?}*{m:?})")
                }
            }
        }
    }
}
