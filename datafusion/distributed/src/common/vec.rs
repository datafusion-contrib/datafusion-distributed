use datafusion::common::internal_err;
use datafusion::error::Result;
use num_traits::AsPrimitive;
use std::ops::{AddAssign, DivAssign, MulAssign};

/// Converts a slice of type `I` to a `Vec<O>` using `as`-style primitive casting.
pub(crate) fn vec_cast<I, O>(input: &[I]) -> Vec<O>
where
    I: AsPrimitive<O>,
    O: Copy + 'static,
{
    input.iter().map(|v| v.as_()).collect()
}

/// Adds each element of `other` into the corresponding element of `one`, converting types via `AsPrimitive`.
pub(crate) fn element_wise_sum<I, O>(mut one: Vec<I>, other: &[O]) -> Result<Vec<I>>
where
    I: AddAssign + Copy + 'static,
    O: AsPrimitive<I> + 'static,
{
    if one.len() != other.len() {
        return internal_err!("Cannot do an element wise sum of two vectors of different lengths");
    }
    for i in 0..one.len() {
        one[i] += other[i].as_();
    }
    Ok(one)
}

/// Multiplies every element of `one` by the scalar `other`, converting types via `AsPrimitive`.
pub(crate) fn vec_mul<I, O>(mut one: Vec<I>, other: O) -> Vec<I>
where
    I: MulAssign + Copy + 'static,
    O: AsPrimitive<I> + 'static,
{
    for el in one.iter_mut() {
        *el *= other.as_();
    }
    one
}

/// Divides every element of `one` by the scalar `other`, converting types via `AsPrimitive`.
pub(crate) fn vec_div<I, O>(mut one: Vec<I>, other: O) -> Vec<I>
where
    I: DivAssign + Copy + 'static,
    O: AsPrimitive<I> + 'static,
{
    for el in one.iter_mut() {
        *el /= other.as_();
    }
    one
}

/// Reduces a collection of same-length `f32` vectors into a single vector by averaging element-wise.
/// Empty inner vecs are skipped; returns an empty vec if all inputs are empty.
pub(crate) fn vec_avg_reduce(vecs: Vec<Vec<f32>>) -> Result<Vec<f32>> {
    let sample_count = vecs.len();
    let mut iter = vecs.into_iter();
    let mut acc = loop {
        let Some(v) = iter.next() else {
            return Ok(vec![]);
        };
        if !v.is_empty() {
            break v;
        }
    };
    for v in iter {
        if v.is_empty() {
            continue;
        } else if acc.len() != v.len() {
            return internal_err!(
                "vec_avg_reduce: length mismatch — first vec has {} elements, got {}",
                acc.len(),
                v.len()
            );
        }
        acc = element_wise_sum(acc, &v)?;
    }
    Ok(vec_div(acc, sample_count as f32))
}
