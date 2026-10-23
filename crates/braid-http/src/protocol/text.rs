//! Text ranges count Unicode scalar values. Every patch addresses the same base.
use crate::{
    error::{BraidError, Result},
    types::Patch,
};

pub fn text_range(range: &str, len: usize) -> Result<(usize, usize)> {
    if range.is_empty() || range == "everything" {
        return Ok((0, len));
    }
    let range = range
        .strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(range);
    let (a, b) = range
        .split_once(':')
        .ok_or_else(|| BraidError::Protocol("Invalid text range".into()))?;
    let start = if a.is_empty() {
        0
    } else {
        a.parse()
            .map_err(|_| BraidError::Protocol("Invalid start index".into()))?
    };
    let end = if b.is_empty() {
        len
    } else {
        b.parse()
            .map_err(|_| BraidError::Protocol("Invalid end index".into()))?
    };
    if start > end || end > len {
        return Err(BraidError::Protocol(
            "Text range outside base content".into(),
        ));
    }
    Ok((start, end))
}

pub fn apply_text_patches(base: &str, patches: &[Patch]) -> Result<String> {
    let mut offsets: Vec<usize> = base.char_indices().map(|(i, _)| i).collect();
    offsets.push(base.len());
    let mut edits = Vec::with_capacity(patches.len());
    for patch in patches {
        if patch.unit != "text" {
            return Err(BraidError::Protocol(format!(
                "Unsupported text unit: {}",
                patch.unit
            )));
        }
        let (start, end) = text_range(&patch.range, offsets.len() - 1)?;
        let text = std::str::from_utf8(&patch.content)
            .map_err(|_| BraidError::Protocol("Invalid UTF-8 text patch".into()))?;
        edits.push((start, end, text));
    }
    edits.sort_by_key(|(start, end, _)| (*start, *end));
    for pair in edits.windows(2) {
        if pair[0].1 > pair[1].0 || pair[0].0 == pair[1].0 {
            return Err(BraidError::Protocol("Overlapping text patches".into()));
        }
    }
    let mut result = base.to_owned();
    for (start, end, text) in edits.into_iter().rev() {
        result.replace_range(offsets[start]..offsets[end], text);
    }
    Ok(result)
}
