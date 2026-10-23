use crate::core::Patch;
use dissimilar::{diff, Chunk}; // Use crate Patch type

pub fn compute_patches(original: &str, new: &str) -> Vec<Patch> {
    let chunks = diff(original, new);
    let mut patches = Vec::new();
    let mut current_pos = 0;

    for chunk in chunks {
        match chunk {
            Chunk::Equal(text) => {
                current_pos += text.chars().count();
            }
            Chunk::Delete(text) => {
                // Delete: Range [start, end]
                let end = current_pos + text.chars().count();
                patches.push(Patch {
                    unit: "text".to_string(),
                    range: format!("[{}:{}]", current_pos, end),
                    content: bytes::Bytes::new(), // Empty content
                    content_length: Some(0),
                });
                // Current pos stays same (text removed)
            }
            Chunk::Insert(text) => {
                // Insert: Range [pos, pos]
                let bytes = bytes::Bytes::copy_from_slice(text.as_bytes());
                patches.push(Patch {
                    unit: "text".to_string(),
                    range: format!("[{}:{}]", current_pos, current_pos),
                    content: bytes.clone(),
                    content_length: Some(bytes.len()),
                });
                current_pos += text.chars().count();
            }
        }
    }

    // Optimize: Merge adjacent delete/inserts? (Replacements)
    // For now, simple list is fine.
    patches
}
