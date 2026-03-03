//! Simpleton merge-type implementation.
//!
//! Matches the behavior of braid-text's simpleton-client.js.

use super::merge_type::{MergePatch, MergeResult, MergeType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SimpletonMergeType {
    pub peer_id: String,
    pub content: String,
    pub version: Vec<braid_http::types::Version>,
    pub char_counter: i64,
}

impl SimpletonMergeType {
    pub fn new(peer_id: &str) -> Self {
        // Use 0 as initial counter. 
        // Uniqueness is guaranteed by the random peer_id generated on startup.
        // Timestamp-based counters caused RangeErrors in JS clients/extensions 
        // that expect small integer version arrays.
        let initial_counter = 0;
        
        Self {
            peer_id: peer_id.to_string(),
            content: String::new(),
            version: Vec::new(),
            char_counter: initial_counter,
        }
    }

    fn simple_diff(&self, old_text: &str, new_text: &str) -> (usize, usize, String) {
        let a: Vec<char> = old_text.chars().collect();
        let b: Vec<char> = new_text.chars().collect();

        // Common prefix
        let mut p = 0;
        let len = std::cmp::min(a.len(), b.len());
        while p < len && a[p] == b[p] {
            p += 1;
        }

        // Common suffix
        let mut s = 0;
        let len_remaining = std::cmp::min(a.len() - p, b.len() - p);
        while s < len_remaining && a[a.len() - s - 1] == b[b.len() - s - 1] {
            s += 1;
        }

        let range_start = p;
        let range_end = a.len() - s;
        let content: String = b[p..b.len() - s].iter().collect();

        (range_start, range_end, content)
    }

    fn count_code_points(&self, s: &str) -> i64 {
        s.chars().count() as i64
    }
}

impl MergeType for SimpletonMergeType {
    fn name(&self) -> &str {
        "simpleton"
    }

    fn initialize(&mut self, content: &str) -> MergeResult {
        if !content.is_empty() || self.content.is_empty() {
            self.content = content.to_string();
        }
        MergeResult {
            version: self.version.first().cloned(),
            rebased_patches: Vec::new(),
            success: true,
            error: None,
        }
    }
    fn apply_patch(&mut self, patch: MergePatch) -> MergeResult {
        // Handle "everything" or "[0:]" as full replacement
        if patch.range == "everything" || patch.range == "[0:]" {
            let content_str = match &patch.content {
                Value::String(s) => s.clone(),
                v => v.to_string(),
            };

            self.content = content_str;
            
            if let Some(ref v) = patch.version {
                self.version = vec![v.clone()];
            }

            return MergeResult::success(self.version.first().cloned(), vec![]);
        }

        // Range format: [start:end]
        if let Some(range) = patch
            .range
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
        {
            let parts: Vec<&str> = range.split(':').collect();
            if parts.len() == 2 {
                if let (Ok(start), Ok(end)) = (parts[0].parse::<usize>(), parts[1].parse::<usize>())
                {
                    let content_str = match &patch.content {
                        Value::String(s) => s.clone(),
                        v => v.to_string(),
                    };

                    let chars: Vec<char> = self.content.chars().collect();
                    if start <= chars.len() && end <= chars.len() && start <= end {
                        let mut new_chars = chars[..start].to_vec();
                        new_chars.extend(content_str.chars());
                        new_chars.extend(&chars[end..]);
                        self.content = new_chars.into_iter().collect();

                        if let Some(ref v) = patch.version {
                            self.version = vec![v.clone()];
                        }
 
                        return MergeResult::success(self.version.first().cloned(), vec![]);
                    }
                }
            }
        }

        warn!(
            "Simpleton: Invalid range format or indices: {}",
            patch.range
        );
        MergeResult::failure("Invalid range")
    }

    fn local_edit(&mut self, patch: MergePatch) -> MergeResult {
        // If content is provided, we default to full replacement logic unless we implement diffing here
        // But better: expect `patch` to specificy the *change*.
        // However, `sync.rs` currently creates an "everything" patch.
        // We should calculate the diff here if it's an "everything" patch but we have previous content.

        let (range, content, delete_count) = if patch.range == "everything" || patch.range == "[0:]" {
            let new_content_str = match &patch.content {
                Value::String(s) => s.clone(),
                v => v.to_string(),
            };

            if self.content.is_empty() {
                // Initial content
                ("[0:0]".to_string(), new_content_str, 0)
            } else {
                // Calculate diff
                let (start, end, text) = self.simple_diff(&self.content, &new_content_str);
                (format!("[{}:{}]", start, end), text, (end - start) as i64)
            }
        } else {
            // Already a specific patch? Not fully supported in this refactor yet for local_edit input,
            // assuming mostly "everything" input from sync.rs for now.
            // But if it IS a patch, we should use it.
            let _content_str = match &patch.content {
                Value::String(s) => s.clone(),
                v => v.to_string(),
            };
             // We need to parse range to know delete count
             // Quick hack: if it's not "everything", assume consumer knows what they are doing?
             // For now, let's treat "everything" as the main path for sync.rs
             return MergeResult::failure("Custom range patches not yet supported in local_edit - use 'everything' to trigger diffing");
        };

        // Apply the change locally
        let patch_obj = MergePatch::new(&range, Value::String(content.clone()));

        // Guard: If nothing changed, return success with no patches to signal sync should skip
        if delete_count == 0 && content.is_empty() && !self.version.is_empty() {
            return MergeResult {
                version: self.version.first().cloned(),
                rebased_patches: Vec::new(),
                success: true,
                error: None,
            };
        }

        // Apply state update
        self.apply_patch(patch_obj.clone());

        // Update version using monotonic counter
        let insert_count = self.count_code_points(&content);
        self.char_counter += delete_count + insert_count;
        
        let version_id = format!("{}-{}", self.peer_id, self.char_counter);
        let version = braid_http::types::Version::String(version_id);
        self.version = vec![version.clone()];
 
        MergeResult::success(Some(version), vec![patch_obj])
    }

    fn get_content(&self) -> String {
        self.content.clone()
    }

    fn get_version(&self) -> Vec<braid_http::types::Version> {
        self.version.clone()
    }

    fn get_all_versions(&self) -> HashMap<String, Vec<braid_http::types::Version>> {
        let mut map = HashMap::new();
        if let Some(v) = self.version.first() {
            map.insert(v.to_string(), vec![]);
        }
        map
    }

    fn prune(&mut self) -> bool {
        // Simpleton doesn't really have history to prune
        false
    }

    fn clone_box(&self) -> Box<dyn MergeType> {
        Box::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    #[test]
    fn test_simpleton_diffing() {
        let mut simpleton = SimpletonMergeType::new("peer1");
        
        // 1. Initial Insert
        let patch1 = MergePatch::new("everything", Value::String("abc".to_string()));
        let res1 = simpleton.local_edit(patch1);
        
        // Should be [0:0] "abc"
        assert_eq!(res1.rebased_patches[0].range, "[0:0]");
        assert_eq!(res1.rebased_patches[0].content, Value::String("abc".to_string()));
        
        // Version check: verify it starts with peer and is followed by a large number
        let v1 = res1.version.unwrap().to_string();
        assert!(v1.starts_with("peer1-"));
        let seq1: i64 = v1.split_once('-').unwrap().1.parse().unwrap();
        
        assert_eq!(simpleton.get_content(), "abc");
 
        // 2. Diffing (Change 'b' to 'x') -> "axc"
        let patch2 = MergePatch::new("everything", Value::String("axc".to_string()));
        let res2 = simpleton.local_edit(patch2);
        
        // Diff should be at index 1, length 1 replaced by 'x'. Range [1:2]
        assert_eq!(res2.rebased_patches[0].range, "[1:2]");
        assert_eq!(res2.rebased_patches[0].content, Value::String("x".to_string()));
        
        // Version check: should be seq1 + 2 (1 del + 1 ins)
        let v2 = res2.version.unwrap().to_string();
        let seq2: i64 = v2.split_once('-').unwrap().1.parse().unwrap();
        assert_eq!(seq2, seq1 + 2);
        assert_eq!(simpleton.get_content(), "axc");
        
        // 3. Append ("axc" -> "axcd")
        let patch3 = MergePatch::new("everything", Value::String("axcd".to_string()));
        let res3 = simpleton.local_edit(patch3);
        
        // Diff: insert 'd' at 3. Range [3:3]
        assert_eq!(res3.rebased_patches[0].range, "[3:3]");
        assert_eq!(res3.rebased_patches[0].content, Value::String("d".to_string()));
        
        // Version check: should be seq2 + 1 (1 ins)
        let v3 = res3.version.unwrap().to_string();
        let seq3: i64 = v3.split_once('-').unwrap().1.parse().unwrap();
        assert_eq!(seq3, seq2 + 1);
    }

    #[test]
    fn test_simpleton_edge_cases() {
        let mut simpleton = SimpletonMergeType::new("peer2");

        // 1. Empty -> Content
        let patch1 = MergePatch::new("everything", Value::String("hello".to_string()));
        let res1 = simpleton.local_edit(patch1);
        assert_eq!(res1.rebased_patches[0].range, "[0:0]");
        assert_eq!(res1.rebased_patches[0].content, Value::String("hello".to_string()));
        assert_eq!(simpleton.get_content(), "hello");

        // 2. Content -> Empty
        let patch2 = MergePatch::new("everything", Value::String("".to_string()));
        let res2 = simpleton.local_edit(patch2);
        // Correct diff for "hello" -> "" is delete 5 chars at 0
        // Range should be [0:5] with empty content
        assert_eq!(res2.rebased_patches[0].range, "[0:5]");
        assert_eq!(res2.rebased_patches[0].content, Value::String("".to_string()));
        assert_eq!(simpleton.get_content(), "");

        // 3. Newline Append ("abc" -> "abc\n")
        simpleton.initialize("abc"); // Reset state
        let patch3 = MergePatch::new("everything", Value::String("abc\n".to_string()));
        let res3 = simpleton.local_edit(patch3);
        assert_eq!(res3.rebased_patches[0].range, "[3:3]");
        assert_eq!(res3.rebased_patches[0].content, Value::String("\n".to_string()));
        assert_eq!(simpleton.get_content(), "abc\n");

        // 4. Trailing Newline Change ("abc\n" -> "abc\r\n")
        let patch4 = MergePatch::new("everything", Value::String("abc\r\n".to_string()));
        let res4 = simpleton.local_edit(patch4);
        // "abc\n" (len 4) -> "abc\r\n" (len 5).
        // Diff: replace "\n" at index 3 with "\r\n"? 
        // Or insert "\r" at index 3? 
        // "abc" matches. Remaining: old="\n", new="\r\n".
        // simple_diff logic:
        // common_prefix("abc\n", "abc\r\n") -> "abc" (len 3).
        // check common_suffix("\n", "\r\n") -> "\n".
        // So change is index 3, delete 0, insert "\r".
        // Range [3:3], content "\r".
        assert_eq!(res4.rebased_patches[0].range, "[3:3]");
        assert_eq!(res4.rebased_patches[0].content, Value::String("\r".to_string()));
        assert_eq!(simpleton.get_content(), "abc\r\n");
    }
}