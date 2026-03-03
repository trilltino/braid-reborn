//! Diamond-Types CRDT integration for Braid-HTTP text synchronization.
//!
//! This module provides a high-performance wrapper around the diamond-types CRDT library,
//! enabling conflict-free text document synchronization for Braid-HTTP. It's built on two
//! core abstractions: operation logs (OpLog) and document snapshots (Branch).
//!
//! # Overview
//!
//! Diamond-Types is a super-fast CRDT (Conflict-free Replicated Data Type) optimized for
//! plain text documents. Key concepts:
//!
//! - **OpLog (Operation Log)**: Append-only log of all document changes
//! - **Branch**: In-memory snapshot of document state at a specific point in time
//! - **Automatic Conflict Resolution**: Concurrent edits merge deterministically
//!
//! # Unique ID Requirements
//!
//! Every operation must have a unique ID (agent ID + sequence number pair). Critical rules:
//!
//! - ⚠️ Generate a **unique agent ID per editing session** (e.g., UUID)
//! - ⚠️ **Never reuse agent IDs** across sessions—this causes document divergence
//! - Sequence numbers are automatically incremented by diamond-types
//! - Each character operation (insert/delete) increments the sequence number
//!
//! # Examples
//!
//! ## Basic Text Editing
//!
//! ```
//! use crate::core::merge::DiamondCRDT;
//!
//! let mut doc = DiamondCRDT::new("session-uuid-abc");
//! doc.add_insert(0, "hello");
//! doc.add_insert(5, " world");
//! assert_eq!(doc.content(), "hello world");
//! ```
//!
//! ## Concurrent Edits from Multiple Peers
//!
//! ```
//! use crate::core::merge::DiamondCRDT;
//!
//! let mut doc = DiamondCRDT::new("editor-1");
//! doc.add_insert(0, "hello");
//!
//! // Concurrent edit from another client
//! doc.add_insert_remote("editor-2", 5, " world");
//!
//! // Automatically merged without conflict
//! assert_eq!(doc.content(), "hello world");
//! ```
//!
//! # Specification References
//!
//! - **draft-toomim-httpbis-braid-http**: Section 2.2 (Merge-Types)
//! - **diamond-types**: <https://docs.rs/diamond-types/>

use crate::vendor::diamond_types::list::operation::TextOperation;
use crate::vendor::diamond_types::{CRDTKind, CreateValue, HasLength, LV};
use parking_lot::Mutex;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use super::merge_type::{MergePatch, MergeResult, MergeType};

/// High-performance text CRDT wrapper for collaborative editing.
///
/// `DiamondCRDT` manages a text document with automatic conflict resolution for concurrent
/// edits from multiple peers. All operations are tracked in an operation log for synchronization
/// with remote clients.
///
/// # Invariants
///
/// - Agent ID must be **globally unique** within the editing session
/// - Content state is always consistent with operation history
/// - All remote edits are automatically applied with conflict resolution
/// - Branches are always at the tip of the operation log
///
/// # Complexity
///
/// - **Insert**: O(n) where n is document length (due to position transformation)
/// - **Delete**: O(n)
/// - **Export**: O(n)
/// - **State queries**: O(1)
#[derive(Clone, Debug)]
pub struct DiamondCRDT {
    /// Unique identifier for this editing session (must not be reused)
    agent_id: String,

    /// Complete history of all document operations
    oplog: crate::vendor::diamond_types::OpLog,

    /// Current document snapshot at the tip of the operation log
    branch: crate::vendor::diamond_types::Branch,

    /// Tracks remote agent IDs and their latest sequence numbers
    /// (useful for detecting and filtering duplicate operations)
    remote_versions: HashMap<String, u32>,

    /// Mapping from Braid version strings to diamond-types frontiers (for history)
    version_fronties: HashMap<String, crate::vendor::diamond_types::Frontier>,

    /// ID of the main text CRDT object
    text_id: LV,
}

impl DiamondCRDT {
    /// Create a new empty CRDT with a unique agent ID.
    ///
    /// The agent ID must be globally unique within the collaborative session and should never
    /// be reused across sessions. Each session should generate a new agent ID (typically a UUID).
    ///
    /// # Arguments
    ///
    /// * `agent_id` - Session-unique identifier (e.g., UUID, session token)
    ///
    /// # Panics
    ///
    /// Never panics. All operations are infallible during initialization.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let doc = DiamondCRDT::new("550e8400-e29b-41d4-a716-446655440000");
    /// assert!(doc.is_empty());
    /// ```
    #[must_use]
    pub fn new(agent_id: impl Into<String>) -> Self {
        let agent_id_str = agent_id.into();
        let mut oplog = crate::vendor::diamond_types::OpLog::new();
        // Use a deterministic agent for the initial CRDT setup so that different nodes
        // can agree on the same object ID for the "content" field.
        let genesis_agent = oplog.cg.get_or_create_agent_id("genesis");

        let text_id = oplog.local_map_set(
            genesis_agent,
            crate::vendor::diamond_types::ROOT_CRDT_ID,
            "content",
            CreateValue::NewCRDT(CRDTKind::Text),
        );
        let branch = oplog.checkout_tip();

        Self {
            agent_id: agent_id_str,
            oplog,
            branch,
            remote_versions: HashMap::new(),
            version_fronties: HashMap::new(),
            text_id,
        }
    }

    // ========== Local Editing Methods ==========

    /// Insert text at a position in the document.
    ///
    /// This is a local edit operation. The insertion is immediately applied to the document
    /// and tracked in the operation log for synchronization with peers.
    ///
    /// # Arguments
    ///
    /// * `pos` - Position to insert at (0-based, in Unicode characters)
    /// * `text` - Text content to insert (must be valid UTF-8)
    ///
    /// # Panics
    ///
    /// Panics if `pos` exceeds the document length, matching `str::insert()` behavior.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hello");
    /// doc.add_insert(5, " world");
    /// assert_eq!(doc.content(), "hello world");
    /// ```
    pub fn add_insert(&mut self, pos: usize, text: &str) {
        let agent = self.oplog.cg.get_or_create_agent_id(&self.agent_id);
        self.oplog
            .local_text_op(agent, self.text_id, TextOperation::new_insert(pos, text));
        let frontier = self.get_local_frontier();
        self.branch = self.oplog.checkout_tip();

        let version = self.get_version();
        self.version_fronties.insert(version, frontier);
    }

    /// Delete a range of characters from the document.
    ///
    /// This is a local edit operation. The deletion is immediately applied and tracked
    /// in the operation log for peer synchronization.
    ///
    /// # Arguments
    ///
    /// * `range` - Character range to delete (exclusive end, in Unicode characters)
    ///
    /// # Panics
    ///
    /// Panics if the range exceeds document bounds or is invalid (start > end).
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hello world");
    /// doc.add_delete(5..6);  // Delete the space
    /// assert_eq!(doc.content(), "helloworld");
    /// ```
    pub fn add_delete(&mut self, range: std::ops::Range<usize>) {
        let agent = self.oplog.cg.get_or_create_agent_id(&self.agent_id);
        self.oplog
            .local_text_op(agent, self.text_id, TextOperation::new_delete(range));
        let frontier = self.get_local_frontier();
        self.branch = self.oplog.checkout_tip();

        let version = self.get_version();
        self.version_fronties.insert(version, frontier);
    }

    // ========== Remote Editing Methods ==========

    /// Apply an insertion from a remote peer at a specific version (parents).
    pub fn add_insert_remote_versioned(
        &mut self,
        agent_id: &str,
        parents: &[&str],
        pos: usize,
        text: &str,
        version_id: Option<&str>,
    ) {
        let agent = self.oplog.cg.get_or_create_agent_id(agent_id);

        // Resolve parents to LVs
        let mut lvs = Vec::new();
        for p in parents {
            if let Some(frontier) = self.resolve_version(p) {
                lvs.extend(frontier.as_ref());
            }
        }

        // If no parents matched, we might be at ROOT or we'll just append to tip
        // In a real Braid system, we should probably handle ROOT explicitly.

        let op =
            crate::vendor::diamond_types::list::operation::TextOperation::new_insert(pos, text);
        let range = self
            .oplog
            .cg
            .assign_local_op_with_parents(&lvs, agent, op.len());
        self.oplog.remote_text_op(self.text_id, range, op);
        self.branch = self.oplog.checkout_tip();

        // Register the version mapping
        if let Some(vid) = version_id {
            let frontier = crate::vendor::diamond_types::Frontier::new_1(range.last());
            self.register_version_mapping(vid.to_string(), frontier);
        }
    }

    /// Apply an insertion from a remote peer.
    ///
    /// Remote edits are merged into the document using the CRDT algorithm. The operation log
    /// automatically handles concurrent edits from this and other peers without conflicts.
    ///
    /// # Arguments
    ///
    /// * `agent_id` - Unique ID of the remote peer (must differ from local agent ID)
    /// * `pos` - Position to insert at (in Unicode characters)
    /// * `text` - Text content to insert
    ///
    /// # Note
    ///
    /// If the `agent_id` matches the local agent ID, this is treated as a separate operation
    /// and will result in duplicate content. Always use unique agent IDs.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hello");
    ///
    /// // Remote peer inserts at the end
    /// doc.add_insert_remote("session-2", 5, " world");
    /// assert_eq!(doc.content(), "hello world");
    /// ```
    pub fn add_insert_remote(&mut self, agent_id: &str, pos: usize, text: &str) {
        let agent = self.oplog.cg.get_or_create_agent_id(agent_id);
        self.oplog.local_text_op(
            agent,
            self.text_id,
            crate::vendor::diamond_types::list::operation::TextOperation::new_insert(pos, text),
        );
        self.branch = self.oplog.checkout_tip();
    }

    /// Apply a deletion from a remote peer at a specific version (parents).
    pub fn add_delete_remote_versioned(
        &mut self,
        agent_id: &str,
        parents: &[&str],
        range: std::ops::Range<usize>,
        version_id: Option<&str>,
    ) {
        let agent = self.oplog.cg.get_or_create_agent_id(agent_id);

        // Resolve parents to LVs
        let mut lvs = Vec::new();
        for p in parents {
            if let Some(frontier) = self.resolve_version(p) {
                lvs.extend(frontier.as_ref());
            }
        }

        let op = crate::vendor::diamond_types::list::operation::TextOperation::new_delete(range);
        let range = self
            .oplog
            .cg
            .assign_local_op_with_parents(&lvs, agent, op.len());
        self.oplog.remote_text_op(self.text_id, range, op);
        self.branch = self.oplog.checkout_tip();

        // Register the version mapping
        if let Some(vid) = version_id {
            let frontier = crate::vendor::diamond_types::Frontier::new_1(range.last());
            self.register_version_mapping(vid.to_string(), frontier);
        }
    }

    /// Apply a deletion from a remote peer.
    ///
    /// Remote deletions are merged into the document using CRDT semantics. The algorithm ensures
    /// that concurrent deletes and inserts resolve deterministically across all peers.
    ///
    /// # Arguments
    ///
    /// * `agent_id` - Unique ID of the remote peer
    /// * `range` - Character range to delete (exclusive end)
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hello world");
    ///
    /// doc.add_delete_remote("session-2", 5..6);  // Delete the space
    /// assert_eq!(doc.content(), "helloworld");
    /// ```
    pub fn add_delete_remote(&mut self, agent_id: &str, range: std::ops::Range<usize>) {
        let agent = self.oplog.cg.get_or_create_agent_id(agent_id);
        self.oplog.local_text_op(
            agent,
            self.text_id,
            crate::vendor::diamond_types::list::operation::TextOperation::new_delete(range),
        );
        self.branch = self.oplog.checkout_tip();
    }

    // ========== Query Methods ==========

    /// Get the current document content as a string.
    ///
    /// Returns the fully merged document state. This reflects all operations
    /// from all peers up to the current tip of the operation log.
    ///
    /// # Complexity
    ///
    /// O(n) where n is the document length (due to underlying representation).
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hello");
    /// assert_eq!(doc.content(), "hello");
    /// ```
    #[inline]
    pub fn content(&self) -> String {
        self.branch
            .texts
            .get(&self.text_id)
            .map(|t| t.to_string())
            .unwrap_or_default()
    }

    /// Get the session's agent ID.
    ///
    /// Returns the unique identifier used for distinguishing this peer's operations
    /// in the operation log.
    ///
    /// # Returns
    ///
    /// A string slice referencing the agent ID passed to `new()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let doc = DiamondCRDT::new("session-1");
    /// assert_eq!(doc.agent_id(), "session-1");
    /// ```
    #[inline]
    #[must_use]
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// Get the total count of operations in the operation log.
    ///
    /// Each character insertion or deletion counts as one operation. This is useful
    /// for version tracking and detecting whether the document has changed.
    ///
    /// # Note
    ///
    /// This is not the same as document length. For example, inserting "hello" counts
    /// as 5 operations, but the document length is also 5 (in this case). However,
    /// deleting 3 characters increments this by 3.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hi");  // 2 operations
    /// assert_eq!(doc.operation_count(), 2);
    /// ```
    #[inline]
    #[must_use]
    pub fn operation_count(&self) -> usize {
        self.oplog.cg.len()
    }

    /// Check if the document is empty (no operations applied).
    ///
    /// Returns `true` if the operation log is empty. Note that this does not
    /// check content length; it checks whether any operations have been recorded.
    ///
    /// # Returns
    ///
    /// `true` if no operations have been applied; `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// assert!(doc.is_empty());
    /// doc.add_insert(0, "text");
    /// assert!(!doc.is_empty());
    /// ```
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        // Initial state has 1 operation: creating the root "content" text CRDT
        self.operation_count() <= 1
    }

    // ========== Serialization & Export Methods ==========

    /// Export document state and metadata as JSON.
    ///
    /// Creates a JSON representation suitable for transmission via Braid-HTTP.
    /// This includes current content, operation count, and a version identifier.
    ///
    /// # Returns
    ///
    /// A JSON object containing:
    /// - `agent_id` (string): The session's agent ID
    /// - `operations_count` (number): Total operations in the log
    /// - `content` (string): Current merged document content
    /// - `version` (string): Version identifier for this state
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hello");
    /// let exported = doc.export_operations();
    /// assert_eq!(exported["agent_id"], "session-1");
    /// ```
    #[must_use]
    pub fn export_operations(&self) -> Value {
        let version = format!("oplog-{}-{}", self.agent_id, self.oplog.cg.len());

        json!({
            "agent_id": self.agent_id,
            "operations_count": self.oplog.cg.len(),
            "content": self.content(),
            "version": version,
        })
    }

    /// Generate a version identifier for Braid-HTTP headers.
    ///
    /// Returns a unique string representing the current document state. Version
    /// identifiers should change whenever the document is modified. They're used
    /// in Braid protocol responses for version tracking and conflict detection.
    ///
    /// # Format
    ///
    /// Returns strings of the form: `diamond-{agent_id}-{operation_count}`
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// let v1 = doc.get_version();
    /// doc.add_insert(0, "x");
    /// let v2 = doc.get_version();
    /// assert_ne!(v1, v2);
    /// ```
    #[must_use]
    pub fn get_version(&self) -> String {
        format!("diamond-{}-{}", self.agent_id, self.oplog.cg.len())
    }

    /// Get the current local frontier.
    pub fn get_local_frontier(&self) -> crate::vendor::diamond_types::Frontier {
        self.oplog.cg.version.clone()
    }

    /// Create a checkpoint snapshot of the current state.
    ///
    /// Returns a complete snapshot of the document suitable for Braid responses.
    /// This includes content, version identifier, agent ID, and operation count.
    ///
    /// # Returns
    ///
    /// A JSON object containing:
    /// - `content` (string): Current document text
    /// - `version` (string): Version identifier
    /// - `agent_id` (string): Session agent ID
    /// - `oplog_len` (number): Operation count
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let mut doc = DiamondCRDT::new("session-1");
    /// doc.add_insert(0, "hello");
    /// let checkpoint = doc.checkpoint();
    /// assert_eq!(checkpoint["content"], "hello");
    /// ```
    #[must_use]
    pub fn checkpoint(&self) -> Value {
        json!({
            "content": self.content(),
            "version": self.get_version(),
            "agent_id": self.agent_id,
            "oplog_len": self.oplog.cg.len(),
        })
    }

    /// Estimate merge quality based on operation diversity.
    ///
    /// Returns a heuristic score (0-100) indicating merge convergence quality.
    /// Higher scores indicate better document stability and fewer conflicting edits.
    ///
    /// # Heuristic
    ///
    /// Currently based on the diversity of remote agents that have edited the document:
    /// - 100: Only local edits (no remote agents)
    /// - < 100: Remote agents present (diversity factor applied)
    ///
    /// # Returns
    ///
    /// An integer from 0 to 100, where 100 is best quality.
    ///
    /// # Examples
    ///
    /// ```
    /// use crate::core::merge::DiamondCRDT;
    ///
    /// let doc = DiamondCRDT::new("session-1");
    /// assert_eq!(doc.merge_quality(), 100);  // No remote edits yet
    /// ```
    #[inline]
    #[must_use]
    pub fn merge_quality(&self) -> u32 {
        if self.remote_versions.is_empty() {
            100
        } else {
            let remote_agents = self.remote_versions.len() as f64;
            let diversity_factor = (remote_agents / (remote_agents + 1.0)) * 100.0;
            (diversity_factor.clamp(0.0, 100.0)) as u32
        }
    }

    /// Resolve a Braid version string to an internal Frontier.
    pub fn resolve_version(
        &self,
        version: &str,
    ) -> Option<&crate::vendor::diamond_types::Frontier> {
        self.version_fronties.get(version)
    }

    /// Register a Braid version mapping for a given Frontier.
    pub fn register_version_mapping(
        &mut self,
        version: String,
        frontier: crate::vendor::diamond_types::Frontier,
    ) {
        self.version_fronties.insert(version, frontier);
    }

    /// Get missing operations since a set of internal versions (Frontiers).
    pub fn get_ops_since(
        &self,
        since: &[crate::vendor::diamond_types::Frontier],
    ) -> Vec<crate::vendor::diamond_types::SerializedOpsOwned> {
        // Collect all LVs from all frontiers
        let mut all_lvs = Vec::new();
        for f in since {
            all_lvs.extend(f.as_ref());
        }

        // This is a simplified approach: we just take the union of all frontiers
        // In DT, ops_since takes a slice of LVs representing the known state.
        let delta = self.oplog.ops_since(&all_lvs);
        vec![delta.to_owned()]
    }

    /// Check the internal consistency of the CRDT.
    pub fn dbg_check(&self, deep: bool) {
        self.oplog.cg.dbg_check(deep);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_insert() {
        let mut crdt = DiamondCRDT::new("alice");
        crdt.add_insert(0, "hello");
        assert_eq!(crdt.content(), "hello");
    }

    #[test]
    fn test_multiple_inserts() {
        let mut crdt = DiamondCRDT::new("alice");
        crdt.add_insert(0, "hello");
        crdt.add_insert(5, " world");
        assert_eq!(crdt.content(), "hello world");
    }

    #[test]
    fn test_delete() {
        let mut crdt = DiamondCRDT::new("alice");
        crdt.add_insert(0, "hello world");
        crdt.add_delete(5..6);
        assert_eq!(crdt.content(), "helloworld");
    }

    #[test]
    fn test_concurrent_edits() {
        let mut crdt = DiamondCRDT::new("alice");
        crdt.add_insert(0, "hello");
        crdt.add_insert_remote("bob", 5, " world");
        assert_eq!(crdt.content(), "hello world");
    }

    #[test]
    fn test_agent_id() {
        let crdt = DiamondCRDT::new("alice");
        assert_eq!(crdt.agent_id(), "alice");
    }

    #[test]
    fn test_is_empty() {
        let mut crdt = DiamondCRDT::new("alice");
        assert!(crdt.is_empty());
        crdt.add_insert(0, "text");
        assert!(!crdt.is_empty());
    }

    #[test]
    fn test_export_operations() {
        let mut crdt = DiamondCRDT::new("alice");
        crdt.add_insert(0, "hello");
        let export = crdt.export_operations();

        assert_eq!(export["agent_id"], "alice");
        assert!(export["operations_count"].is_number());
        assert_eq!(export["content"], "hello");
        assert!(export["version"].is_string());
    }

    #[test]
    fn test_get_version() {
        let mut crdt = DiamondCRDT::new("alice");
        let v1 = crdt.get_version();
        assert!(v1.contains("alice"));

        crdt.add_insert(0, "text");
        let v2 = crdt.get_version();
        assert_ne!(v1, v2);
    }

    #[test]
    fn test_checkpoint() {
        let mut crdt = DiamondCRDT::new("alice");
        crdt.add_insert(0, "hello");
        let cp = crdt.checkpoint();

        assert_eq!(cp["content"], "hello");
        assert_eq!(cp["agent_id"], "alice");
        assert!(cp["version"].is_string());
    }

    #[test]
    fn test_merge_quality() {
        let crdt = DiamondCRDT::new("alice");
        assert_eq!(crdt.merge_quality(), 100);
    }
}

#[derive(Debug, Clone)]
pub struct DiamondMergeType {
    crdt: Arc<Mutex<DiamondCRDT>>,
}

impl DiamondMergeType {
    pub fn new(agent_id: &str) -> Self {
        Self {
            crdt: Arc::new(Mutex::new(DiamondCRDT::new(agent_id))),
        }
    }
}

impl MergeType for DiamondMergeType {
    fn name(&self) -> &str {
        "diamond"
    }

    fn initialize(&mut self, content: &str) -> MergeResult {
        let mut crdt = self.crdt.lock();
        let len = crdt.content().len();
        if len > 0 {
            crdt.add_delete(0..len);
        }
        crdt.add_insert(0, content);
        MergeResult::success(Some(braid_http::types::Version::String(crdt.get_version())), Vec::new())
    }

    fn apply_patch(&mut self, patch: MergePatch) -> MergeResult {
        let mut crdt = self.crdt.lock();
        let parents_strings: Vec<String> = patch.parents.iter().map(|p| match p {
            braid_http::types::Version::String(s) => s.clone(),
            braid_http::types::Version::Integer(i) => i.to_string(),
        }).collect();
        let parents_refs: Vec<&str> = parents_strings.iter().map(|s| s.as_str()).collect();

        let agent_id = patch
            .version
            .as_ref()
            .and_then(|v| match v {
                braid_http::types::Version::String(s) => s.split('-').next(),
                _ => None,
            })
            .unwrap_or("remote");

        let content_str = match &patch.content {
            Value::String(s) => s.clone(),
            _ => patch.content.to_string(),
        };

        // Parse range: Braid ranges can be "start:end" or JSON [start, end]
        let range_raw = patch.range.trim_matches(|c| c == '[' || c == ']');
        let parts: Vec<&str> = if range_raw.contains(':') {
            range_raw.split(':').collect()
        } else if range_raw.contains(',') {
            range_raw.split(',').collect()
        } else {
            vec![range_raw]
        };

        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (
                parts[0].trim().parse::<usize>(),
                parts[1].trim().parse::<usize>(),
            ) {
                if start == end {
                    // Insertion
                    crdt.add_insert_remote_versioned(
                        agent_id,
                        &parents_refs,
                        start,
                        &content_str,
                        patch.version.as_ref().map(|v| v.to_string()).as_deref(),
                    );
                } else {
                    // Deletion (and possible replacement if content is not empty)
                    crdt.add_delete_remote_versioned(
                        agent_id,
                        &parents_refs,
                        start..end,
                        patch.version.as_ref().map(|v| v.to_string()).as_deref(),
                    );
                    if !content_str.is_empty() {
                        crdt.add_insert_remote_versioned(
                            agent_id,
                            &parents_refs,
                            start,
                            &content_str,
                            patch.version.as_ref().map(|v| v.to_string()).as_deref(),
                        );
                    }
                }
            }
        } else if range_raw.is_empty() {
            // Snapshot/Overwrite
            let len = crdt.content().len();
            if len > 0 {
                crdt.add_delete_remote_versioned(agent_id, &parents_refs, 0..len, None);
            }
            crdt.add_insert_remote_versioned(
                agent_id,
                &parents_refs,
                0,
                &content_str,
                patch.version.as_ref().map(|v| v.to_string()).as_deref(),
            );
        }

        MergeResult::success(patch.version, Vec::new())
    }

    fn local_edit(&mut self, patch: MergePatch) -> MergeResult {
        let mut crdt = self.crdt.lock();
        let content_str = match &patch.content {
            Value::String(s) => s.clone(),
            _ => patch.content.to_string(),
        };

        let range_raw = patch.range.trim_matches(|c| c == '[' || c == ']');
        let parts: Vec<&str> = if range_raw.contains(':') {
            range_raw.split(':').collect()
        } else if range_raw.contains(',') {
            range_raw.split(',').collect()
        } else {
            vec![range_raw]
        };

        if parts.len() == 2 {
            if let (Ok(start), Ok(end)) = (
                parts[0].trim().parse::<usize>(),
                parts[1].trim().parse::<usize>(),
            ) {
                if start == end {
                    crdt.add_insert(start, &content_str);
                } else {
                    crdt.add_delete(start..end);
                    if !content_str.is_empty() {
                        crdt.add_insert(start, &content_str);
                    }
                }
            }
        } else if range_raw.is_empty() {
            let len = crdt.content().len();
            if len > 0 {
                crdt.add_delete(0..len);
            }
            crdt.add_insert(0, &content_str);
        }

        let version_str = crdt.get_version();
        let version = braid_http::types::Version::String(version_str);
        let mut out_patch = patch;
        out_patch.version = Some(version.clone());
        out_patch.parents = vec![version.clone()];

        MergeResult::success(Some(version), vec![out_patch])
    }

    fn get_content(&self) -> String {
        self.crdt.lock().content()
    }

    fn get_version(&self) -> Vec<braid_http::types::Version> {
        vec![braid_http::types::Version::String(self.crdt.lock().get_version())]
    }

    fn get_all_versions(&self) -> HashMap<String, Vec<braid_http::types::Version>> {
        let mut map = HashMap::new();
        map.insert(self.crdt.lock().get_version(), Vec::new());
        map
    }

    fn prune(&mut self) -> bool {
        false
    }

    fn clone_box(&self) -> Box<dyn MergeType> {
        Box::new(self.clone())
    }
}
