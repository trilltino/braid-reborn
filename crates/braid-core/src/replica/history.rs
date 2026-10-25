//! Bounded original-event retrieval, separate from positional subscriptions.
use super::{ReplicaDocument, ReplicaError};
use braid_http::types::{Update, Version};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};

pub const MAX_HISTORY_PAGE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_HISTORY_PAGE_EVENTS: usize = 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryLimits {
    pub max_events: usize,
    pub max_bytes: usize,
}
impl Default for HistoryLimits {
    fn default() -> Self {
        Self {
            max_events: 128,
            max_bytes: MAX_HISTORY_PAGE_BYTES,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryPage {
    pub target: Vec<Version>,
    pub through: Vec<Version>,
    pub events: Vec<Update>,
    pub complete: bool,
}

fn validate_frontier(
    document: &ReplicaDocument,
    frontier: &[Version],
) -> Result<HashSet<String>, ReplicaError> {
    let mut seen = HashSet::new();
    for version in frontier {
        if !seen.insert(version.to_string()) {
            return Err(ReplicaError::Invalid(
                "Duplicate history frontier member".into(),
            ));
        }
    }
    let ancestors = document.ancestors(frontier)?;
    // All selected heads must be concurrent. Redundant ancestors are not a
    // complete frontier and would make continuation ambiguous.
    for version in frontier {
        let parents = document
            .version_graph
            .get(&version.to_string())
            .ok_or_else(|| ReplicaError::HistoryUnavailable(vec![version.clone()]))?;
        let parent_versions: Vec<_> = parents.iter().map(Version::new).collect();
        if document
            .ancestors(&parent_versions)?
            .iter()
            .any(|id| seen.contains(id))
        {
            return Err(ReplicaError::Invalid(
                "History frontier contains an ancestor of another member".into(),
            ));
        }
    }
    Ok(ancestors)
}

pub(super) fn page(
    document: &ReplicaDocument,
    from: &[Version],
    target: &[Version],
    limits: HistoryLimits,
) -> Result<HistoryPage, ReplicaError> {
    if limits.max_events == 0
        || limits.max_events > MAX_HISTORY_PAGE_EVENTS
        || limits.max_bytes == 0
        || limits.max_bytes > MAX_HISTORY_PAGE_BYTES
    {
        return Err(ReplicaError::Invalid(
            "History page limits must be positive and within 1024 events / 32 MiB".into(),
        ));
    }
    let base = validate_frontier(document, from)?;
    let target_ancestors = validate_frontier(document, target)?;
    if !base.is_subset(&target_ancestors) {
        return Err(ReplicaError::Invalid(
            "History boundaries are not causally ordered".into(),
        ));
    }
    let needed: HashSet<_> = target_ancestors.difference(&base).cloned().collect();
    let mut event_indices = HashMap::new();
    for (index, event) in document.history.iter().enumerate() {
        if !event
            .version
            .iter()
            .any(|id| needed.contains(&id.to_string()))
        {
            continue;
        }
        if event.version.len() != 1 {
            return Err(ReplicaError::Invalid(
                "Stored causal event must have one identity".into(),
            ));
        }
        let id = event.version[0].to_string();
        if event_indices.insert(id, index).is_some() {
            return Err(ReplicaError::Invalid(
                "Duplicate identity in stored causal history".into(),
            ));
        }
    }
    let mut unsatisfied: Vec<_> = needed
        .iter()
        .filter(|id| !event_indices.contains_key(*id))
        .map(Version::new)
        .collect();
    unsatisfied.sort_by_key(ToString::to_string);
    if !unsatisfied.is_empty() {
        return Err(ReplicaError::HistoryUnavailable(unsatisfied));
    }

    let mut indegree = HashMap::new();
    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    let mut ready = BTreeSet::new();
    for id in &needed {
        let index = *event_indices
            .get(id)
            .ok_or_else(|| ReplicaError::HistoryUnavailable(vec![Version::new(id)]))?;
        let event = &document.history[index];
        let mut actual: Vec<_> = event.parents.iter().map(ToString::to_string).collect();
        actual.sort();
        let mut declared = document
            .version_graph
            .get(id)
            .cloned()
            .ok_or_else(|| ReplicaError::HistoryUnavailable(vec![Version::new(id)]))?;
        declared.sort();
        if actual != declared {
            return Err(ReplicaError::Invalid(
                "Stored event parents disagree with causal graph".into(),
            ));
        }
        let dependencies = actual
            .iter()
            .filter(|parent| needed.contains(*parent))
            .count();
        indegree.insert(id.clone(), dependencies);
        if dependencies == 0 {
            ready.insert((index, id.clone()));
        }
        for parent in actual.into_iter().filter(|parent| needed.contains(parent)) {
            children.entry(parent).or_default().push(id.clone());
        }
    }
    let mut heads: BTreeSet<_> = from.iter().map(ToString::to_string).collect();
    let mut result = HistoryPage {
        target: target.to_vec(),
        through: heads.iter().map(Version::new).collect(),
        events: Vec::new(),
        complete: needed.is_empty(),
    };
    let mut event_bytes = 0usize;
    while result.events.len() < limits.max_events {
        let Some((index, id)) = ready.pop_first() else {
            break;
        };
        let event = &document.history[index];
        let mut next_heads = heads.clone();
        for parent in &event.parents {
            next_heads.remove(&parent.to_string());
        }
        next_heads.insert(id.clone());
        let next_through: Vec<_> = next_heads.iter().map(Version::new).collect();
        let next_complete = result.events.len() + 1 == needed.len();
        let metadata = HistoryPage {
            target: result.target.clone(),
            through: next_through.clone(),
            events: Vec::new(),
            complete: next_complete,
        };
        let next_event_bytes = event_bytes.saturating_add(serde_json::to_vec(event)?.len());
        // Empty-array metadata already includes []; add event bytes and commas.
        let encoded_size = serde_json::to_vec(&metadata)?
            .len()
            .saturating_add(next_event_bytes)
            .saturating_add(result.events.len());
        if encoded_size > limits.max_bytes {
            if result.events.is_empty() {
                return Err(ReplicaError::Limit(
                    "An indivisible causal event exceeds the history page byte limit".into(),
                ));
            }
            break;
        }
        result.events.push(event.clone());
        result.through = next_through;
        result.complete = next_complete;
        heads = next_heads;
        event_bytes = next_event_bytes;
        if let Some(dependents) = children.get(&id) {
            for child in dependents {
                let count = indegree.get_mut(child).ok_or_else(|| {
                    ReplicaError::Invalid("Missing causal dependency count".into())
                })?;
                *count = count.checked_sub(1).ok_or_else(|| {
                    ReplicaError::Invalid("Invalid causal dependency count".into())
                })?;
                if *count == 0 {
                    let child_index = *event_indices.get(child).ok_or_else(|| {
                        ReplicaError::HistoryUnavailable(vec![Version::new(child)])
                    })?;
                    ready.insert((child_index, child.clone()));
                }
            }
        }
    }
    if !result.complete && result.events.is_empty() {
        return Err(ReplicaError::Invalid(
            "Causal history cannot make progress".into(),
        ));
    }
    if serde_json::to_vec(&result)?.len() > limits.max_bytes {
        return Err(ReplicaError::Limit(
            "History page metadata exceeds byte limit".into(),
        ));
    }
    Ok(result)
}

/// Internal authenticated ingestion endpoint. It is separate from finite Braid
/// GET and from positional subscriptions, which only wake a replication worker.
pub const REPLICATION_HISTORY_PATH: &str = "/.well-known/braid/replication/v1/history";
pub const REPLICATION_NAMESPACE: &str = "/.well-known/braid/replication/";
pub const MAX_HISTORY_REQUEST_BYTES: usize = 64 * 1024;

/// A page request. Unknown fields are rejected so later protocol versions need a new path.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRequest {
    pub resource: String,
    #[serde(default)]
    pub from: Vec<Version>,
    #[serde(default)]
    pub target: Option<Vec<Version>>,
    #[serde(default)]
    pub limits: Option<HistoryLimits>,
}
