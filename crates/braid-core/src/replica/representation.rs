//! Explicit versioned JSON snapshots; no JSON sniffing or implicit merge engine.
use super::{same_frontier, ReplicaDocument, ReplicaError};
use braid_http::types::{RepresentationAt, Update, Version};

pub(super) const JSON_SNAPSHOT_PROFILE: &str = "json-snapshot";

pub(super) fn media_type(document: &ReplicaDocument) -> Option<String> {
    if document.merge_type == JSON_SNAPSHOT_PROFILE {
        return document
            .merge_state
            .get("media_type")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
    }
    document
        .history
        .iter()
        .find_map(|event| {
            event.extra_headers.get("repr-type").cloned().or_else(|| {
                event
                    .content_type
                    .as_ref()
                    .filter(|value| {
                        !value.split(';').next().is_some_and(|value| {
                            value
                                .trim()
                                .eq_ignore_ascii_case("application/http-patches")
                        })
                    })
                    .cloned()
            })
        })
        .or_else(|| Some("text/plain; charset=utf-8".into()))
}

fn json_media_type(value: &str) -> bool {
    matches!(
        value.split(';').next().map(str::trim),
        Some("application/json" | "application/linked+json")
    )
}

pub(super) fn apply_snapshot(
    document: &mut ReplicaDocument,
    update: &Update,
) -> Result<(), ReplicaError> {
    let parents: Vec<_> = update.parents.iter().map(ToString::to_string).collect();
    if !same_frontier(&document.heads, &parents) {
        return Err(ReplicaError::Conflict(
            "JSON snapshot requires the complete current parent frontier".into(),
        ));
    }
    let body = match (&update.body, &update.patches) {
        (Some(body), None) => body,
        _ => {
            return Err(ReplicaError::Invalid(
                "JSON snapshot profile requires a finite snapshot body".into(),
            ))
        }
    };
    if body.len() > super::MAX_HISTORY_PAGE_BYTES {
        return Err(ReplicaError::Limit("JSON snapshot exceeds 32 MiB".into()));
    }
    let declared = update
        .content_type
        .clone()
        .or_else(|| media_type(document))
        .ok_or_else(|| {
            ReplicaError::Invalid("New JSON resources require an explicit media type".into())
        })?;
    if !json_media_type(&declared) {
        return Err(ReplicaError::Invalid(
            "JSON snapshot media type must be application/json or application/linked+json".into(),
        ));
    }
    if !document.history.is_empty() && media_type(document).as_deref() != Some(declared.as_str()) {
        return Err(ReplicaError::Conflict(
            "Changing representation media type requires an explicit migration".into(),
        ));
    }
    let _: serde_json::Value = serde_json::from_slice(body).map_err(|error| {
        ReplicaError::Invalid(format!(
            "Invalid JSON at line {}, column {}",
            error.line(),
            error.column()
        ))
    })?;
    let content = std::str::from_utf8(body)
        .map_err(|_| ReplicaError::Invalid("JSON snapshot must be UTF-8".into()))?;
    super::validate_digest(content, update)?;
    document.content = content.to_owned();
    document.merge_state = serde_json::json!({"schema_version": 1, "media_type": declared});
    Ok(())
}

pub(super) fn snapshot_at(
    document: &ReplicaDocument,
    frontier: &[Version],
) -> Result<Update, ReplicaError> {
    let declared = media_type(document)
        .filter(|value| json_media_type(value))
        .ok_or_else(|| {
            ReplicaError::Invalid("Missing or invalid persisted JSON media type".into())
        })?;
    if document
        .merge_state
        .get("schema_version")
        .and_then(|value| value.as_u64())
        != Some(1)
    {
        return Err(ReplicaError::Invalid(
            "Unsupported JSON snapshot schema".into(),
        ));
    }
    document.ancestors(frontier)?;
    if frontier.len() != 1 {
        return Err(ReplicaError::HistoryUnavailable(frontier.to_vec()));
    }
    let ids: Vec<_> = frontier.iter().map(ToString::to_string).collect();
    let mut snapshot = if same_frontier(&ids, &document.heads) {
        document.snapshot()
    } else {
        document
            .history
            .iter()
            .find(|event| event.version == frontier)
            .cloned()
            .ok_or_else(|| ReplicaError::HistoryUnavailable(frontier.to_vec()))?
    };
    let body = snapshot
        .body
        .as_ref()
        .filter(|_| snapshot.patches.is_none())
        .ok_or_else(|| ReplicaError::Invalid("Persisted JSON history is not a snapshot".into()))?;
    let _: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| ReplicaError::Invalid("Persisted JSON snapshot is invalid".into()))?;
    snapshot.content_type = Some(declared);
    Ok(snapshot)
}

pub(super) fn representation_at(
    document: &ReplicaDocument,
    frontier: Option<&[Version]>,
) -> Result<RepresentationAt, ReplicaError> {
    let current: Vec<_> = document.heads.iter().map(Version::new).collect();
    let snapshot = document.snapshot_at(frontier.unwrap_or(&current))?;
    Ok(RepresentationAt {
        bytes: snapshot.body.ok_or_else(|| {
            ReplicaError::Invalid("Materialized representation has no body".into())
        })?,
        media_type: snapshot.content_type.ok_or_else(|| {
            ReplicaError::Invalid("Materialized representation has no declared media type".into())
        })?,
        frontier: snapshot.version,
    })
}
