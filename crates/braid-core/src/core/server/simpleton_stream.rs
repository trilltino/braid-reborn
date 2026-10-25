//! Per-subscriber Simpleton views. Own writes suppress their echo; concurrent
//! corrections wait for a quiet interval and use that peer's optimistic base.
use crate::{
    replica::{ReplicaDocument, ReplicaError, ReplicaStore},
    types::{Update, Version},
};
use axum::{body::Body, response::Response};
use braid_http::protocol::{format_update, parse_peer};
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tokio::{sync::broadcast, time::Instant};

pub(super) struct SimpletonSubscription {
    pub store: ReplicaStore,
    pub resource: String,
    pub peer: Option<String>,
    pub parents: Option<Vec<Version>>,
    pub document: ReplicaDocument,
    pub receiver: broadcast::Receiver<Update>,
    pub keepalive: Arc<broadcast::Sender<Update>>,
    pub heartbeat: Duration,
}

fn projection(
    document: &ReplicaDocument,
    from: &[Version],
    to: &[Version],
) -> Result<Update, ReplicaError> {
    let before = document.snapshot_at(from)?;
    let after = document.snapshot_at(to)?;
    let text = after
        .body_str()
        .ok_or_else(|| ReplicaError::Invalid("Non-text Simpleton state".into()))?;
    let (patch, _) = crate::simpleton::scalar_diff(
        before
            .body_str()
            .ok_or_else(|| ReplicaError::Invalid("Non-text Simpleton base".into()))?,
        text,
    )
    .map_err(|e| ReplicaError::Invalid(e.to_string()))?;
    let mut update = Update {
        version: to.to_vec(),
        parents: from.to_vec(),
        patches: Some(vec![patch]),
        merge_type: Some("simpleton".into()),
        content_type: Some("text/plain; charset=utf-8".into()),
        current_version: Some(document.snapshot().version),
        ..Update::default()
    };
    update
        .extra_headers
        .insert("repr-digest".into(), crate::simpleton::digest(text));
    Ok(update)
}
fn event_peer(update: &Update) -> Result<Option<String>, ReplicaError> {
    update
        .extra_headers
        .get("peer")
        .map(|value| parse_peer(value).map_err(|error| ReplicaError::Invalid(error.to_string())))
        .transpose()
}
fn encode(update: &Update) -> Result<Bytes, std::io::Error> {
    format_update(update).map_err(std::io::Error::other)
}
impl SimpletonSubscription {
    pub fn into_response(self) -> Result<Response, ReplicaError> {
        let Self {
            store,
            resource,
            peer,
            parents,
            document,
            mut receiver,
            keepalive,
            heartbeat,
        } = self;
        let current = document.snapshot().version;
        let initial = match &parents {
            Some(from) => projection(&document, from, &current)?,
            None => document.snapshot(),
        };
        let mut view = current;
        let mut correction = None;
        // A lost ACK may reconnect from before an already accepted own write.
        // Its idempotent retry sends no new broadcast, so recover that base here.
        if let (Some(from), Some(peer)) = (&parents, &peer) {
            let known = document.ancestors(from)?;
            for event in document.history.iter().rev() {
                if event_peer(event)?.as_deref() == Some(peer.as_str()) {
                    if event
                        .version
                        .iter()
                        .any(|id| !known.contains(&id.to_string()))
                        && event.version != view
                    {
                        view = event.version.clone();
                        correction = Some(Instant::now() + Duration::from_millis(23));
                    }
                    break;
                }
            }
        }
        let stream = async_stream::try_stream! {
            let _keepalive = keepalive;
            yield encode(&initial)?;
            let mut ticker = tokio::time::interval(heartbeat);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            let mut unused = 0u32;
            loop {
                // This local select result stays on the stream stack; boxing
                // would allocate for every received event just to shrink it.
                #[allow(clippy::large_enum_variant)]
                enum Wake { Event(Result<Update, broadcast::error::RecvError>), Heartbeat, Correct }
                let wake = tokio::select! {
                    event = receiver.recv() => Wake::Event(event),
                    _ = ticker.tick() => Wake::Heartbeat,
                    _ = async { match correction { Some(deadline) => tokio::time::sleep_until(deadline).await, None => std::future::pending().await } } => Wake::Correct,
                };
                match wake {
                    Wake::Heartbeat => { yield Bytes::from_static(b"\r\n"); }
                    Wake::Correct => {
                        correction = None;
                        let document = store.load(&resource).await.map_err(std::io::Error::other)?
                            .ok_or_else(|| std::io::Error::other("Subscribed resource disappeared"))?;
                        let target = document.snapshot().version;
                        if target != view {
                            yield encode(&projection(&document, &view, &target).map_err(std::io::Error::other)?)?;
                            view = target;
                        }
                    }
                    Wake::Event(Err(broadcast::error::RecvError::Closed)) => break,
                    Wake::Event(Err(broadcast::error::RecvError::Lagged(_))) => {
                        Err(std::io::Error::other("Simpleton subscriber lagged; reconnect from committed parents"))?;
                    }
                    Wake::Event(Ok(event)) => {
                        let target = event.current_version.clone().ok_or_else(|| std::io::Error::other("Missing committed frontier"))?;
                        let source = event_peer(&event).map_err(std::io::Error::other)?;
                        if peer.is_some() && source == peer {
                            view = event.version;
                            if view == target { correction = None; unused = 0; }
                            else {
                                unused = unused.saturating_add(1).min(14);
                                let delay = (23.0 * 1.5f64.powi(unused.saturating_sub(1) as i32)).min(3000.0);
                                correction = Some(Instant::now() + Duration::from_secs_f64(delay / 1000.0));
                            }
                            continue;
                        }
                        if correction.is_some() { continue; }
                        let document = store.load(&resource).await.map_err(std::io::Error::other)?
                            .ok_or_else(|| std::io::Error::other("Subscribed resource disappeared"))?;
                        let covered = document.ancestors(&view).map_err(std::io::Error::other)?;
                        if target.iter().all(|v| covered.contains(&v.to_string())) { continue; }
                        yield encode(&projection(&document, &view, &target).map_err(std::io::Error::other)?)?;
                        view = target;
                    }
                }
            }
        };
        let stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send>,
        > = Box::pin(stream);
        Response::builder()
            .status(209)
            .header("subscribe", "true")
            .header("merge-type", "simpleton")
            .header("heartbeats", heartbeat.as_secs_f64().to_string())
            .header("cache-control", "no-cache")
            .body(Body::from_stream(stream))
            .map_err(|e| ReplicaError::Invalid(e.to_string()))
    }
}
