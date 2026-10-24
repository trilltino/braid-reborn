//! Bounded chat update streams encoded by the canonical Braid wire codec.
use crate::core::{config::AppState, models::Message, store::json_store::UpdateType};
use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use braid_http::{
    protocol::{
        format_update, headers_from_http, parse_heartbeat_duration, parse_subscribe,
        parse_version_header,
    },
    types::{Update, Version},
};
use bytes::Bytes;
use std::{collections::HashSet, time::Duration};

fn message_update(message: &Message) -> Result<Update, std::io::Error> {
    Ok(Update {
        version: vec![Version::new(&message.version)],
        parents: message.parents.clone(),
        body: Some(
            serde_json::to_vec(message)
                .map_err(std::io::Error::other)?
                .into(),
        ),
        content_type: Some("application/json".into()),
        ..Update::default()
    })
}
fn encode(update: &Update) -> Result<Bytes, std::io::Error> {
    format_update(update).map_err(std::io::Error::other)
}

pub async fn braid_subscribe(
    Path(room_id): Path<String>,
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response<Body>, StatusCode> {
    let fields = headers_from_http(&headers).map_err(|_| StatusCode::BAD_REQUEST)?;
    if fields.contains_key("version")
        || !fields
            .get("subscribe")
            .map(|v| parse_subscribe(v))
            .transpose()
            .map_err(|_| StatusCode::BAD_REQUEST)?
            .unwrap_or(false)
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let parents = fields
        .get("parents")
        .map(|v| parse_version_header(v))
        .transpose()
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let heartbeat = fields
        .get("heartbeats")
        .map(|v| parse_heartbeat_duration(v))
        .transpose()
        .map_err(|_| StatusCode::BAD_REQUEST)?
        .unwrap_or(Duration::from_secs(30));
    if heartbeat < Duration::from_millis(100) {
        return Err(StatusCode::BAD_REQUEST);
    }

    // Register before acquiring the room's read lock. Mutations publish while
    // holding its write lock. Captured event identities suppress the overlap.
    let channel = state.store.get_channel(&room_id).await;
    let mut receiver = channel.tx.subscribe();
    let mut covered = HashSet::new();
    let mut initial = Vec::new();
    if let Some(room) = state.store.get_room(&room_id).await.map_err(|error| {
        tracing::error!(%error, "Could not load subscribed room");
        StatusCode::INTERNAL_SERVER_ERROR
    })? {
        let room = room.read().await;
        let exported = room.crdt.export_state();
        covered.extend(exported.version_graph.keys().cloned());
        if let Some(parents) = &parents {
            let missing = room
                .crdt
                .generate_sync_braid(parents)
                .map_err(|_| StatusCode::from_u16(432).unwrap_or(StatusCode::CONFLICT))?;
            let wanted: HashSet<_> = missing.iter().map(|u| u.version.clone()).collect();
            // Replay actual retained events to recover each message at its event
            // version, rather than labelling its current content as old history.
            let mut replay = crate::chat::crdt::ChatCrdt::new(&room_id, &exported.node_id);
            for event in exported.events {
                let selected = wanted.contains(&event.version);
                let messages = replay.merge_updates(vec![event]).map_err(|error| {
                    tracing::error!(%error, "Invalid retained chat history");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
                if selected {
                    initial.extend(messages);
                }
            }
        } else {
            initial = room
                .crdt
                .get_messages_sorted()
                .into_iter()
                .filter(|m| !m.deleted)
                .cloned()
                .collect();
        }
    } else if parents.as_ref().is_some_and(|v| !v.is_empty()) {
        return Err(StatusCode::from_u16(432).unwrap_or(StatusCode::CONFLICT));
    }
    let owner = state.store.clone();
    let stream = async_stream::try_stream! {
        let _owner = owner;
        for message in initial { yield encode(&message_update(&message)?)?; }
        let mut timer = tokio::time::interval(heartbeat);
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let received = tokio::select! {
                received = receiver.recv() => Some(received),
                _ = timer.tick() => None,
            };
            let Some(received) = received else {
                yield Bytes::from_static(b"\r\n");
                continue;
            };
                    let event = match received {
                        Ok(event) => event,
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            Err(std::io::Error::other("Chat subscriber lagged; resume from committed parents"))?;
                            break;
                        }
                    };
                    if event.crdt_version.as_ref().is_some_and(|id| covered.contains(id)) { continue; }
                    let update = if matches!(event.update_type, UpdateType::Message) {
                        let message: Message = serde_json::from_value(event.data).map_err(std::io::Error::other)?;
                        if event.crdt_version.as_ref().is_some_and(|id| *id != message.version) {
                            Err(std::io::Error::other("Chat broadcast identity differs from message"))?;
                        }
                        message_update(&message)?
                    } else {
                        let kind = match event.update_type {
                            UpdateType::Presence => "presence", UpdateType::Typing => "typing",
                            UpdateType::RoomUpdate => "room", UpdateType::Sync => "sync", _ => "unknown",
                        };
                        let mut update = Update {
                            version: event.crdt_version.iter().map(Version::new).collect(),
                            body: Some(serde_json::to_vec(&event.data).map_err(std::io::Error::other)?.into()),
                            content_type: Some("application/json".into()), ..Update::default()
                        };
                        update.extra_headers.insert("type".into(), kind.into());
                        update
                    };
                    yield encode(&update)?;
        }
    };
    let stream: std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<Bytes, std::io::Error>> + Send>,
    > = Box::pin(stream);
    Response::builder()
        .status(209)
        .header("content-type", "application/json")
        .header("subscribe", "true")
        .header("merge-type", "diamond")
        .header("cache-control", "no-cache")
        .header("heartbeats", heartbeat.as_secs_f64().to_string())
        .body(Body::from_stream(stream))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}
