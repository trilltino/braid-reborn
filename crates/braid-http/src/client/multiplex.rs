//! Client-side Braid Multiplexing implementation.
#![cfg(not(target_arch = "wasm32"))]

use crate::error::{BraidError, Result};
use crate::protocol::multiplex::{MultiplexEvent, MultiplexParser};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Manages a multiplexed connection to a Braid server.
pub struct Multiplexer {
    pub id: String,
    requests: Arc<Mutex<HashMap<String, MultiplexedRequestState>>>,
}

impl Multiplexer {
    pub fn new(_origin: String, id: String) -> Self {
        Self {
            id,
            requests: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn run_stream(&self, body: reqwest::Response) -> Result<()> {
        let mut stream = body.bytes_stream();
        let mut parser = MultiplexParser::new();

        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res.map_err(|e| BraidError::Http(e.to_string()))?;
            match parser.feed(&chunk) {
                Ok(events) => {
                    for event in events {
                        self.handle_event(event).await?;
                    }
                }
                Err(e) => return Err(BraidError::Protocol(e)),
            }
        }
        self.requests.lock().await.clear();
        Ok(())
    }

    async fn handle_event(&self, event: MultiplexEvent) -> Result<()> {
        match event {
            MultiplexEvent::StartResponse(_id) => {}
            MultiplexEvent::Data(id, bytes) => {
                let sender = self
                    .requests
                    .lock()
                    .await
                    .get(&id)
                    .map(|state| state.raw_tx.clone());
                if let Some(sender) = sender {
                    if sender.try_send(bytes).is_err() {
                        self.requests.lock().await.remove(&id);
                    }
                }
            }
            MultiplexEvent::CloseResponse(id) => {
                let mut requests = self.requests.lock().await;
                requests.remove(&id);
            }
        }
        Ok(())
    }

    pub async fn add_request(&self, id: String, raw_tx: async_channel::Sender<Vec<u8>>) {
        let mut requests = self.requests.lock().await;
        requests.insert(id, MultiplexedRequestState { raw_tx });
    }
}

pub struct MultiplexedRequestState {
    pub raw_tx: async_channel::Sender<Vec<u8>>,
}
