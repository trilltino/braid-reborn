//! Independent braid-fuzz JSON controller using the production Rust Simpleton client.
#[path = "../../braid-http/examples/fuzz_controller.rs"]
mod raw;
use braid_core::simpleton::SimpletonClient;
use braid_http::{client::ChannelJournal, BraidClient};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = BraidClient::new()?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut text: Option<SimpletonClient> = None;
    let mut http: Option<tokio::task::JoinHandle<()>> = None;
    while let Some(line) = lines.next_line().await? {
        let message: Value = serde_json::from_str(&line)?;
        let id = message["id"].clone();
        let result: Result<Value, Box<dyn std::error::Error>> = async {
            match message["cmd"].as_str().unwrap_or("") {
                "hello" | "results" => Ok(json!({})),
                "sync-text" => {
                    if let Some(old) = text.take() {
                        old.close().await;
                    }
                    let url = message["url"].as_str().ok_or("Missing URL")?;
                    text = Some(
                        SimpletonClient::connect(&client, url, ChannelJournal::memory()).await?,
                    );
                    Ok(json!({}))
                }
                "edit" => {
                    let text = text.as_ref().ok_or("No active sync-text")?;
                    let mut chars: Vec<char> = text.status().borrow().text.chars().collect();
                    let pos =
                        usize::try_from(message["pos"].as_u64().unwrap_or(0))?.min(chars.len());
                    let len = usize::try_from(message["len"].as_u64().unwrap_or(0))?
                        .min(chars.len() - pos);
                    chars.splice(
                        pos..pos + len,
                        message["text"].as_str().unwrap_or("").chars(),
                    );
                    text.changed(chars.into_iter().collect()).await?;
                    Ok(json!({}))
                }
                "send-text" => {
                    let text = text.as_ref().ok_or("No active sync-text")?;
                    Ok(json!({"state":text.status().borrow().text}))
                }
                "ack" => {
                    let mut status = text.as_ref().ok_or("No active sync-text")?.status();
                    tokio::time::timeout(std::time::Duration::from_secs(60), async {
                        loop {
                            let current = status.borrow_and_update().clone();
                            if let Some(error) = current.error {
                                return Err(error);
                            }
                            if !current.has_pending_edit && current.outstanding_puts == 0 {
                                return Ok(());
                            }
                            status.changed().await.map_err(|e| e.to_string())?;
                        }
                    })
                    .await??;
                    Ok(json!({}))
                }
                "end-sync" => {
                    if let Some(old) = text.take() {
                        old.close().await;
                    }
                    Ok(json!({}))
                }
                "close-http" => {
                    if let Some(task) = http.take() {
                        task.abort();
                    }
                    Ok(json!({}))
                }
                "open-http" => {
                    let request = raw::parse_request(&message)?;
                    let url = message["url"].as_str().ok_or("Missing URL")?.to_owned();
                    let subscribe = message["subscribe"].as_bool().unwrap_or(false);
                    if subscribe {
                        if let Some(task) = http.take() {
                            task.abort();
                        }
                    }
                    let task = raw::spawn_request(client.clone(), request, url, subscribe);
                    if subscribe {
                        http = Some(task);
                    }
                    Ok(json!({}))
                }
                _ => Err("Unsupported controller command".into()),
            }
        }
        .await;
        match result {
            Ok(mut reply) => {
                reply["id"] = id;
                reply["ok"] = json!(true);
                println!("{reply}");
            }
            Err(error) => println!("{}", json!({"id":id,"error":error.to_string()})),
        }
        if message["cmd"] == "results" {
            break;
        }
    }
    if let Some(text) = text {
        text.close().await;
    }
    if let Some(task) = http {
        task.abort();
    }
    Ok(())
}
