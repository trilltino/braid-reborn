//! JSON-lines adapter for the pinned braid-fuzz HTTP and subscription suites.
use braid_http::{
    protocol::WireCodec,
    types::{BraidRequest, Patch, Update, Version},
    BraidClient,
};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, BufReader};

fn emit(value: Value) {
    println!("{value}");
}
fn update_event(update: Update) {
    let mut data = json!({"version":update.version,"parents":update.parents});
    if let Some(body) = update.body {
        data["body"] = json!(String::from_utf8_lossy(&body));
    }
    if let Some(patches) = update.patches {
        data["patches"]=json!(patches.into_iter().map(|patch| {
            let range=patch.range.trim_matches(['[',']']).split(':').map(|n|n.parse::<usize>().map(Value::from).unwrap_or(Value::Null)).collect::<Vec<_>>();
            json!({"unit":patch.unit,"range":range,"content":String::from_utf8_lossy(&patch.content)})
        }).collect::<Vec<_>>());
    }
    emit(json!({"event":"update","data":data}));
}
pub(crate) fn parse_request(message: &Value) -> Result<BraidRequest, String> {
    let mut request = BraidRequest::new().with_method(message["method"].as_str().unwrap_or("GET"));
    for (name, target) in [
        ("version", &mut request.version),
        ("parents", &mut request.parents),
    ] {
        if let Some(value) = message.get(name) {
            *target = Some(
                serde_json::from_value::<Vec<Version>>(value.clone()).map_err(|e| e.to_string())?,
            );
        }
    }
    if let Some(patches) = message["patches"].as_array() {
        request.patches = Some(
            patches
                .iter()
                .map(|p| {
                    Patch::new(
                        p["unit"].as_str().unwrap_or("text"),
                        p["range"].as_str().unwrap_or("[0:0]"),
                        p["content"].as_str().unwrap_or("").to_owned(),
                    )
                })
                .collect(),
        );
    }
    if let Some(seconds) = message["heartbeats"].as_u64() {
        request.heartbeat_interval = Some(seconds);
    }
    if let Some(body) = message["body"].as_str() {
        request = request.with_body(body.to_owned());
    }
    if let Some(headers) = message["headers"].as_object() {
        for (key, value) in headers {
            let value = value.as_str().ok_or("Header value is not a string")?;
            match key.to_ascii_lowercase().as_str() {
                "merge-type" => request.merge_type = Some(value.into()),
                "content-type" => request.content_type = Some(value.into()),
                _ => {
                    request.extra_headers.insert(key.clone(), value.into());
                }
            }
        }
    }
    Ok(request)
}
#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = BraidClient::new()?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut http: Option<tokio::task::JoinHandle<()>> = None;
    while let Some(line) = lines.next_line().await? {
        let message: Value = serde_json::from_str(&line)?;
        let id = message["id"].clone();
        match message["cmd"].as_str().unwrap_or("") {
            "hello" => emit(json!({"id":id,"ok":true})),
            "results" => {
                emit(json!({"id":id,"ok":true}));
                break;
            }
            "close-http" => {
                if let Some(task) = http.take() {
                    task.abort();
                }
                emit(json!({"id":id,"ok":true}));
            }
            "open-http" => {
                let request = match parse_request(&message) {
                    Ok(req) => req,
                    Err(e) => {
                        emit(json!({"id":id,"error":e}));
                        continue;
                    }
                };
                let url = message["url"].as_str().ok_or("Missing URL")?.to_owned();
                let subscribe = message["subscribe"].as_bool().unwrap_or(false);
                if subscribe {
                    if let Some(task) = http.take() {
                        task.abort();
                    }
                }
                let client = client.clone();
                let task = spawn_request(client, request, url, subscribe);
                if subscribe {
                    http = Some(task);
                }
                emit(json!({"id":id,"ok":true}));
            }
            _ => emit(json!({"id":id,"error":"Unsupported controller command"})),
        }
    }
    if let Some(task) = http {
        task.abort();
    }
    Ok(())
}

pub(crate) fn spawn_request(
    client: BraidClient,
    request: BraidRequest,
    url: String,
    subscribe: bool,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let result = async {
            if subscribe {
                let mut subscription = client.subscribe_reliable(&url, request).await?;
                while let Some(update) = subscription.next().await {
                    let update = update?;
                    update_event(update.clone());
                    subscription.acknowledge(&update).await?;
                }
            } else {
                let put = request.method == "PUT";
                let response = client.fetch(&url, request).await?;
                if put {
                    emit(json!({"event":"ack","data":{"status":response.status}}));
                } else {
                    update_event(WireCodec::decode_entity(response.headers, response.body)?);
                }
            }
            Ok::<(), braid_http::BraidError>(())
        }
        .await;
        if let Err(e) = result {
            emit(json!({"event":"error","data":{"message":e.to_string()}}));
        }
    })
}
