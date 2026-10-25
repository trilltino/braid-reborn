//! Line-oriented JSON oracle over the upstream engine. One request per stdin line.
use diamond_types as dt;
use std::io::{BufRead, Write};

include!("../runner.rs");

#[derive(Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum Request {
    Run { script: Script },
    Decode { bytes: Vec<u8> },
    Merge { chunks: Vec<Vec<u8>> },
}

fn main() {
    let stdin = std::io::stdin();
    let mut out = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line.expect("stdin");
        if line.trim().is_empty() {
            continue;
        }
        let reply = match serde_json::from_str::<Request>(&line) {
            Err(error) => serde_json::json!({ "error": error.to_string() }),
            Ok(request) => {
                // The oracle must report engine panics rather than die silently.
                let outcome = std::panic::catch_unwind(|| match request {
                    Request::Run { script } => run(&script).map(|o| serde_json::to_value(o).unwrap()),
                    Request::Decode { bytes } => decode_text(&bytes).map(|t| serde_json::json!({ "text": t })),
                    Request::Merge { chunks } => merge_text(&chunks).map(|t| serde_json::json!({ "text": t })),
                });
                match outcome {
                    Ok(Ok(value)) => value,
                    Ok(Err(error)) => serde_json::json!({ "error": error }),
                    Err(_) => serde_json::json!({ "panic": true }),
                }
            }
        };
        writeln!(out, "{reply}").unwrap();
        out.flush().unwrap();
    }
}
