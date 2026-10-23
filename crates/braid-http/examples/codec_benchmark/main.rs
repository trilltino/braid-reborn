//! Same-binary baseline/current parser timings. Run with the same Cargo profile.
mod error {
    pub use braid_http::error::*;
}
mod types {
    pub use braid_http::types::Patch;
}
mod protocol {
    pub use braid_http::protocol::parse_content_range;
}
#[allow(dead_code)]
mod baseline_parser;
use std::{hint::black_box, time::Instant};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let frame = [
        b"X-Long: ".as_slice(),
        &vec![b'a'; 8192],
        b"\r\nVersion: \"v\"\r\nContent-Length: 1\r\n\r\nx",
    ]
    .concat();
    let iterations = 20;
    let before = Instant::now();
    for _ in 0..iterations {
        let mut parser = baseline_parser::MessageParser::new();
        for chunk in frame.chunks(1) {
            black_box(parser.feed(black_box(chunk))?);
        }
        parser.finish()?;
    }
    let baseline = before.elapsed().as_micros();
    let before = Instant::now();
    for _ in 0..iterations {
        let mut parser = braid_http::client::MessageParser::new();
        for chunk in frame.chunks(1) {
            black_box(parser.feed(black_box(chunk))?);
        }
        parser.finish()?;
    }
    println!("{{\"workload\":\"8192-byte header, one-byte fragments\",\"iterations\":{iterations},\"baseline_us\":{baseline},\"current_us\":{}}}", before.elapsed().as_micros());
    Ok(())
}
