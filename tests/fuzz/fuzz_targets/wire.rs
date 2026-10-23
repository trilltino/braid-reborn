#![no_main]
use braid_http::client::{MessageParser, ParserLimits};
use libfuzzer_sys::fuzz_target;

fn decode(data: &[u8], chunk: usize) -> Result<Vec<serde_json::Value>, braid_http::BraidError> {
    let mut parser = MessageParser::with_limits(ParserLimits {
        max_header_bytes: 8192,
        max_header_fields: 64,
        max_header_value_bytes: 4096,
        max_body_bytes: 65536,
        max_patches: 128,
        max_update_bytes: 131072,
    });
    let mut updates = Vec::new();
    for chunk in data.chunks(chunk) {
        for message in parser.feed(chunk)? {
            let update = braid_http::client::utils::message_to_update(message)?;
            // Accepted input must also be safely encodable by the canonical codec.
            braid_http::protocol::format_update(&update)?;
            updates.push(serde_json::to_value(update)?);
        }
    }
    parser.finish()?;
    Ok(updates)
}
fuzz_target!(|data: &[u8]| {
    if data.len() > 65536 {
        return;
    }
    let whole = decode(data, data.len().max(1));
    let fragmented = decode(
        data,
        data.first().copied().map_or(1, |n| usize::from(n) % 31 + 1),
    );
    match (whole, fragmented) {
        (Ok(a), Ok(b)) => assert_eq!(a, b),
        (Err(_), Err(_)) => {}
        _ => panic!("Parser acceptance depends on transport fragmentation"),
    }
});
