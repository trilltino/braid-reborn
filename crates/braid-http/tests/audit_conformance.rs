use braid_http::{
    client::{message_to_update, MessageParser, ParserLimits},
    protocol::{encode_request, format_update, WireCodec},
    types::{BraidRequest, Patch, Update, Version},
};
use bytes::Bytes;

#[test]
fn every_fragmentation_preserves_binary_frontiers_and_patches() {
    let mut snapshot = Update::snapshot(Version::new("a"), Bytes::from_static(b"\x00\xff\r\n"));
    snapshot.version.push(Version::new("b"));
    snapshot.current_version = Some(vec![Version::new("tip-a"), Version::new("tip-b")]);
    let patch = Update::patched(
        Version::new("c"),
        vec![
            Patch::text("[1:2]", "😀"),
            Patch::bytes("[5:6]", Bytes::from_static(b"\xff")),
        ],
    )
    .with_parents(snapshot.version.clone());
    let frame = [
        format_update(&snapshot).unwrap(),
        format_update(&patch).unwrap(),
    ]
    .concat();
    for split in 0..=frame.len() {
        let mut parser = MessageParser::new();
        let mut messages = parser.feed(&frame[..split]).unwrap();
        messages.extend(parser.feed(&frame[split..]).unwrap());
        parser.finish().unwrap();
        assert_eq!(messages.len(), 2, "split {split}");
        let first = message_to_update(messages.remove(0)).unwrap();
        let second = message_to_update(messages.remove(0)).unwrap();
        assert_eq!(first.version, snapshot.version);
        assert_eq!(first.current_version, snapshot.current_version);
        assert_eq!(first.body, snapshot.body);
        assert_eq!(second.patches, patch.patches);
        assert_eq!(second.parents, patch.parents);
    }
    let mut parser = MessageParser::new();
    let mut count = 0;
    for b in &frame {
        count += parser.feed(&[*b]).unwrap().len();
    }
    parser.finish().unwrap();
    assert_eq!(count, 2);
}

#[test]
fn malformed_framing_and_truncated_eof_fail_closed() {
    for frame in [
        "Patches: garbage\r\n\r\n",
        "Patches: -1\r\n\r\n",
        "Content-Length: +1\r\n\r\nx",
        "Content-Length: 0\r\nContent-Length: 1\r\n\r\nx",
        "Patches: 1\r\nContent-Length: 0\r\n\r\n",
        "Patches: 1\r\n\r\nContent-Length: 1\r\n\r\nx",
    ] {
        let mut parser = MessageParser::new();
        assert!(parser.feed(frame.as_bytes()).is_err(), "{frame}");
        assert!(parser.feed(b"Content-Length: 0\r\n\r\n").is_err());
    }
    let bytes = b"Patches: 1\r\n\r\nContent-Length: 3\r\nContent-Range: text [0:0]\r\n\r\nabc";
    for len in 1..bytes.len() {
        let mut parser = MessageParser::new();
        assert!(parser.feed(&bytes[..len]).unwrap().is_empty());
        assert!(parser.finish().is_err(), "len {len}");
    }
}

#[test]
fn limits_reject_declarations_before_allocating_payloads() {
    let limits = ParserLimits {
        max_header_bytes: 64,
        max_body_bytes: 8,
        max_patches: 2,
        max_update_bytes: 128,
        ..Default::default()
    };
    for frame in [
        "Content-Length: 9\r\n\r\n",
        "Patches: 3\r\n\r\n",
        "Content-Length: 99999999999999999999999999999999\r\n\r\n",
    ] {
        assert!(MessageParser::with_limits(limits)
            .feed(frame.as_bytes())
            .is_err());
    }
    assert!(MessageParser::with_limits(limits)
        .feed(&[b'a'; 65])
        .is_err());
}

#[test]
fn patch_request_has_decimal_count_and_framed_bytes() {
    let request = BraidRequest::new()
        .with_method("PUT")
        .with_version(Version::new("edit"))
        .with_patches(vec![Patch::text("[0:0]", "é")]);
    let (headers, body) = encode_request(&request).unwrap();
    assert_eq!(headers["patches"], "1");
    assert_eq!(
        body,
        Bytes::from_static(
            "content-length: 2\r\ncontent-range: text [0:0]\r\n\r\né\r\n".as_bytes()
        )
    );
    let update = WireCodec::decode_entity(headers, body).unwrap();
    assert_eq!(update.patches, request.patches);
    assert_eq!(update.version, request.version.clone().unwrap());
    let ambiguous = request.clone().with_body("snapshot");
    assert!(encode_request(&ambiguous).is_err());
}

#[test]
fn unicode_patch_ranges_are_checked_atomically() {
    use braid_http::protocol::text::apply_text_patches;
    assert_eq!(
        apply_text_patches(
            "a😀éz",
            &[Patch::text("[1:2]", "X"), Patch::text("[3:4]", "!")]
        )
        .unwrap(),
        "aXé!"
    );
    assert!(apply_text_patches(
        "😀",
        &[Patch::text("[0:1]", "ok"), Patch::text("[3:4]", "bad")]
    )
    .is_err());
    assert!(apply_text_patches(
        "abc",
        &[Patch::text("[0:2]", "a"), Patch::text("[1:3]", "b")]
    )
    .is_err());
}

#[test]
fn repeated_version_fields_preserve_the_whole_frontier() {
    let mut headers = http::HeaderMap::new();
    headers.append("version", http::HeaderValue::from_static("\"a\""));
    headers.append("version", http::HeaderValue::from_static("\"b\""));
    let parsed = braid_http::protocol::headers_from_http(&headers).unwrap();
    let update = WireCodec::decode_entity(parsed, Bytes::new()).unwrap();
    assert_eq!(update.version, vec![Version::new("a"), Version::new("b")]);
}
