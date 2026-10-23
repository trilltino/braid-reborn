use braid_http::{
    client::{BraidHeaders, MessageParser, ParserLimits},
    protocol::{format_update, format_version_header, headers_from_http, parse_version_header},
    types::{Update, Version},
};
use http::{HeaderMap, HeaderValue};

#[test]
fn http_adapters_reject_ambiguous_or_nontext_protocol_headers() {
    for name in [
        "patches",
        "content-length",
        "content-range",
        "merge-type",
        "subscribe",
    ] {
        let mut headers = HeaderMap::new();
        headers.append(name, HeaderValue::from_static("1"));
        headers.append(name, HeaderValue::from_static("2"));
        assert!(headers_from_http(&headers).is_err(), "{name}");
        assert!(BraidHeaders::from_header_map(&headers).is_err(), "{name}");
    }
    let mut headers = HeaderMap::new();
    headers.insert("patches", HeaderValue::from_bytes(b"\xff").unwrap());
    assert!(headers_from_http(&headers).is_err());
    headers.insert("patches", HeaderValue::from_static("[{}]"));
    assert!(BraidHeaders::from_header_map(&headers).is_err());
}

#[test]
fn frontiers_follow_versions_05_structured_strings_and_display_strings() {
    let versions = vec![
        Version::new("z"),
        Version::new("é😀"),
        Version::new("a\"\\b"),
        Version::new("z"),
    ];
    let header = format_version_header(&versions);
    assert!(header.contains("%\""));
    assert!(header.is_ascii());
    let parsed = parse_version_header(&header).unwrap();
    assert_eq!(
        parsed.iter().map(ToString::to_string).collect::<Vec<_>>(),
        ["a\"\\b", "z", "é😀"]
    );
    for invalid in [
        "token",
        "12",
        "[\"v\"]",
        "\"\"",
        "\"v\";parameter=1",
        "(\"v\")",
    ] {
        assert!(parse_version_header(invalid).is_err(), "{invalid}");
    }
    let mut headers = HeaderMap::new();
    headers.append("parents", HeaderValue::from_static("\"z\""));
    headers.append("parents", HeaderValue::from_static("\"a\""));
    headers.insert("subscribe", HeaderValue::from_static("?1"));
    let parsed = BraidHeaders::from_header_map(&headers).unwrap();
    assert!(parsed.subscribe);
    assert_eq!(
        parsed.parents.unwrap(),
        vec![Version::new("a"), Version::new("z")]
    );
}

#[test]
fn mixed_line_endings_and_every_fragment_boundary_preserve_frame_boundaries() {
    let bytes =
        b"Version: \"a\"\nContent-Length: 1\n\naVersion: \"b\"\r\nContent-Length: 1\r\n\r\nb";
    for split in 0..=bytes.len() {
        let mut parser = MessageParser::new();
        let mut messages = parser.feed(&bytes[..split]).unwrap();
        messages.extend(parser.feed(&bytes[split..]).unwrap());
        parser.finish().unwrap();
        assert_eq!(messages.len(), 2, "split {split}");
        assert_eq!(messages[0].body.as_ref(), b"a");
        assert_eq!(messages[1].body.as_ref(), b"b");
    }
}

#[test]
fn header_count_values_and_control_bytes_are_bounded() {
    for frame in [
        b"A: 1\r\nB: 2\r\nContent-Length: 0\r\n\r\n".as_slice(),
        b"A: 12345\r\nContent-Length: 0\r\n\r\n",
        b"A: a\0b\r\nContent-Length: 0\r\n\r\n",
    ] {
        let limits = ParserLimits {
            max_header_fields: 2,
            max_header_value_bytes: 4,
            ..Default::default()
        };
        assert!(MessageParser::with_limits(limits).feed(frame).is_err());
    }
    let mut parser = MessageParser::new();
    let frame = format_update(&Update::snapshot(Version::new("😀"), bytes::Bytes::new())).unwrap();
    for byte in frame {
        parser.feed(&[byte]).unwrap();
    }
    parser.finish().unwrap();
}
