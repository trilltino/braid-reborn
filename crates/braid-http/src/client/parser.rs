//! Bounded incremental decoding of Braid frames. The transport decodes HTTP transfer coding.
use crate::error::{BraidError, Result};
use crate::types::Patch;
use bytes::{Buf, Bytes, BytesMut};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseState {
    WaitingForHeaders,
    ParsingHeaders,
    WaitingForBody,
    WaitingForPatchHeaders,
    WaitingForPatchBody,
    SkippingSeparator,
    Complete,
    Error,
}
#[derive(Debug, Clone, Copy)]
pub struct ParserLimits {
    pub max_header_bytes: usize,
    pub max_body_bytes: usize,
    pub max_patches: usize,
    pub max_update_bytes: usize,
}
impl Default for ParserLimits {
    fn default() -> Self {
        Self {
            max_header_bytes: 64 * 1024,
            max_body_bytes: 16 * 1024 * 1024,
            max_patches: 1024,
            max_update_bytes: 32 * 1024 * 1024,
        }
    }
}
#[derive(Debug)]
pub struct MessageParser {
    buffer: BytesMut,
    state: ParseState,
    headers: BTreeMap<String, String>,
    body_buffer: BytesMut,
    patches: Vec<Patch>,
    patch_headers: BTreeMap<String, String>,
    expected_length: usize,
    expected_patches: usize,
    update_bytes: usize,
    limits: ParserLimits,
}
impl Default for MessageParser {
    fn default() -> Self {
        Self::new()
    }
}
impl MessageParser {
    pub fn new() -> Self {
        Self::with_limits(ParserLimits::default())
    }
    pub fn with_limits(limits: ParserLimits) -> Self {
        Self {
            buffer: BytesMut::new(),
            state: ParseState::WaitingForHeaders,
            headers: BTreeMap::new(),
            body_buffer: BytesMut::new(),
            patches: vec![],
            patch_headers: BTreeMap::new(),
            expected_length: 0,
            expected_patches: 0,
            update_bytes: 0,
            limits,
        }
    }
    /// Decode a finite HTTP entity with already parsed outer headers.
    /// Subscription bodies use `new`, independent of HTTP transfer coding.
    pub fn new_with_state(headers: BTreeMap<String, String>, content_length: usize) -> Self {
        let mut parser = Self::new();
        parser.headers = headers;
        parser
            .headers
            .entry("content-length".into())
            .or_insert_with(|| content_length.to_string());
        if parser.start_body().is_err() {
            parser.state = ParseState::Error;
        }
        parser
    }
    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<Message>> {
        if self.state == ParseState::Error {
            return Err(BraidError::Protocol("Parser is in a failed state".into()));
        }
        let mut messages = vec![];
        for chunk in data.chunks(8192) {
            self.buffer.extend_from_slice(chunk);
            if let Err(e) = self.drain(&mut messages) {
                self.state = ParseState::Error;
                return Err(e);
            }
        }
        if let Err(e) = self.drain(&mut messages) {
            self.state = ParseState::Error;
            return Err(e);
        }
        Ok(messages)
    }
    fn drain(&mut self, messages: &mut Vec<Message>) -> Result<()> {
        loop {
            match self.state {
                ParseState::WaitingForHeaders | ParseState::WaitingForPatchHeaders => {
                    while matches!(self.buffer.first(), Some(b'\r' | b'\n')) {
                        self.buffer.advance(1);
                    }
                    let end = self
                        .buffer
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                        .map(|p| p + 4)
                        .or_else(|| {
                            self.buffer
                                .windows(2)
                                .position(|w| w == b"\n\n")
                                .map(|p| p + 2)
                        });
                    if end.unwrap_or(self.buffer.len()) > self.limits.max_header_bytes {
                        return Err(BraidError::Protocol("Header limit exceeded".into()));
                    }
                    let Some(end) = end else {
                        break;
                    };
                    self.account(end)?;
                    let headers = Self::parse_headers(&self.buffer.split_to(end))?;
                    if self.state == ParseState::WaitingForHeaders {
                        self.headers = headers;
                        self.start_body()?;
                    } else {
                        if !headers.contains_key("content-range") {
                            return Err(BraidError::Protocol(
                                "Patch requires Content-Range".into(),
                            ));
                        }
                        self.expected_length = Self::decimal(&headers, "content-length")?;
                        self.check_length(self.expected_length)?;
                        crate::protocol::parse_content_range(&headers["content-range"])?;
                        self.patch_headers = headers;
                        self.state = ParseState::WaitingForPatchBody;
                    }
                }
                ParseState::WaitingForBody | ParseState::WaitingForPatchBody => {
                    let needed = self.expected_length - self.body_buffer.len();
                    let n = needed.min(self.buffer.len());
                    self.body_buffer.extend_from_slice(&self.buffer.split_to(n));
                    if n < needed {
                        break;
                    }
                    if self.state == ParseState::WaitingForPatchBody {
                        let (unit, range) = crate::protocol::parse_content_range(
                            &self.patch_headers["content-range"],
                        )?;
                        self.patches.push(Patch::new(
                            unit,
                            range,
                            self.body_buffer.split().freeze(),
                        ));
                        if self.patches.len() < self.expected_patches {
                            self.state = ParseState::WaitingForPatchHeaders;
                            continue;
                        }
                    }
                    messages.push(Message {
                        status_code: self.headers.get(":status").and_then(|s| s.parse().ok()),
                        encoding: self.headers.get("encoding").cloned(),
                        url: self.headers.get("content-location").cloned(),
                        headers: std::mem::take(&mut self.headers),
                        body: self.body_buffer.split().freeze(),
                        patches: std::mem::take(&mut self.patches),
                    });
                    self.expected_patches = 0;
                    self.update_bytes = 0;
                    self.state = ParseState::WaitingForHeaders;
                }
                _ => break,
            }
        }
        Ok(())
    }
    fn parse_headers(data: &[u8]) -> Result<BTreeMap<String, String>> {
        let text = std::str::from_utf8(data)
            .map_err(|_| BraidError::Protocol("Invalid header UTF-8".into()))?;
        let mut headers = BTreeMap::new();
        for (index, line) in text.lines().enumerate() {
            if line.is_empty() {
                continue;
            }
            if index == 0 {
                if let Some(status) = parse_status_line(line) {
                    headers.insert(":status".into(), status.to_string());
                    continue;
                }
            }
            let (key, value) = line
                .split_once(':')
                .ok_or_else(|| BraidError::Protocol("Malformed header".into()))?;
            if key.is_empty()
                || !key
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
            {
                return Err(BraidError::Protocol("Invalid header name".into()));
            }
            let key = key.to_ascii_lowercase();
            if let Some(existing) = headers.get_mut(&key) {
                if matches!(key.as_str(), "version" | "parents" | "current-version") {
                    existing.push_str(", ");
                    existing.push_str(value.trim());
                } else {
                    return Err(BraidError::Protocol(format!("Duplicate header: {key}")));
                }
            } else {
                headers.insert(key, value.trim().into());
            }
        }
        Ok(headers)
    }
    fn decimal(headers: &BTreeMap<String, String>, name: &str) -> Result<usize> {
        let s = headers
            .get(name)
            .ok_or_else(|| BraidError::Protocol(format!("Missing {name}")))?;
        if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
            return Err(BraidError::Protocol(format!("Invalid {name}")));
        }
        s.parse()
            .map_err(|_| BraidError::Protocol(format!("Overflow in {name}")))
    }
    fn account(&mut self, n: usize) -> Result<()> {
        self.update_bytes = self
            .update_bytes
            .checked_add(n)
            .filter(|n| *n <= self.limits.max_update_bytes)
            .ok_or_else(|| BraidError::Protocol("Update limit exceeded".into()))?;
        Ok(())
    }
    fn check_length(&mut self, n: usize) -> Result<()> {
        if n > self.limits.max_body_bytes {
            return Err(BraidError::Protocol("Body limit exceeded".into()));
        }
        self.account(n)
    }
    fn start_body(&mut self) -> Result<()> {
        if self.headers.contains_key("patches") {
            if self.headers.contains_key("content-length")
                || self.headers.contains_key("content-range")
            {
                return Err(BraidError::Protocol(
                    "Conflicting patch and snapshot framing".into(),
                ));
            }
            self.expected_patches = Self::decimal(&self.headers, "patches")?;
            if self.expected_patches > self.limits.max_patches {
                return Err(BraidError::Protocol("Patch count limit exceeded".into()));
            }
            self.expected_length = 0;
            self.state = if self.expected_patches == 0 {
                ParseState::WaitingForBody
            } else {
                ParseState::WaitingForPatchHeaders
            };
        } else {
            self.expected_length = if self.headers.contains_key("encoding") {
                Self::decimal(&self.headers, "length")?
            } else {
                Self::decimal(&self.headers, "content-length")?
            };
            self.check_length(self.expected_length)?;
            self.state = ParseState::WaitingForBody;
        }
        Ok(())
    }
    /// Reject EOF in the middle of a frame.
    pub fn finish(&mut self) -> Result<()> {
        if self.state == ParseState::WaitingForHeaders
            && self.buffer.iter().all(|b| *b == b'\r' || *b == b'\n')
        {
            self.buffer.clear();
            Ok(())
        } else {
            self.state = ParseState::Error;
            Err(BraidError::Protocol("Truncated Braid frame at EOF".into()))
        }
    }
    pub fn state(&self) -> ParseState {
        self.state
    }
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }
    pub fn body(&self) -> &[u8] {
        &self.body_buffer
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub headers: BTreeMap<String, String>,
    pub body: Bytes,
    pub patches: Vec<Patch>,
    pub status_code: Option<u16>,
    pub encoding: Option<String>,
    pub url: Option<String>,
}

impl Message {
    pub fn status(&self) -> Option<u16> {
        self.status_code
            .or_else(|| self.headers.get(":status").and_then(|v| v.parse().ok()))
    }

    pub fn version(&self) -> Option<&str> {
        self.headers.get("version").map(|s| s.as_str())
    }
    pub fn current_version(&self) -> Option<&str> {
        self.headers.get("current-version").map(|s| s.as_str())
    }
    pub fn parents(&self) -> Option<&str> {
        self.headers.get("parents").map(|s| s.as_str())
    }

    pub fn decode_body(&self) -> Result<Bytes> {
        match self.encoding.as_deref() {
            Some("dt") => Ok(self.body.clone()),
            Some(enc) => Err(BraidError::Protocol(format!("Unknown encoding: {}", enc))),
            None => Ok(self.body.clone()),
        }
    }

    pub fn extra_headers(&self) -> BTreeMap<String, String> {
        const KNOWN_HEADERS: &[&str] = &[
            "version",
            "parents",
            "current-version",
            "patches",
            "content-length",
            "content-range",
            ":status",
        ];
        self.headers
            .iter()
            .filter(|(k, _)| !KNOWN_HEADERS.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn body_text(&self) -> Option<String> {
        std::str::from_utf8(&self.body).ok().map(|s| s.to_string())
    }
}

pub fn parse_status_line(line: &str) -> Option<u16> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.len() >= 2 && parts[0].to_uppercase().starts_with("HTTP") {
        parts[1].parse().ok()
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_creation() {
        let parser = MessageParser::new();
        assert_eq!(parser.state(), ParseState::WaitingForHeaders);
    }

    #[test]
    fn test_simple_message_parsing() {
        let mut parser = MessageParser::new();
        let data = b"Content-Length: 5\r\n\r\nHello";
        let messages = parser.feed(data).unwrap();
        assert!(!messages.is_empty());
        assert_eq!(messages[0].body, Bytes::from_static(b"Hello"));
    }

    #[test]
    fn test_parse_status_line() {
        assert_eq!(parse_status_line("HTTP/1.1 200 OK"), Some(200));
        assert_eq!(parse_status_line("HTTP 209 Subscription"), Some(209));
        assert_eq!(parse_status_line("HTTP/2 404"), Some(404));
    }

    #[test]
    fn test_message_extra_headers() {
        let mut headers = BTreeMap::new();
        headers.insert("version".to_string(), "\"v1\"".to_string());
        headers.insert("x-custom-header".to_string(), "value".to_string());

        let msg = Message {
            headers,
            body: Bytes::new(),
            patches: vec![],
            status_code: None,
            encoding: None,
            url: None,
        };

        let extra = msg.extra_headers();
        assert_eq!(extra.len(), 1);
        assert!(extra.contains_key("x-custom-header"));
        assert!(!extra.contains_key("version"));
    }

    #[test]
    fn test_multi_patch_parsing() {
        let mut parser = MessageParser::new();
        let data = b"Patches: 2\r\n\r\n\
                     Content-Length: 5\r\n\
                     Content-Range: json .a\r\n\r\n\
                     hello\r\n\
                     Content-Length: 5\r\n\
                     Content-Range: json .b\r\n\r\n\
                     world\r\n";

        let messages = parser.feed(data).unwrap();
        assert!(!messages.is_empty());
        let msg = &messages[0];
        assert_eq!(msg.patches.len(), 2);
    }
}
