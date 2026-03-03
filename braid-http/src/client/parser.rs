//! Message parser for Braid protocol streaming.

use crate::error::{BraidError, Result};
use crate::types::Patch;
use bytes::{Buf, Bytes, BytesMut};
use once_cell::sync::Lazy;
use regex::Regex;
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

#[derive(Debug)]
pub struct MessageParser {
    buffer: BytesMut,
    state: ParseState,
    headers: BTreeMap<String, String>,
    body_buffer: BytesMut,
    expected_body_length: usize,
    read_body_length: usize,
    patches: Vec<Patch>,
    expected_patches: usize,
    patches_read: usize,
    patch_headers: BTreeMap<String, String>,
    expected_patch_length: usize,
    read_patch_length: usize,
    is_encoding_block: bool,
    /// For chunked transfer encoding, we don't know the body length upfront
    /// and should read until connection closes or we detect end-of-stream
    is_chunked: bool,
}

static HTTP_STATUS_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^HTTP/?\d*\.?\d* (\d{3})").unwrap());

static ENCODING_BLOCK_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)Encoding:\s*(\w+)\r?\nLength:\s*(\d+)\r?\n").unwrap());

impl MessageParser {
    pub fn new() -> Self {
        MessageParser {
            buffer: BytesMut::with_capacity(8192),
            state: ParseState::WaitingForHeaders,
            headers: BTreeMap::new(),
            body_buffer: BytesMut::new(),
            expected_body_length: 0,
            read_body_length: 0,
            patches: Vec::new(),
            expected_patches: 0,
            patches_read: 0,
            patch_headers: BTreeMap::new(),
            expected_patch_length: 0,
            read_patch_length: 0,
            is_encoding_block: false,
            is_chunked: false,
        }
    }

    pub fn new_with_state(headers: BTreeMap<String, String>, content_length: usize) -> Self {
        let mut parser = MessageParser::new();
        parser.headers = headers.clone();
        parser.expected_body_length = content_length;
        
        // Check for chunked transfer encoding
        let is_chunked = headers
            .get("transfer-encoding")
            .map(|v| v.to_lowercase().contains("chunked"))
            .unwrap_or(false);
        parser.is_chunked = is_chunked;
        
        // For chunked encoding in subscriptions, the body contains braid protocol messages.
        // Each message starts with headers like "Version:", "Content-Length:" etc.
        // So we need to parse headers from the stream, not treat it as raw body.
        // Start in WaitingForHeaders state to parse the first braid message headers.
        if is_chunked && content_length == 0 {
            parser.state = ParseState::WaitingForHeaders;
        } else {
            parser.state = ParseState::WaitingForBody;
        }
        
        parser
    }

    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<Message>> {
        tracing::debug!("[Parser] feed() called with {} bytes, state: {:?}", data.len(), self.state);
        self.buffer.extend_from_slice(data);
        let mut messages = Vec::new();

        loop {
            match self.state {
                ParseState::WaitingForHeaders => {
                    tracing::debug!("[Parser] WaitingForHeaders, buffer len: {}", self.buffer.len());
                    while !self.buffer.is_empty()
                        && (self.buffer[0] == b'\r' || self.buffer[0] == b'\n')
                    {
                        self.buffer.advance(1);
                    }

                    if self.buffer.is_empty() {
                        tracing::debug!("[Parser] Buffer empty after trimming, breaking");
                        break;
                    }

                    if self.check_encoding_block()? {
                        tracing::debug!("[Parser] Found encoding block");
                        self.state = ParseState::WaitingForBody;
                        continue;
                    }

                    if let Some(pos) = self.find_header_end() {
                        tracing::debug!("[Parser] Found header end at pos {}", pos);
                        self.parse_headers(pos)?;
                        tracing::debug!("[Parser] Headers parsed, content-length: {}, patches: {}", 
                            self.expected_body_length, self.expected_patches);
                        self.state = ParseState::WaitingForBody;
                    } else {
                        tracing::debug!("[Parser] Header end not found, waiting for more data");
                        break;
                    }
                }
                ParseState::WaitingForBody => {
                    tracing::debug!("[Parser] WaitingForBody, expected_body_length: {}, buffer len: {}", 
                        self.expected_body_length, self.buffer.len());
                    if self.expected_patches > 0 {
                        tracing::debug!("[Parser] Have {} patches to parse", self.expected_patches);
                        self.state = ParseState::WaitingForPatchHeaders;
                    } else if self.try_parse_body()? {
                        tracing::debug!("[Parser] Body parsed successfully, finalizing message");
                        if let Some(msg) = self.finalize_message()? {
                            tracing::debug!("[Parser] Message finalized with {} bytes body", msg.body.len());
                            messages.push(msg);
                        }
                        self.reset();
                        self.state = ParseState::WaitingForHeaders;
                    } else {
                        tracing::debug!("[Parser] Body incomplete, waiting for more data");
                        break;
                    }
                }
                ParseState::WaitingForPatchHeaders => {
                    if let Some(pos) = self.find_header_end() {
                        self.parse_patch_headers(pos)?;
                        self.state = ParseState::WaitingForPatchBody;
                    } else {
                        break;
                    }
                }
                ParseState::WaitingForPatchBody => {
                    if self.try_parse_patch_body()? {
                        self.patches_read += 1;
                        if self.patches_read < self.expected_patches {
                            self.state = ParseState::SkippingSeparator;
                        } else {
                            if let Some(msg) = self.finalize_message()? {
                                messages.push(msg);
                            }
                            self.reset();
                            self.state = ParseState::WaitingForHeaders;
                        }
                    } else {
                        break;
                    }
                }
                ParseState::SkippingSeparator => {
                    if self.buffer.len() >= 2 {
                        if &self.buffer[..2] == b"\r\n" {
                            self.buffer.advance(2);
                        } else if self.buffer[0] == b'\n' {
                            self.buffer.advance(1);
                        }
                        self.state = ParseState::WaitingForPatchHeaders;
                    } else if self.buffer.len() == 1 && self.buffer[0] == b'\n' {
                        self.buffer.advance(1);
                        self.state = ParseState::WaitingForPatchHeaders;
                    } else {
                        break;
                    }
                }
                _ => break,
            }
        }
        Ok(messages)
    }

    fn check_encoding_block(&mut self) -> Result<bool> {
        if self.buffer.is_empty() || (self.buffer[0] != b'E' && self.buffer[0] != b'e') {
            return Ok(false);
        }

        if let Some(end) = self.find_double_newline() {
            let header_bytes = &self.buffer[..end];
            let header_str = std::str::from_utf8(header_bytes).map_err(|e| {
                BraidError::Protocol(format!("Invalid encoding block UTF-8: {}", e))
            })?;

            if let Some(caps) = ENCODING_BLOCK_REGEX.captures(header_str) {
                let encoding = caps.get(1).unwrap().as_str().to_string();
                let length: usize = caps.get(2).unwrap().as_str().parse().map_err(|_| {
                    BraidError::Protocol("Invalid length in encoding block".to_string())
                })?;

                let _ = self.buffer.split_to(end);
                self.headers.insert("encoding".to_string(), encoding);
                self.headers
                    .insert("length".to_string(), length.to_string());
                self.expected_body_length = length;
                self.is_encoding_block = true;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn find_double_newline(&self) -> Option<usize> {
        if let Some(pos) = self.buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            return Some(pos + 4);
        }
        if let Some(pos) = self.buffer.windows(2).position(|w| w == b"\n\n") {
            return Some(pos + 2);
        }
        None
    }

    fn find_header_end(&self) -> Option<usize> {
        self.buffer
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .map(|p| p + 4)
    }

    fn parse_headers(&mut self, end: usize) -> Result<()> {
        let header_bytes = self.buffer.split_to(end);
        let mut header_str = String::from_utf8(header_bytes[..header_bytes.len() - 4].to_vec())?;

        if let Some(caps) = HTTP_STATUS_REGEX.captures(&header_str) {
            if let Some(status_match) = caps.get(1) {
                let status = status_match.as_str();
                if let Some(first_newline) = header_str.find('\n') {
                    let replacement = format!(":status: {}\r", status);
                    header_str = replacement + &header_str[first_newline..];
                }
            }
        }

        for line in header_str.lines() {
            if let Some(colon_pos) = line.find(':') {
                let key = line[..colon_pos].trim().to_lowercase();
                let value = line[colon_pos + 1..].trim().to_string();
                self.headers.insert(key, value);
            }
        }

        if let Some(patches_str) = self.headers.get("patches") {
            self.expected_patches = patches_str.parse().unwrap_or(0);
        }

        if let Some(len_str) = self
            .headers
            .get("content-length")
            .or_else(|| self.headers.get("length"))
        {
            self.expected_body_length = len_str.parse().map_err(|_| {
                BraidError::HeaderParse(format!("Invalid content-length: {}", len_str))
            })?;
        }
        Ok(())
    }

    fn parse_patch_headers(&mut self, end: usize) -> Result<()> {
        let header_bytes = self.buffer.split_to(end);
        let header_str = String::from_utf8(header_bytes[..header_bytes.len() - 4].to_vec())?;

        self.patch_headers.clear();
        for line in header_str.lines() {
            if let Some(colon_pos) = line.find(':') {
                let key = line[..colon_pos].trim().to_lowercase();
                let value = line[colon_pos + 1..].trim().to_string();
                self.patch_headers.insert(key, value);
            }
        }

        if let Some(len_str) = self.patch_headers.get("content-length") {
            self.expected_patch_length = len_str.parse().map_err(|_| {
                BraidError::HeaderParse(format!("Invalid patch content-length: {}", len_str))
            })?;
        } else {
            return Err(BraidError::Protocol(
                "Every patch MUST include Content-Length".to_string(),
            ));
        }

        self.read_patch_length = 0;
        Ok(())
    }

    fn try_parse_patch_body(&mut self) -> Result<bool> {
        let remaining = self.expected_patch_length - self.read_patch_length;
        if self.buffer.len() >= remaining {
            let body_chunk = self.buffer.split_to(remaining);
            let unit = self
                .patch_headers
                .get("content-range")
                .and_then(|cr| cr.split_whitespace().next())
                .unwrap_or("bytes")
                .to_string();
            let range = self
                .patch_headers
                .get("content-range")
                .and_then(|cr| cr.split_whitespace().nth(1))
                .unwrap_or("")
                .to_string();
            let patch = Patch::with_length(unit, range, body_chunk, self.expected_patch_length);
            self.patches.push(patch);
            self.read_patch_length += remaining;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn try_parse_body(&mut self) -> Result<bool> {
        // For non-chunked with known content-length
        if self.expected_body_length == 0 {
            // No body expected (e.g., HEAD request or empty response with explicit 0 length)
            return Ok(true);
        }
        
        let remaining = self.expected_body_length - self.read_body_length;
        if self.buffer.len() >= remaining {
            let body_chunk = self.buffer.split_to(remaining);
            self.body_buffer.extend_from_slice(&body_chunk);
            self.read_body_length += body_chunk.len();
            Ok(true)
        } else {
            let chunk_len = self.buffer.len();
            self.body_buffer
                .extend_from_slice(&self.buffer.split_to(chunk_len));
            self.read_body_length += chunk_len;
            Ok(false)
        }
    }

    fn finalize_message(&mut self) -> Result<Option<Message>> {
        let body = self.body_buffer.split().freeze();
        let headers = std::mem::take(&mut self.headers);
        let url = headers.get("content-location").cloned();
        let encoding = headers.get("encoding").cloned();

        Ok(Some(Message {
            headers,
            body,
            patches: std::mem::take(&mut self.patches),
            status_code: None,
            encoding,
            url,
        }))
    }

    fn reset(&mut self) {
        self.headers.clear();
        self.body_buffer.clear();
        self.expected_body_length = 0;
        self.read_body_length = 0;
        self.patches.clear();
        self.expected_patches = 0;
        self.patches_read = 0;
        self.patch_headers.clear();
        self.expected_patch_length = 0;
        self.read_patch_length = 0;
        self.is_encoding_block = false;
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

impl Default for MessageParser {
    fn default() -> Self {
        Self::new()
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
