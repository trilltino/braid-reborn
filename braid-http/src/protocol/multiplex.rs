//! Multiplexing protocol constants and framing for Braid-HTTP.
//!
//! Implements the framing logic for Braid Multiplexing Protocol v1.0.

/// The version of the multiplexing protocol implemented.
pub const MULTIPLEX_VERSION: &str = "1.0";

/// Header used to specify the multiplexing version.
pub const HEADER_MULTIPLEX_VERSION: &str = "Multiplex-Version";

/// Header used to specify the multiplexing ID and request ID.
pub const HEADER_MULTIPLEX_THROUGH: &str = "Multiplex-Through";

/// Braid-specific status code for "Responded via multiplexer"
pub const STATUS_MULTIPLEX_REDIRECT: u16 = 293;

/// Events in the multiplexing stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiplexEvent {
    /// "start response <request_id>"
    StartResponse(String),
    /// "<N> bytes for response <request_id>"
    Data(String, Vec<u8>),
    /// "close response <request_id>"
    CloseResponse(String),
}

impl std::fmt::Display for MultiplexEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MultiplexEvent::StartResponse(id) => write!(f, "start response {}\r\n", id),
            MultiplexEvent::Data(id, data) => {
                write!(f, "{} bytes for response {}\r\n", data.len(), id)
            }
            MultiplexEvent::CloseResponse(id) => write!(f, "close response {}\r\n", id),
        }
    }
}

/// State machine for parsing the multiplexing protocol.
#[derive(Debug, Default, Clone)]
enum ParserState {
    #[default]
    Header,
    Data {
        id: String,
        remaining: usize,
    },
}

/// A parser for Braid Multiplexing Protocol streams.
pub struct MultiplexParser {
    buffer: Vec<u8>,
    state: ParserState,
}

impl Default for MultiplexParser {
    fn default() -> Self {
        Self::new()
    }
}

impl MultiplexParser {
    /// Creates a new MultiplexParser.
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            state: ParserState::Header,
        }
    }

    /// Feeds data into the parser and returns any complete events found.
    pub fn feed(&mut self, data: &[u8]) -> Result<Vec<MultiplexEvent>, String> {
        self.buffer.extend_from_slice(data);
        let mut events = Vec::new();

        loop {
            match self.state.clone() {
                ParserState::Header => {
                    // Look for \r\n
                    let mut found_newline = false;
                    let mut line_end = 0;

                    for i in 0..self.buffer.len() {
                        if self.buffer[i] == b'\n' {
                            line_end = i + 1;
                            found_newline = true;
                            break;
                        }
                    }

                    if !found_newline {
                        break;
                    }

                    let line_bytes = &self.buffer[..line_end];
                    let line = String::from_utf8_lossy(line_bytes);

                    if let Some(suffix) = line.strip_prefix("start response ") {
                        let id = suffix.trim().to_string();
                        events.push(MultiplexEvent::StartResponse(id));
                        self.consume(line_end);
                    } else if line.contains(" bytes for response ") {
                        let parts: Vec<&str> = line.splitn(2, " bytes for response ").collect();
                        if parts.len() == 2 {
                            let size_str = parts[0].trim_start_matches(['\r', '\n']).trim();
                            if let Ok(size) = size_str.parse::<usize>() {
                                let id = parts[1].trim().to_string();
                                self.state = ParserState::Data {
                                    id,
                                    remaining: size,
                                };
                                self.consume(line_end);
                            } else {
                                return Err(format!("Invalid size in multiplex header: {}", line));
                            }
                        } else {
                            return Err(format!("Invalid multiplex header: {}", line));
                        }
                    } else if let Some(suffix) = line.strip_prefix("close response ") {
                        let id = suffix.trim().to_string();
                        events.push(MultiplexEvent::CloseResponse(id));
                        self.consume(line_end);
                    } else if line.trim().is_empty() {
                        self.consume(line_end);
                    } else {
                        return Err(format!("Unknown multiplex header: {}", line));
                    }
                }
                ParserState::Data { id, remaining } => {
                    if self.buffer.len() >= remaining {
                        let data = self.buffer[..remaining].to_vec();
                        events.push(MultiplexEvent::Data(id, data));
                        self.consume(remaining);
                        self.state = ParserState::Header;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(events)
    }

    fn consume(&mut self, n: usize) {
        self.buffer.drain(0..n);
    }
}
