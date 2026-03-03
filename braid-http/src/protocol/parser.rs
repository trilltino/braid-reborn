//! Protocol parsing utilities.

use crate::error::Result;
use crate::types::Version;

/// Header parser for protocol messages.
pub struct HeaderParser;

impl HeaderParser {
    pub fn parse_version(value: &str) -> Result<Vec<Version>> {
        crate::protocol::parse_version_header(value)
    }
    pub fn parse_content_range(value: &str) -> Result<(String, String)> {
        crate::protocol::parse_content_range(value)
    }
    pub fn format_version(versions: &[Version]) -> String {
        crate::protocol::format_version_header(versions)
    }
    pub fn format_content_range(unit: &str, range: &str) -> String {
        crate::protocol::format_content_range(unit, range)
    }
}
