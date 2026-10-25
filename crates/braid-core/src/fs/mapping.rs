use crate::fs::config::get_root_dir;
use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use url::Url;

pub fn url_to_path(url_str: &str) -> Result<PathBuf> {
    // Normalize URL string: handle Windows-style backslashes and missing protocols
    let mut normalized = url_str.replace('\\', "/");
    if !normalized.contains("://") {
        if normalized.starts_with("braid.org") || normalized.starts_with("braidfs") {
            normalized = format!("https://{}", normalized);
        } else if normalized.starts_with("localhost") || normalized.starts_with("127.0.0.1") {
            normalized = format!("http://{}", normalized);
        }
    }

    let url = Url::parse(&normalized)?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(anyhow!("URL cannot be represented by a unique sync file"));
    }
    let host = url.host_str().ok_or_else(|| anyhow!("URL missing host"))?;
    let port = url.port();

    let mut domain_dir = host.replace(':', "_").replace(['[', ']'], "");
    if let Some(p) = port {
        // Use + instead of : for port separator on Windows (OS Error 123)
        domain_dir.push_str(&format!("+{}", p));
    }

    let root = get_root_dir()?;
    let mut path = root.join(domain_dir);

    // Trim leading slash from path segments
    for segment in url.path_segments().unwrap_or_else(|| "".split('/')) {
        if segment.is_empty() {
            continue;
        }
        let decoded = percent_encoding::percent_decode_str(segment).decode_utf8()?;
        if decoded == "."
            || decoded == ".."
            || decoded.contains(['/', '\\', ':', '\0'])
            || decoded.ends_with(['.', ' '])
        {
            return Err(anyhow!("Unsafe URL path segment"));
        }
        path.push(segment);
    }

    // If path ends in slash or is empty, it might be a directory in URL semantics
    if url.path().ends_with('/') {
        path.push("index");
    }

    let mut ancestor = path.as_path();
    loop {
        if std::fs::symlink_metadata(ancestor)
            .is_ok_and(|m| braid_http::durable::is_link_or_reparse_point(&m))
        {
            return Err(anyhow!("Sync paths cannot traverse symlinks"));
        }
        match ancestor.parent() {
            Some(parent) => ancestor = parent,
            None => break,
        }
    }
    Ok(path)
}

pub fn path_to_url(path: &Path) -> Result<String> {
    let root = get_root_dir()?;
    let root_abs = path_key(&root)?;
    let path_abs = path_key(path)?;

    let relative = path_abs.strip_prefix(&root_abs).map_err(|_| {
        anyhow!(
            "Path {:?} is not within BraidFS root {:?}",
            path_abs,
            root_abs
        )
    })?;

    let mut components = relative.components();

    // First component is domain[:port]
    let domain_comp = components.next().ok_or_else(|| anyhow!("Path too short"))?;

    let domain_str = domain_comp.as_os_str().to_string_lossy();
    if domain_str.starts_with('.') {
        return Err(anyhow!("Ignoring dotfile/directory"));
    }

    let (host, port) = if let Some((h, p)) = domain_str.rsplit_once('+') {
        (h, Some(p.parse::<u16>()?))
    } else {
        (domain_str.as_ref(), None)
    };

    // Construct URL
    let scheme = if host == "localhost" || host == "127.0.0.1" {
        "http"
    } else {
        "https"
    };

    let mut url = Url::parse(&format!("{}://{}", scheme, host))?;
    if let Some(p) = port {
        url.set_port(Some(p)).map_err(|_| anyhow!("Invalid port"))?;
    }

    let mut path_segments = Vec::new();
    for comp in components {
        path_segments.push(comp.as_os_str().to_string_lossy().to_string());
    }

    if let Some(last) = path_segments.last() {
        if last == "index" {
            path_segments.pop();
        }
    }

    // Existing sync files use encoded URL segments as filenames. Passing these
    // through path_segments_mut would encode '%' again and name another resource.
    url.set_path(&format!("/{}", path_segments.join("/")));
    if path_key(&url_to_path(url.as_str())?)? != path_abs {
        return Err(anyhow!(
            "Filename cannot round-trip through the existing URL mapping"
        ));
    }

    Ok(url.to_string())
}

/// Resolve the existing prefix even after unlink. This also keeps Windows
/// extended path prefixes/casing consistent without hiding permission errors.
pub(crate) fn path_key(path: &Path) -> std::io::Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    loop {
        match std::fs::canonicalize(existing) {
            Ok(mut path) => {
                for component in missing.into_iter().rev() {
                    path.push(component);
                }
                return Ok(path);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(existing.file_name().ok_or(error)?.to_os_string());
                existing = existing
                    .parent()
                    .ok_or_else(|| std::io::Error::other("Path has no existing prefix"))?;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(crate) async fn configured_url(
    path: &Path,
    state: &crate::fs::state::DaemonState,
) -> crate::core::Result<Option<String>> {
    let key = path_key(path)?;
    let config = state.config.read().await;
    let mut found = None;
    for (url, enabled) in &config.sync {
        if *enabled && path_key(&url_to_path(url)?)? == key {
            if found.is_some() {
                return Err(crate::core::BraidError::Config(
                    "Multiple enabled URLs map to the same local path".into(),
                ));
            }
            found = Some(url.clone());
        }
    }
    Ok(found)
}

pub fn path_join(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{}", name)
    } else {
        format!("{}/{}", parent, name)
    }
}

/// Select the same script for reads and writes, preserving its exact byte view.
fn markdown_span(content: &str) -> Option<std::ops::Range<usize>> {
    let trimmed = content.trim_start();
    if !trimmed.starts_with("<!DOCTYPE") && !trimmed.starts_with("<html") {
        return None;
    }
    let marker = "<script type=\"statebus\">";
    let mut start = 0;
    let mut spans = Vec::new();
    while let Some(offset) = content[start..].find(marker) {
        let body = start + offset + marker.len();
        let Some(end) = content[body..].find("</script>") else {
            break;
        };
        let span = body..body + end;
        let text = &content[span.clone()];
        let markdown = text.contains("# ") || text.contains("\n- ") || text.contains("](");
        spans.push((markdown, span.len(), span));
        start = body + end + "</script>".len();
    }
    spans
        .into_iter()
        .max_by_key(|(markdown, len, _)| (*markdown, *len))
        .map(|(_, _, span)| span)
}
pub fn extract_markdown(content: &str) -> String {
    markdown_span(content).map_or_else(|| content.to_owned(), |span| content[span].to_owned())
}
pub fn wrap_markdown(original_content: &str, new_markdown: &str) -> String {
    if let Some(span) = markdown_span(original_content) {
        format!(
            "{}{}{}",
            &original_content[..span.start],
            new_markdown,
            &original_content[span.end..]
        )
    } else {
        new_markdown.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_markdown() {
        let html = "<html><script type=\"statebus\">hello world</script></html>";
        assert_eq!(extract_markdown(html), "hello world");
    }

    #[test]
    fn test_wrap_markdown() {
        let html = "<html><script type=\"statebus\">hello world</script></html>";
        let wrapped = wrap_markdown(html, "new content");
        assert!(wrapped.contains("<script type=\"statebus\">"));
        assert!(wrapped.contains("new content"));
        assert!(wrapped.contains("</script>"));
    }

    #[test]
    fn test_url_to_path() {
        for url in [
            "https://example.org/a%20b",
            "https://example.org/%F0%9F%98%80",
            "https://example.org/a%23b",
        ] {
            let path = url_to_path(url).unwrap();
            assert_eq!(path_to_url(&path).unwrap(), url);
        }
        let root = tempfile::tempdir().unwrap();
        let removed = root.path().join("absent/file");
        assert_eq!(
            path_key(&removed).unwrap(),
            root.path().canonicalize().unwrap().join("absent/file")
        );
    }

    #[test]
    fn test_path_join() {
        assert_eq!(path_join("/", "foo"), "/foo");
        assert_eq!(path_join("/bar", "baz"), "/bar/baz");
    }
}
