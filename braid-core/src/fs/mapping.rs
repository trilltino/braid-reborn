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
    let host = url.host_str().ok_or_else(|| anyhow!("URL missing host"))?;
    let port = url.port();

    let mut domain_dir = host.to_string();
    if let Some(p) = port {
        // Use + instead of : for port separator on Windows (OS Error 123)
        domain_dir.push_str(&format!("+{}", p));
    }

    let root = get_root_dir()?;
    let mut path = root.join(domain_dir);

    // Trim leading slash from path segments
    for segment in url.path_segments().unwrap_or_else(|| "".split('/')) {
        path.push(segment);
    }

    // If path ends in slash or is empty, it might be a directory in URL semantics
    if url.path().ends_with('/') {
        path.push("index");
    }

    Ok(path)
}

pub fn path_to_url(path: &Path) -> Result<String> {
    let root = get_root_dir()?;

    // Canonicalize both paths to ensure matching prefix format (e.g. \\?\ prefix and casing)
    let root_abs = std::fs::canonicalize(&root).unwrap_or_else(|_| root.clone());
    let path_abs = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());

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

    url.path_segments_mut()
        .map_err(|_| anyhow!("Cannot be base"))?
        .extend(path_segments);

    Ok(url.to_string())
}

pub fn path_join(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{}", name)
    } else {
        format!("{}/{}", parent, name)
    }
}

pub fn extract_markdown(content: &str) -> String {
    let trimmed = content.trim();
    if trimmed.starts_with("<!DOCTYPE") || trimmed.starts_with("<html") {
        let mut candidates = Vec::new();
        let mut current_pos = 0;

        while let Some(start_idx) = trimmed[current_pos..].find("<script type=\"statebus\">") {
            let actual_start = current_pos + start_idx + "<script type=\"statebus\">".len();
            if let Some(end_idx) = trimmed[actual_start..].find("</script>") {
                let script_content = trimmed[actual_start..actual_start + end_idx].trim();
                candidates.push(script_content.to_string());
                current_pos = actual_start + end_idx + "</script>".len();
            } else {
                break;
            }
        }

        if !candidates.is_empty() {
            // Heuristic: Pick the script content that looks most like Markdown.
            // Sort by length first (descending)
            candidates.sort_by_key(|c| std::cmp::Reverse(c.len()));
            for candidate in &candidates {
                // If it has markdown-like features, it's likely the one we want
                if candidate.contains("# ")
                    || candidate.contains("\n- ")
                    || candidate.contains("](")
                    || candidate.contains("\n## ")
                {
                    return candidate.clone();
                }
            }
            // Fallback to the longest candidate if no clear markdown indicators found
            return candidates[0].clone();
        }
    }
    content.to_string()
}

pub fn wrap_markdown(original_content: &str, new_markdown: &str) -> String {
    let trimmed = original_content.trim();
    if trimmed.starts_with("<!DOCTYPE") || trimmed.starts_with("<html") {
        if let Some(start_idx) = trimmed.find("<script type=\"statebus\">") {
            let prefix = &trimmed[..start_idx + "<script type=\"statebus\">".len()];
            let after_script = &trimmed[start_idx..];
            if let Some(end_idx) = after_script.find("</script>") {
                let suffix = &after_script[end_idx..];
                return format!("{}\n{}\n{}", prefix, new_markdown, suffix);
            }
        }
    }
    new_markdown.to_string()
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
        // Logic check
    }

    #[test]
    fn test_path_join() {
        assert_eq!(path_join("/", "foo"), "/foo");
        assert_eq!(path_join("/bar", "baz"), "/bar/baz");
    }
}
