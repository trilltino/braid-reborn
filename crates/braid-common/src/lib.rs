//! Shared, local storage layout. Set BRAID_ROOT to relocate all server data.
use std::path::PathBuf;

pub fn root_dir() -> PathBuf {
    std::env::var_os("BRAID_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("braid_data"))
}
pub fn sync_dir() -> PathBuf {
    root_dir()
}
pub fn peers_dir() -> PathBuf {
    root_dir().join("peers")
}
pub fn blobs_dir() -> PathBuf {
    root_dir().join("blobs")
}
pub fn braid_org_dir() -> PathBuf {
    sync_dir().join("braid.org")
}
pub fn ai_context_dir() -> PathBuf {
    root_dir().join("ai_context")
}
pub fn init_structure() -> std::io::Result<()> {
    for dir in [
        sync_dir(),
        peers_dir(),
        blobs_dir(),
        braid_org_dir(),
        ai_context_dir(),
        sync_dir().join("drafts"),
    ] {
        std::fs::create_dir_all(dir)?;
    }
    Ok(())
}
