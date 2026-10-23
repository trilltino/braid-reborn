use crate::fs::mapping;
use crate::fs::state::{Command, DaemonState};
use notify::Event;

pub async fn handle_fs_event(event: Event, state: DaemonState) {
    if event.kind.is_access() || event.kind.is_other() {
        return;
    }
    tracing::debug!("[BraidFS-WATCHER] Event: {:?}", event);

    for path in event.paths {
        // We handle both modifications and removals
        // We handle both modifications and removals
        let is_removal = event.kind.is_remove() || !path.exists();
        let is_create = event.kind.is_create();

        // Skip non-files if it's NOT a removal (e.g. it's a new directory)
        if !is_removal && !path.is_file() {
            tracing::trace!("[BraidFS] Skipping non-file: {:?}", path);
            continue;
        }

        // Skip if this is a dotfile or inside a hidden directory (like .braidfs) or a .tmp file
        if path.components().any(|c| {
            let s = c.as_os_str().to_string_lossy();
            s.starts_with('.') || s.ends_with(".tmp") || s.ends_with(".sqlite") || s.ends_with("-journal") || s.ends_with(".db")
        }) {
            continue;
        }

        // Skip if this was a pending write from us (to avoid echo loops)
        if state.pending.should_ignore(&path) {
            tracing::trace!("[BraidFS] Skipping pending write: {:?}", path);
            continue;
        }

        match mapping::path_to_url(&path) {
            Ok(url) => {
                tracing::info!("[BraidFS] File changed: {:?} -> {}", path, url);

                // Auto-add new files to config.sync (IDE sync feature)
                if is_create {
                    let is_synced = {
                        let cfg = state.config.read().await;
                        cfg.sync.get(&url).copied().unwrap_or(false)
                    };

                    if !is_synced {
                        tracing::info!("[BraidFS] Auto-adding new file to sync: {}", url);
                        // Add to config.sync
                        {
                            let mut cfg = state.config.write().await;
                            cfg.sync.insert(url.clone(), true);
                            let _ = cfg.save().await;
                        }
                        // Send sync command to spawn subscription
                        let _ = state.tx_cmd.send(Command::Sync { url: url.clone() }).await;
                    }
                }

                // 3. Delegate to Debouncer for the actual sync
                state.debouncer.request_sync(url, path).await;
            }
            Err(e) => {
                tracing::debug!("[BraidFS] Ignoring file {:?}: {:?}", path, e);
            }
        }
    }
}
