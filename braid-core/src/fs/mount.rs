use anyhow::{Context, Result};
use std::path::Path;
use std::process::Command;
use tracing::{info, warn};

/// Mounts the BraidFS NFS share at the specified mount point.
pub fn mount(port: u16, mount_point: &Path) -> Result<()> {
    info!("Mounting using system Native NFS client...");

    // ensure mount point exists
    if !mount_point.exists() {
        std::fs::create_dir_all(mount_point).context("Failed to create mount point")?;
    }

    #[cfg(target_os = "macos")]
    {
        // Ported from legit-code
        // mount_nfs -o nolocks,soft,retrans=2,timeo=10,vers=3,tcp,rsize=131072,actimeo=120,port=${port},mountport=${port} localhost:/ ${mountPoint}
        let status = Command::new("mount_nfs")
            .arg("-o")
            .arg(format!("nolocks,soft,retrans=2,timeo=10,vers=3,tcp,rsize=131072,actimeo=120,port={},mountport={}", port, port))
            .arg("localhost:/")
            .arg(mount_point)
            .status()
            .context("Failed to execute mount_nfs")?;

        if !status.success() {
            anyhow::bail!("mount_nfs failed with exit code: {:?}", status.code());
        }
    }

    #[cfg(target_os = "linux")]
    {
        // Standard Linux mount
        // mount -t nfs -o port=${port},mountport=${port},tcp,vers=3,nolock localhost:/ ${mountPoint}
        let status = Command::new("mount")
            .arg("-t")
            .arg("nfs")
            .arg("-o")
            .arg(format!(
                "port={},mountport={},tcp,vers=3,nolock",
                port, port
            ))
            .arg("localhost:/")
            .arg(mount_point)
            .status()
            .context("Failed to execute mount")?;

        if !status.success() {
            anyhow::bail!("mount failed with exit code: {:?}", status.code());
        }
    }

    #[cfg(target_os = "windows")]
    {
        // Windows 'mount' command from Client for NFS
        // mount -o anon \\localhost\ Z:
        // Note: Windows mount syntax is: mount [options] \\computername\sharename device_name

        // We need to map the port. Windows built-in NFS client often assumes 2049.
        // If we are running on a custom port, Windows might have trouble unless 'rpcbind' maps it,
        // or we use specific syntax if supported.
        // However, standard `mount.exe` on Windows is limited.

        // Strategy: Try standard mount. If it fails, warn user.
        // If mount_point acts as a drive letter (e.g. "Z:"), use it.
        // Otherwise, Windows mounting usually requires a Drive Letter.

        // For CLI tools without a drive letter, we might just be out of luck with standard 'mount'.
        // But let's assume the user might want to map to a drive.

        // WORKAROUND: We can't easily force Windows to mount to a directory on a custom port via CLI easily
        // without more setup. But we will try the basic command.

        warn!("Windows Auto-Mounting is experimental. Ensure 'Client for NFS' is installed.");
        warn!(
            "If using a custom port ({}), Windows might not discover the NFS server easily.",
            port
        );

        // Try mapping to * (next available drive)
        let status = Command::new("mount")
            .arg("-o")
            .arg("anon")
            .arg(format!("\\\\localhost:{}", port))
            .arg("*")
            .status();

        match status {
            Ok(s) => {
                if !s.success() {
                    warn!("Windows mount command returned error code: {:?}", s.code());
                } else {
                    info!("Windows mount command succeeded.");
                }
            }
            Err(e) => {
                warn!(
                    "Failed to execute 'mount'. Is Client for NFS installed? Error: {}",
                    e
                );
            }
        }
    }

    info!("Mounting command executed.");
    Ok(())
}

/// Unmounts the BraidFS NFS share.
pub fn unmount(mount_point: &Path) -> Result<()> {
    info!("Unmounting...");

    #[cfg(target_os = "macos")]
    {
        let status = Command::new("umount")
            .arg(mount_point)
            .status()
            .context("Failed to execute umount")?;
        if !status.success() {
            anyhow::bail!("umount failed with exit code: {:?}", status.code());
        }
    }

    #[cfg(target_os = "linux")]
    {
        // umount -l for lazy unmount if busy?
        let status = Command::new("umount")
            .arg(mount_point)
            .status()
            .context("Failed to execute umount")?;
        if !status.success() {
            anyhow::bail!("umount failed with exit code: {:?}", status.code());
        }
    }

    #[cfg(target_os = "windows")]
    {
        // umount [drive_letter]
        // Since we don't know the driver letter for sure if we used '*', ensuring unmount is hard.
        // But if the user provided "Z:", we can unmount it.
        let s = mount_point.to_string_lossy();
        if s.contains(":") {
            let _status = Command::new("umount").arg(s.as_ref()).status();
            // ignore errors
        } else {
            // umount -a to unmount all? Dangerous.
            // umount \\localhost\...
            warn!("Windows unmount requires a drive letter. Skipping explicit unmount logic for path.");
        }
    }

    Ok(())
}
