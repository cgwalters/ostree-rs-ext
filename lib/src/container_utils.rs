//! Helpers for interacting with containers at runtime.

use anyhow::Result;

const REPO: &str = "/sysroot/ostree/repo";

/// Attempts to detect if the current process is running inside a container.
/// This looks for the `container` environment variable or the presence
/// of Docker or podman's more generic `/run/.containerenv`.
pub fn running_in_container() -> bool {
    if std::env::var_os("container").is_some() {
        return true;
    }
    // https://stackoverflow.com/questions/20010199/how-to-determine-if-a-process-runs-inside-lxc-docker
    for p in ["/run/.containerenv", "/.dockerenv"] {
        if std::path::Path::new(p).exists() {
            return true;
        }
    }
    false
}

pub(crate) fn has_ostree() -> bool {
    std::path::Path::new(REPO).exists()
}

pub(crate) fn require_ostree_repo() -> Result<()> {
    if !has_ostree() {
        anyhow::bail!("Missing {}", REPO)
    }
    Ok(())
}

pub(crate) fn require_container() -> Result<()> {
    if !running_in_container() {
        anyhow::bail!("The current process does not appear to be running inside a container")
    }
    Ok(())
}
