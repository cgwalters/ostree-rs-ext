//! APIs to generate an ostree commit using (layered) container content.
//!

use anyhow::Result;

/// Perform all final steps to generate an OSTree commit for the current root filesystem.
pub fn container_commit() -> Result<()> {
    crate::container_utils::require_container()?;
    crate::container_utils::require_ostree_repo()?;
    todo!()
}
