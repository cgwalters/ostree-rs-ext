//! APIs for creating container images from OSTree commits

use super::Result;
use super::{oci, skopeo};
use super::{ImageReference, Transport};
use crate::tar as ostree_tar;
use anyhow::Context;
use fn_error_context::context;
use std::path::Path;

/// Write an ostree commit to an OCI blob
#[context("Writing ostree root to blob")]
fn export_ostree_ref_to_blobdir(
    repo: &ostree::Repo,
    rev: &str,
    ocidir: &openat::Dir,
) -> Result<oci::Layer> {
    let commit = repo.resolve_rev(rev, false)?.unwrap();
    let mut w = oci::LayerWriter::new(ocidir)?;
    ostree_tar::export_commit(repo, commit.as_str(), &mut w)?;
    w.complete()
}

/// Generate an OCI image from a given ostree root
#[context("Building oci")]
fn build_oci(repo: &ostree::Repo, commit: &str, ocidir_path: &Path) -> Result<ImageReference> {
    // Explicitly error if the target exists
    std::fs::create_dir(ocidir_path).context("Creating OCI dir")?;
    let ocidir = &openat::Dir::open(ocidir_path)?;
    let writer = &mut oci::OciWriter::new(ocidir)?;

    let rootfs_blob = export_ostree_ref_to_blobdir(repo, commit, ocidir)?;
    writer.set_root_layer(rootfs_blob);
    writer.complete()?;

    Ok(ImageReference {
        transport: Transport::OciDir,
        name: ocidir_path.to_str().unwrap().to_string(),
    })
}

/// Helper for `build()` that avoids generics
fn build_impl(repo: &ostree::Repo, ostree_ref: &str, dest: &ImageReference) -> Result<()> {
    if dest.transport == Transport::OciDir {
        let _copied: ImageReference = build_oci(repo, ostree_ref, Path::new(dest.name.as_str()))?;
        Ok(())
    } else {
        let tempdir = tempfile::tempdir_in("/var/tmp")?;
        let dest = tempdir.path().join("d");
        let dest = dest.to_str().unwrap();
        let src = build_oci(repo, ostree_ref, Path::new(dest))?;
        let mut cmd = skopeo::new_cmd();
        cmd.stdout(std::process::Stdio::null())
            .arg("copy")
            .arg(src.to_string())
            .arg(dest.to_string());
        let proc = super::skopeo::spawn(cmd)?;
        let rt = tokio::runtime::Runtime::new()?;
        let output = rt.block_on(async move { proc.wait_with_output().await })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("skopeo failed: {}\n", stderr));
        }
        Ok(())
    }
}

/// Given an OSTree repository and ref, generate a container image.
pub fn export<S: AsRef<str>>(
    repo: &ostree::Repo,
    ostree_ref: S,
    dest: &ImageReference,
) -> Result<()> {
    build_impl(repo, ostree_ref.as_ref(), dest)
}
