//! APIs for extracting OSTree commits from container images

// # Implementation
//
// This code currently forks off `/usr/bin/skopeo` as a subprocess, and uses
// it to fetch the container content and convert it into a `docker-archive:`
// formatted tarball stream, which is written to a FIFO and parsed by
// this code.
//
// The rationale for this is that `/usr/bin/skopeo` is a frontend for
// the Go library https://github.com/containers/image/ which supports
// key things we want for production use like:
//
// - Image mirroring and remapping; effectively `man containers-registries.conf`
//   For example, we need to support an administrator mirroring an ostree-container
//   into a disconnected registry, without changing all the pull specs.
// - Signing
//
// # Import phases
//
// First, we support explicitly fetching just the manifest: https://github.com/opencontainers/image-spec/blob/main/manifest.md
// This will give us information about the layers it contains, and crucially the digest (sha256) of
// the manifest is how higher level software can detect changes.
//
// Once we have the manifest, we expect it to point to a single `application/vnd.oci.image.layer.v1.tar+gzip` layer,
// which is exactly what is exported by the [`crate::tar::export`] process.
//
// What we get from skopeo is a `docker-archive:` tarball, which then will contain this *inner* tarball
// layer that we extract and pass to the [`crate::tar::import`] code.

use super::*;
use anyhow::{anyhow, Context};
use camino::Utf8Path;
use fn_error_context::context;
use futures_util::{Future, FutureExt, Stream, StreamExt, TryFutureExt};
use std::io::prelude::*;
use std::pin::Pin;
use std::process::Stdio;
use tokio::io::AsyncRead;
use tracing::{event, instrument, Level};

/// The result of an import operation
#[derive(Copy, Clone, Debug, Default)]
pub struct ImportProgress {
    /// Number of bytes downloaded (approximate)
    pub processed_bytes: u64,
}

type Progress = tokio::sync::watch::Sender<ImportProgress>;

/// A read wrapper that updates the download progress.
struct ProgressReader {
    reader: Box<dyn AsyncRead + Unpin + Send + 'static>,
    progress: Option<Progress>,
}

impl AsyncRead for ProgressReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let pinned = Pin::new(&mut self.reader);
        let len = buf.filled().len();
        match pinned.poll_read(cx, buf) {
            v @ std::task::Poll::Ready(Ok(_)) => {
                let success = if let Some(progress) = self.progress.as_ref() {
                    let state = {
                        let mut state = *progress.borrow();
                        let newlen = buf.filled().len();
                        debug_assert!(newlen >= len);
                        let read = (newlen - len) as u64;
                        state.processed_bytes += read;
                        state
                    };
                    // Ignore errors, if the caller disconnected from progress that's OK.
                    progress.send(state).is_ok()
                } else {
                    true
                };
                if !success {
                    let _ = self.progress.take();
                }
                v
            }
            o => o,
        }
    }
}

/// Download the manifest for a target image and its sha256 digest.
#[context("Fetching manifest")]
pub async fn fetch_manifest(imgref: &OstreeImageReference) -> Result<(Vec<u8>, String)> {
    let mut proc = skopeo::new_cmd();
    let imgref_base = &imgref.imgref;
    proc.args(&["inspect", "--raw"])
        .arg(imgref_base.to_string());
    proc.stdout(Stdio::piped());
    let proc = skopeo::spawn(proc)?.wait_with_output().await?;
    if !proc.status.success() {
        let errbuf = String::from_utf8_lossy(&proc.stderr);
        return Err(anyhow!("skopeo inspect failed\n{}", errbuf));
    }
    let raw_manifest = proc.stdout;
    let digest = openssl::hash::hash(openssl::hash::MessageDigest::sha256(), &raw_manifest)?;
    let digest = format!("sha256:{}", hex::encode(digest.as_ref()));
    Ok((raw_manifest, digest))
}

/// Read the contents of the targeted layers identified via sha256.
/// The first return value is a stream of `AsyncRead` of those tar files.
/// The second return value is a background worker task that will
/// return back to the caller the provided input stream (converted
/// to a synchronous reader).  This ensures the caller can take
/// care of closing the input stream.
pub async fn find_layer_tars(
    src: impl AsyncRead + Send + Unpin + 'static,
    blobids: &[&str],
) -> Result<(
    impl Stream<Item = impl AsyncRead + Send + Unpin + 'static>,
    impl Future<Output = Result<impl Read + Send + Unpin + 'static>>,
)> {
    // Convert the async input stream to synchronous, becuase we currently use the
    // sync tar crate.
    let pipein = crate::async_util::async_read_to_sync(src);
    // An internal channel of Bytes
    let (tx_buf, rx_buf) = tokio::sync::mpsc::channel(2);
    // Clone to appease borrow checker
    let blobids: Vec<String> = blobids.iter().map(|s| s.to_string()).collect();
    let import = tokio::task::spawn_blocking(move || find_layer_tar_sync(pipein, blobids, tx_buf))
        .map_err(anyhow::Error::msg);
    // Bridge the channel to an AsyncRead
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx_buf);
    let stream = stream.map(|r| tokio_util::io::StreamReader::new(r));
    // This async task owns the internal worker thread, which also owns the provided
    // input stream which we return to the caller.
    let worker = async move {
        let src_as_sync = import.await?.context("Import worker")?;
        Ok::<_, anyhow::Error>(src_as_sync)
    };
    Ok((stream, worker))
}

// Helper function invoked to synchronously parse a `docker-archive:` formatted tar stream, finding
// the desired layer tarballs and writing its contents via a stream of byte chunks
// to a channel.
fn find_layer_tar_sync(
    pipein: impl Read + Send + Unpin,
    blobids: Vec<String>,
    tx_buf: tokio::sync::mpsc::Sender<tokio::sync::mpsc::Receiver<std::io::Result<bytes::Bytes>>>,
) -> Result<impl Read + Send + Unpin> {
    let mut archive = tar::Archive::new(pipein);
    let mut buf = vec![0u8; 8192];
    let mut ents = archive.entries()?.into_iter();
    for blobid in blobids {
        let digest = blobid
            .strip_prefix("sha256:")
            .ok_or_else(|| anyhow!("Invalid blobid: {}", blobid))?;
        let blob_target = format!("../{}", digest);
        let mut found = false;
        while let Some(entry) = ents.next() {
            let mut entry = entry.context("Reading entry")?;
            let path = entry.path()?;
            let path: &Utf8Path = path.deref().try_into()?;
            // We generally expect our layer to be first, but let's just skip anything
            // unexpected to be robust against changes in skopeo.
            if path.extension() != Some("tar") {
                continue;
            }
            event!(Level::DEBUG, "Found {}", path);

            match entry.header().entry_type() {
                tar::EntryType::Symlink => {
                    if let Some(name) = path.file_name() {
                        if name == "layer.tar" {
                            let target = entry
                                .link_name()?
                                .ok_or_else(|| anyhow!("Invalid link {}", path))?;
                            let target = Utf8Path::from_path(&*target)
                                .ok_or_else(|| anyhow!("Invalid non-UTF8 path {:?}", target))?;
                            if target != blob_target {
                                return Err(anyhow!(
                                    "Found unexpected layer link {} -> {}",
                                    path,
                                    target
                                ));
                            }
                        }
                    }
                }
                tar::EntryType::Regular => {
                    let (tx_child, rx_child) = tokio::sync::mpsc::channel(2);
                    tx_buf.send(rx_child);
                    loop {
                        let n = entry
                            .read(&mut buf[..])
                            .context("Reading tar file contents")?;
                        let done = 0 == n;
                        let r = Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(&buf[0..n]));
                        let receiver_closed = tx_child.blocking_send(r).is_err();
                        if receiver_closed || done {
                            found = true;
                            break;
                        }
                    }
                }
                _ => continue,
            }
        }
        if !found {
            return Err(anyhow!("Failed to find layer {}", blobid));
        }
    }
    Ok(archive.into_inner())
}

/// Fetch a remote docker/OCI image and extract targeted layers.
async fn fetch_layers<'s>(
    imgref: &OstreeImageReference,
    layers: &[&str],
    progress: Option<tokio::sync::watch::Sender<ImportProgress>>,
) -> Result<(
    impl futures_util::Stream<Item = impl AsyncRead + Unpin + Send>,
    impl Future<Output = Result<()>>,
)> {
    let mut proc = skopeo::new_cmd();
    proc.stdout(Stdio::null());
    let tempdir = tempfile::Builder::new()
        .prefix("ostree-rs-ext")
        .tempdir_in("/var/tmp")?;
    let tempdir = Utf8Path::from_path(tempdir.path()).unwrap();
    let fifo = &tempdir.join("skopeo.pipe");
    nix::unistd::mkfifo(
        fifo.as_os_str(),
        nix::sys::stat::Mode::from_bits(0o600).unwrap(),
    )?;
    tracing::trace!("skopeo pull starting to {}", fifo);
    proc.arg("copy")
        .arg(imgref.imgref.to_string())
        .arg(format!("docker-archive:{}", fifo));
    let proc = skopeo::spawn(proc)?;
    let fifo_reader = ProgressReader {
        reader: Box::new(tokio::fs::File::open(fifo).await?),
        progress,
    };
    let waiter = async move {
        let res = proc.wait_with_output().await?;
        if !res.status.success() {
            return Err(anyhow!(
                "skopeo failed: {}\n{}",
                res.status,
                String::from_utf8_lossy(&res.stderr)
            ));
        }
        Ok(())
    }
    .boxed();
    let (contents, worker) = find_layer_tars(fifo_reader, layers).await?;
    let worker = async move {
        let (worker, waiter) = tokio::join!(worker, waiter);
        let _: () = waiter?;
        let _pipein = worker.context("Layer worker failed")?;
        Ok::<_, anyhow::Error>(())
    };
    Ok((contents, worker))
}

/// The result of an import operation
#[derive(Debug)]
pub struct Import {
    /// The ostree commit that was imported
    pub ostree_commit: String,
    /// The image digest retrieved
    pub image_digest: String,
}

fn find_layer_blobids(manifest: &oci::Manifest) -> Result<Vec<&str>> {
    manifest
        .layers
        .iter()
        .filter_map(|&layer| -> Option<Result<&str>> {
            if matches!(
                layer.media_type.as_str(),
                super::oci::DOCKER_TYPE_LAYER | oci::OCI_TYPE_LAYER
            ) {
                Some(
                    layer
                        .digest
                        .strip_prefix("sha256:")
                        .ok_or_else(|| anyhow!("Expected sha256: in digest: {}", layer.digest)),
                )
            } else {
                None
            }
        })
        .collect()
}

/// Configuration for container fetches.
#[derive(Debug, Default)]
pub struct ImportOptions {
    /// Channel which will receive progress updates
    pub progress: Option<tokio::sync::watch::Sender<ImportProgress>>,
    /// Process layers
    pub layers: bool,
}

/// Fetch a container image and import its embedded OSTree commit.
#[context("Importing {}", imgref)]
#[instrument(skip(repo, options))]
pub async fn import(
    repo: &ostree::Repo,
    imgref: &OstreeImageReference,
    options: Option<ImportOptions>,
) -> Result<Import> {
    let (manifest, image_digest) = fetch_manifest(imgref).await?;
    let ostree_commit = import_from_manifest(repo, imgref, &manifest, options).await?;
    Ok(Import {
        ostree_commit,
        image_digest,
    })
}

/// Fetch a container image using an in-memory manifest and import its embedded OSTree commit.
#[context("Importing {}", imgref)]
#[instrument(skip(repo, options, manifest_bytes))]
pub async fn import_from_manifest(
    repo: &ostree::Repo,
    imgref: &OstreeImageReference,
    manifest_bytes: &[u8],
    options: Option<ImportOptions>,
) -> Result<String> {
    if matches!(imgref.sigverify, SignatureSource::ContainerPolicy)
        && skopeo::container_policy_is_default_insecure()?
    {
        return Err(anyhow!("containers-policy.json specifies a default of `insecureAcceptAnything`; refusing usage"));
    }
    let options = options.unwrap_or_default();
    let manifest: oci::Manifest = serde_json::from_slice(manifest_bytes)?;
    let layerids = find_layer_blobids(&manifest)?;
    if layerids.len() == 0 {
        return Err(anyhow!("No layers found in image"));
    }
    let target_layer_index = layerids.len().checked_sub(1).unwrap();
    event!(Level::DEBUG, "target blobs: {:?}", layerids);
    let (blobs, worker) = fetch_layers(imgref, &layerids, options.progress).await?;
    let mut n = 0usize;
    let mut imports = Vec::new();
    while let Some(blob) = blobs.next().await {
        let at_target = target_layer_index == n;
        let blob = tokio::io::BufReader::new(blob);
        let mut taropts: crate::tar::TarImportOptions = Default::default();
        // We only verify the final layer's signature.
        if at_target {
            match &imgref.sigverify {
                SignatureSource::OstreeRemote(remote) => taropts.remote = Some(remote.clone()),
                SignatureSource::ContainerPolicy
                | SignatureSource::ContainerPolicyAllowInsecure => {}
            }
        }
        // Note that we explicitly don't `await?` i.e. check for errors here, because
        // if the skopeo process errored out, we want that to take precedence.
        imports.push(crate::tar::import_tar(repo, blob, Some(taropts)).await);
    }
    // Explicitly check if skopeo errored out - if so, any error from our tar import
    // is likely caused by that, so show the user the real error.
    let _: () = worker.await?;
    let ostree_commit = imports
        .pop()
        .ok_or_else(|| anyhow!("Failed to find a layer"))??;
    event!(Level::DEBUG, "created commit {}", ostree_commit);
    Ok(ostree_commit)
}
