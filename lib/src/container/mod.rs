//! # APIs bridging OSTree and container images
//!
//! This crate contains APIs to bidirectionally map
//! between OSTree repositories and container images.

//#![deny(missing_docs)]
// Good defaults
#![forbid(unused_must_use)]
#![deny(unsafe_code)]

use std::convert::{TryFrom, TryInto};

/// Our generic catchall fatal error, expected to be converted
/// to a string to output to a terminal or logs.
type Result<T> = anyhow::Result<T>;

/// A backend/transport for OCI/Docker images.
#[derive(Debug, PartialEq, Eq)]
pub enum Transport {
    /// A remote Docker/OCI registry (`registry://` or `docker://`)
    Registry,
    /// A local OCI directory (`oci://`)
    OciDir,
    /// A local OCI archive tarball (`oci-archive://`)
    OciArchive,
}

/// Combination of a remote image reference and transport.
///
/// For example,
#[derive(Debug)]
pub struct ImageReference {
    /// The storage and transport for the image
    pub transport: Transport,
    /// The image name (e.g. `quay.io/somerepo/someimage:latest`)
    pub name: String,
}

impl TryFrom<&str> for Transport {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        Ok(match value {
            "registry" | "docker" => Self::Registry,
            "oci" => Self::OciDir,
            "oci-archive" => Self::OciArchive,
            o => return Err(anyhow::anyhow!("Unknown transport '{}'", o)),
        })
    }
}

impl TryFrom<&str> for ImageReference {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        let mut parts = value.splitn(2, "://");
        let transport: Transport = parts.next().unwrap().try_into()?;
        let name = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("Missing '://' in {}", value))?;
        Ok(Self {
            transport,
            name: name.to_string(),
        })
    }
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Registry => "registry",
            Self::OciArchive => "oci-archive",
            Self::OciDir => "oci",
        };
        f.write_str(s)
    }
}

impl std::fmt::Display for ImageReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}://{}", self.transport, self.name)
    }
}

mod export;
pub use export::*;
mod import;
pub use import::*;
pub mod oci;
mod skopeo;
