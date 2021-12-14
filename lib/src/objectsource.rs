//! Metadata about the source of an object: a component or package.
//!
//! This is used to help split up containers into distinct layers.

use crate::Result;
use cap_std_ext::cap_std;
use cap_std_ext::dirext::CapStdExtDirExt;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// Initial serialized version;
const VERSION: u32 = 1;

/// Metadata about a component/package.
#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectSourceMeta {
    /// Unitless relative frequency of changes; zero is lowest.
    pub change_frequency: u32,
}

/// Maps from e.g. "bash" or "kernel" to metadata about that content
pub type ObjectMetaSet = BTreeMap<u32, ObjectSourceMeta>;

/// Maps from an ostree content object digest to the `ContentSet` key.
pub type ObjectMetaMap = BTreeMap<String, u32>;

/// Grouping of metadata about an object.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ObjectMeta {
    /// The set of object sources with their metadata.
    pub set: ObjectMetaSet,
    /// Mapping from content object to source.
    pub map: ObjectMetaMap,
}

#[derive(Serialize, Deserialize)]
struct SerializedObjectSourceMeta {
    version: u32,
    map: ObjectMetaMap,
    set: ObjectMetaSet,
}

impl From<SerializedObjectSourceMeta> for ObjectMeta {
    fn from(s: SerializedObjectSourceMeta) -> Self {
        ObjectMeta {
            set: s.set,
            map: s.map,
        }
    }
}

impl ObjectMeta {
    /// Write object metadata to a file.
    pub fn serialize(self, dir: &cap_std::fs::Dir, path: impl AsRef<Path>) -> Result<()> {
        serialize_impl(dir, path.as_ref(), self)
    }

    /// Write object metadata to a file.
    pub fn deserialize(dir: &cap_std::fs::Dir, path: impl AsRef<Path>) -> Result<Self> {
        deserialize_impl(dir, path.as_ref())
    }
}

fn serialize_impl(dir: &cap_std::fs::Dir, path: &Path, meta: ObjectMeta) -> Result<()> {
    let serialized = SerializedObjectSourceMeta {
        version: VERSION,
        map: meta.map,
        set: meta.set,
    };
    dir.replace_file_with(path, move |mut w| -> Result<()> {
        serde_json::to_writer(&mut w, &serialized)?;
        Ok(())
    })
}

fn deserialize_impl(dir: &cap_std::fs::Dir, path: &Path) -> Result<ObjectMeta> {
    let f = dir.open(path)?;
    let r: SerializedObjectSourceMeta = serde_json::from_reader(std::io::BufReader::new(f))?;
    anyhow::ensure!(r.version == VERSION);
    Ok(r.into())
}
