//! Split an OSTree commit into separate chunks

// SPDX-License-Identifier: Apache-2.0 OR MIT

use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::convert::TryInto;
use std::rc::Rc;

use crate::objectsource::{ContentID, ObjectMeta};
use crate::objgv::*;
use anyhow::Result;
use camino::Utf8PathBuf;
use gvariant::aligned_bytes::TryAsAligned;
use gvariant::{Marker, Structure};
use ostree::prelude::*;
use ostree::{gio, glib};

const FIRMWARE: &str = "/usr/lib/firmware";
const MODULES: &str = "/usr/lib/modules";

const QUERYATTRS: &str = "standard::name,standard::type";

/// Size in bytes of the smallest chunk we will emit.
// pub(crate) const MIN_CHUNK_SIZE: u32 = 10 * 1024;
/// Maximum number of layers (chunks) we will use.
// We take half the limit of 128.
// https://github.com/ostreedev/ostree-rs-ext/issues/69
pub(crate) const MAX_CHUNKS: u32 = 64;

// The percentage of size remaining for which we generate a new chunk
pub(crate) const CHUNKING_PERCENTAGE: u32 = 10;

/// Size in bytes for the minimum size for chunks
#[allow(dead_code)]
pub(crate) const DEFAULT_MIN_CHUNK: usize = 10 * 1024;

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub(crate) struct RcStr(Rc<str>);

impl Borrow<str> for RcStr {
    fn borrow(&self) -> &str {
        &*self.0
    }
}

impl From<&str> for RcStr {
    fn from(s: &str) -> Self {
        Self(Rc::from(s))
    }
}

#[derive(Debug, Default)]
pub(crate) struct Chunk {
    pub(crate) name: String,
    pub(crate) content: BTreeMap<RcStr, (u64, Vec<Utf8PathBuf>)>,
    pub(crate) size: u64,
}

#[derive(Debug)]
pub(crate) enum Meta {
    DirTree(RcStr),
    DirMeta(RcStr),
}

impl Meta {
    pub(crate) fn objtype(&self) -> ostree::ObjectType {
        match self {
            Meta::DirTree(_) => ostree::ObjectType::DirTree,
            Meta::DirMeta(_) => ostree::ObjectType::DirMeta,
        }
    }

    pub(crate) fn checksum(&self) -> &str {
        match self {
            Meta::DirTree(v) => &*v.0,
            Meta::DirMeta(v) => &*v.0,
        }
    }
}

/// How to split up an ostree commit into "chunks" - designed to map to container image layers.
#[derive(Debug, Default)]
pub struct Chunking {
    pub(crate) metadata_size: u64,
    pub(crate) commit: Box<str>,
    pub(crate) meta: Vec<Meta>,
    pub(crate) remainder: Chunk,
    pub(crate) chunks: Vec<Chunk>,
}

// pub(crate) struct ChunkConfig {
//     pub(crate) min_size: u32,
//     pub(crate) max_chunks: u32,
// }
//
// impl Default for ChunkConfig {
//     fn default() -> Self {
//         Self {
//             min_size: MIN_CHUNK_SIZE,
//             max_chunks: MAX_CHUNKS,
//         }
//     }
// }

#[derive(Default)]
struct Generation {
    path: Utf8PathBuf,
    metadata_size: u64,
    meta: Vec<Meta>,
    dirtree_found: BTreeSet<RcStr>,
    dirmeta_found: BTreeSet<RcStr>,
}

fn push_dirmeta(repo: &ostree::Repo, gen: &mut Generation, checksum: &str) -> Result<()> {
    if gen.dirtree_found.contains(checksum) {
        return Ok(());
    }
    let checksum = RcStr::from(checksum);
    gen.dirmeta_found.insert(RcStr::clone(&checksum));
    let child_v = repo.load_variant(ostree::ObjectType::DirMeta, checksum.borrow())?;
    gen.metadata_size += child_v.data_as_bytes().as_ref().len() as u64;
    gen.meta.push(Meta::DirMeta(checksum));
    Ok(())
}

fn push_dirtree(
    repo: &ostree::Repo,
    gen: &mut Generation,
    checksum: &str,
) -> Result<Option<glib::Variant>> {
    if gen.dirtree_found.contains(checksum) {
        return Ok(None);
    }
    let child_v = repo.load_variant(ostree::ObjectType::DirTree, checksum)?;
    let checksum = RcStr::from(checksum);
    gen.dirtree_found.insert(RcStr::clone(&checksum));
    gen.meta.push(Meta::DirTree(checksum));
    gen.metadata_size += child_v.data_as_bytes().as_ref().len() as u64;
    Ok(Some(child_v))
}

fn generate_chunking_recurse(
    repo: &ostree::Repo,
    gen: &mut Generation,
    chunk: &mut Chunk,
    dt: &glib::Variant,
) -> Result<()> {
    let dt = dt.data_as_bytes();
    let dt = dt.try_as_aligned()?;
    let dt = gv_dirtree!().cast(dt);
    let (files, dirs) = dt.to_tuple();
    // A reusable buffer to avoid heap allocating these
    let mut hexbuf = [0u8; 64];
    for file in files {
        let (name, csum) = file.to_tuple();
        let fpath = gen.path.join(name.to_str());
        hex::encode_to_slice(csum, &mut hexbuf)?;
        let checksum = std::str::from_utf8(&hexbuf)?;
        let (_, meta, _) = repo.load_file(checksum, gio::NONE_CANCELLABLE)?;
        // SAFETY: We know this API returns this value; it only has a return nullable because the
        // caller can pass NULL to skip it.
        let meta = meta.unwrap();
        let size = meta.size() as u64;
        let entry = chunk.content.entry(RcStr::from(checksum)).or_default();
        entry.0 = size;
        let first = entry.1.is_empty();
        if first {
            chunk.size += size;
        }
        entry.1.push(fpath);
    }
    for item in dirs {
        let (name, contents_csum, meta_csum) = item.to_tuple();
        let name = name.to_str();
        // Extend our current path
        gen.path.push(name);
        hex::encode_to_slice(contents_csum, &mut hexbuf)?;
        let checksum_s = std::str::from_utf8(&hexbuf)?;
        if let Some(child_v) = push_dirtree(repo, gen, checksum_s)? {
            generate_chunking_recurse(repo, gen, chunk, &child_v)?;
        }
        hex::encode_to_slice(meta_csum, &mut hexbuf)?;
        let checksum_s = std::str::from_utf8(&hexbuf)?;
        push_dirmeta(repo, gen, checksum_s)?;
        // We did a push above, so pop must succeed.
        assert!(gen.path.pop());
    }
    Ok(())
}

impl Chunk {
    fn new(name: &str) -> Self {
        Chunk {
            name: name.to_string(),
            ..Default::default()
        }
    }

    fn move_obj(&mut self, dest: &mut Self, checksum: &str) -> bool {
        // In most cases, we expect the object to exist in the source.  However, it's
        // conveneient here to simply ignore objects which were already moved into
        // a chunk.
        if let Some((name, (size, paths))) = self.content.remove_entry(checksum) {
            let v = dest.content.insert(name, (size, paths));
            debug_assert!(v.is_none());
            self.size -= size;
            dest.size += size;
            true
        } else {
            false
        }
    }
}

fn find_kernel_dir(
    root: &gio::File,
    cancellable: Option<&gio::Cancellable>,
) -> Result<Option<gio::File>> {
    let moddir = root.resolve_relative_path(MODULES);
    let e = moddir.enumerate_children(
        "standard::name",
        gio::FileQueryInfoFlags::NOFOLLOW_SYMLINKS,
        cancellable,
    )?;
    let mut r = None;
    for child in e.clone() {
        let child = &child?;
        let childpath = e.child(child);
        if child.file_type() == gio::FileType::Directory && r.replace(childpath).is_some() {
            anyhow::bail!("Found multiple subdirectories in {}", MODULES);
        }
    }
    Ok(r)
}

impl Chunking {
    /// Generate an initial single chunk.
    pub fn new(repo: &ostree::Repo, rev: &str) -> Result<Self> {
        // Find the target commit
        let rev = repo.require_rev(rev)?;

        // Load and parse the commit object
        let (commit_v, _) = repo.load_commit(&rev)?;
        let commit_v = commit_v.data_as_bytes();
        let commit_v = commit_v.try_as_aligned()?;
        let commit = gv_commit!().cast(commit_v);
        let commit = commit.to_tuple();

        // Load it all into a single chunk
        let mut gen = Generation {
            path: Utf8PathBuf::from("/"),
            ..Default::default()
        };
        let mut chunk: Chunk = Default::default();

        // Find the root directory tree
        let contents_checksum = &hex::encode(commit.6);
        let contents_v = repo.load_variant(ostree::ObjectType::DirTree, contents_checksum)?;
        push_dirtree(repo, &mut gen, contents_checksum)?;
        let meta_checksum = &hex::encode(commit.7);
        push_dirmeta(repo, &mut gen, meta_checksum.as_str())?;

        generate_chunking_recurse(repo, &mut gen, &mut chunk, &contents_v)?;

        let chunking = Chunking {
            commit: Box::from(rev.as_str()),
            metadata_size: gen.metadata_size,
            meta: gen.meta,
            remainder: chunk,
            ..Default::default()
        };
        Ok(chunking)
    }

    fn remaining(&self) -> u32 {
        MAX_CHUNKS.saturating_sub(self.chunks.len() as u32)
    }

    /// Find the object named by `path` in `src`, and move it to `dest`.
    fn extend_chunk(
        repo: &ostree::Repo,
        src: &mut Chunk,
        dest: &mut Chunk,
        path: &ostree::RepoFile,
    ) -> Result<()> {
        let cancellable = gio::NONE_CANCELLABLE;
        let ft = path.query_file_type(gio::FileQueryInfoFlags::NOFOLLOW_SYMLINKS, cancellable);
        if ft == gio::FileType::Directory {
            let e = path.enumerate_children(
                QUERYATTRS,
                gio::FileQueryInfoFlags::NOFOLLOW_SYMLINKS,
                cancellable,
            )?;
            for child in e {
                let childi = child?;
                let child = path.child(childi.name());
                let child = child.downcast::<ostree::RepoFile>().unwrap();
                Self::extend_chunk(repo, src, dest, &child)?;
            }
        } else {
            let checksum = path.checksum().unwrap();
            src.move_obj(dest, checksum.as_str());
        }
        Ok(())
    }

    /// Create a new chunk from the provided filesystem paths.
    pub(crate) fn chunk_paths<'a>(
        &mut self,
        repo: &ostree::Repo,
        paths: impl IntoIterator<Item = &'a gio::File>,
        name: &str,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<()> {
        // Do nothing if we've hit our max.
        if self.remaining() == 0 {
            return Ok(());
        }

        let mut chunk = Chunk::new(name);
        for path in paths {
            if !path.query_exists(cancellable) {
                continue;
            }
            let child = path.downcast_ref::<ostree::RepoFile>().unwrap();
            Self::extend_chunk(repo, &mut self.remainder, &mut chunk, child)?;
        }
        if !chunk.content.is_empty() {
            self.chunks.push(chunk);
        }
        Ok(())
    }

    /// Given metadata about which objects are owned by a particular content source,
    /// generate chunks that group together those objects.
    pub fn process_mapping(&mut self, repo: &ostree::Repo, contentmeta: &ObjectMeta) -> Result<()> {
        let remaining = self.remaining();
        if remaining == 0 {
            return Ok(());
        }

        let cancellable = gio::NONE_CANCELLABLE;
        let mut sizes = HashMap::<u32, u64>::new();
        // Reverses `contentmeta.map` i.e. contentid -> Vec<checksum>
        let mut rmap = HashMap::<u32, Vec<&String>>::new();
        for (checksum, &contentid) in contentmeta.map.iter() {
            rmap.entry(contentid).or_default().push(checksum);
            let (_, finfo, _) = repo.load_file(checksum, cancellable)?;
            let finfo = finfo.unwrap();
            let sz = sizes.entry(contentid).or_default();
            *sz += finfo.size() as u64;
        }
        let mut sizes: Vec<_> = sizes.into_iter().collect();
        sizes.sort_by(|a, b| b.1.cmp(&a.1));

        for (id, sz) in sizes.into_iter().take(remaining.try_into().unwrap()) {
            let srcmeta = contentmeta
                .set
                .get(&id)
                .ok_or_else(|| anyhow::anyhow!("Missing metadata for {}", id))?;
            let mut chunk = Chunk::new(srcmeta.name.as_str());
            for &obj in rmap.get(&id).unwrap() {
                self.remainder.move_obj(&mut chunk, obj.as_str());
            }
            if !chunk.content.is_empty() {
                self.chunks.push(chunk);
            }
        }

        Ok(())
    }

    /// Find the kernel and initramfs, and put them in their own chunk.
    pub(crate) fn chunk_kernel_initramfs(
        &mut self,
        repo: &ostree::Repo,
        root: &gio::File,
        cancellable: Option<&gio::Cancellable>,
    ) -> Result<()> {
        let moddir = if let Some(m) = find_kernel_dir(root, cancellable)? {
            m
        } else {
            return Ok(());
        };
        // The initramfs has a dependency on userspace *and* kernel, so we
        // should chunk the kernel separately.
        let initramfs = &moddir.resolve_relative_path("initramfs.img");
        self.chunk_paths(repo, [initramfs], "initramfs", cancellable)?;
        let kernel_name = format!("Linux kernel {:?}", moddir.basename().unwrap());
        // Gather all of the rest of the kernel as a single chunk
        self.chunk_paths(repo, [&moddir], kernel_name.as_str(), cancellable)
    }

    /// Apply built-in heuristics to automatically create chunks.
    pub(crate) fn auto_chunk(&mut self, repo: &ostree::Repo) -> Result<()> {
        let cancellable = gio::NONE_CANCELLABLE;
        let root = &repo.read_commit(&self.commit, cancellable)?.0;

        // Grab all of linux-firmware; it's the largest thing in FCOS.
        let firmware = root.resolve_relative_path(FIRMWARE);
        self.chunk_paths(repo, [&firmware], FIRMWARE, cancellable)?;

        // Kernel and initramfs
        self.chunk_kernel_initramfs(repo, root, cancellable)?;

        self.large_files(20, 1)?;

        Ok(())
    }

    /// Gather large files (up to `max` chunks) as a percentage (1-99) of total size.
    pub(crate) fn large_files(&mut self, max: u32, percentage: u32) -> Result<()> {
        let max = max.min(self.remaining());
        if max == 0 {
            return Ok(());
        }

        let mut large_objects = Vec::new();
        let total_size = self.remainder.size;
        let largefile_limit = (total_size * (percentage * 100) as u64) / total_size;
        for (objid, (size, _names)) in &self.remainder.content {
            if *size > largefile_limit {
                large_objects.push((*size, objid.clone()));
            }
        }
        large_objects.sort_by(|a, b| a.0.cmp(&b.0));
        for (_size, objid) in large_objects.iter().rev().take(max as usize) {
            let mut chunk = {
                let (_size, names) = self.remainder.content.get(objid).unwrap();
                let name = &names[0];
                Chunk::new(name.as_str())
            };
            let moved = self.remainder.move_obj(&mut chunk, objid.borrow());
            // The object only exists once, so we must have moved it.
            assert!(moved);
            self.chunks.push(chunk);
        }
        Ok(())
    }

    pub(crate) fn take_chunks(&mut self) -> Vec<Chunk> {
        let mut r = Vec::new();
        std::mem::swap(&mut self.chunks, &mut r);
        r
    }

    /// Print information about chunking to standard output.
    pub fn print(&self) {
        println!("Metadata: {}", glib::format_size(self.metadata_size));
        for (n, chunk) in self.chunks.iter().enumerate() {
            let sz = glib::format_size(chunk.size);
            println!(
                "Chunk {}: \"{}\": objects:{} size:{}",
                n,
                chunk.name,
                chunk.content.len(),
                sz
            );
        }
        let sz = glib::format_size(self.remainder.size);
        println!(
            "Remainder: objects:{} size:{}",
            self.remainder.content.len(),
            sz
        );
    }
}
