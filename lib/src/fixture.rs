//! Test suite fixture.  Should only be used by this library.

#![allow(missing_docs)]

use crate::chunking::ObjectMetaSized;
use crate::container::store::LayeredImageState;
use crate::container::{
    store, Config, ExportOpts, ImageReference, OstreeImageReference, Transport,
};
use crate::objectsource::{ObjectMeta, ObjectSourceMeta};
use crate::prelude::*;
use crate::{gio, glib};
use anyhow::{anyhow, Context, Result};
use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
use cap_std::fs::Dir;
use cap_std_ext::cap_std;
use cap_std_ext::prelude::CapStdExtCommandExt;
use chrono::TimeZone;
use fn_error_context::context;
use io_lifetimes::AsFd;
use once_cell::sync::Lazy;
use regex::Regex;
use std::borrow::Cow;
use std::io::Write;
use std::ops::Add;
use std::process::Stdio;
use std::rc::Rc;
use std::sync::Arc;

const OSTREE_GPG_HOME: &[u8] = include_bytes!("fixtures/ostree-gpg-test-home.tar.gz");
const TEST_GPG_KEYID_1: &str = "7FCA23D8472CDAFA";
#[allow(dead_code)]
const TEST_GPG_KEYFPR_1: &str = "5E65DE75AB1C501862D476347FCA23D8472CDAFA";
const TESTREF: &str = "exampleos/x86_64/stable";

#[derive(Debug)]
enum FileDefType {
    Regular(Cow<'static, str>),
    Symlink(Cow<'static, Utf8Path>),
    Directory,
}

#[derive(Debug)]
pub struct FileDef {
    uid: u32,
    gid: u32,
    mode: u32,
    path: Cow<'static, Utf8Path>,
    ty: FileDefType,
}

impl TryFrom<&'static str> for FileDef {
    type Error = anyhow::Error;

    fn try_from(value: &'static str) -> Result<Self, Self::Error> {
        let mut parts = value.split(' ');
        let tydef = parts
            .next()
            .ok_or_else(|| anyhow!("Missing type definition"))?;
        let name = parts.next().ok_or_else(|| anyhow!("Missing file name"))?;
        let contents = parts.next();
        let contents = move || contents.ok_or_else(|| anyhow!("Missing file contents: {}", value));
        if parts.next().is_some() {
            anyhow::bail!("Invalid filedef: {}", value);
        }
        let ty = match tydef {
            "r" => FileDefType::Regular(contents()?.into()),
            "l" => FileDefType::Symlink(Cow::Borrowed(contents()?.into())),
            "d" => FileDefType::Directory,
            _ => anyhow::bail!("Invalid filedef type: {}", value),
        };
        Ok(FileDef {
            uid: 0,
            gid: 0,
            mode: 0o644,
            path: Cow::Borrowed(name.into()),
            ty,
        })
    }
}

fn parse_mode(line: &str) -> Result<(u32, u32, u32)> {
    let mut parts = line.split(' ').skip(1);
    // An empty mode resets to defaults
    let uid = if let Some(u) = parts.next() {
        u
    } else {
        return Ok((0, 0, 0o644));
    };
    let gid = parts.next().ok_or_else(|| anyhow!("Missing gid"))?;
    let mode = parts.next().ok_or_else(|| anyhow!("Missing mode"))?;
    if parts.next().is_some() {
        anyhow::bail!("Invalid mode: {}", line);
    }
    Ok((uid.parse()?, gid.parse()?, u32::from_str_radix(mode, 8)?))
}

impl FileDef {
    /// Parse a list of newline-separated file definitions.
    pub fn iter_from(defs: &'static str) -> impl Iterator<Item = Result<FileDef>> {
        let mut uid = 0;
        let mut gid = 0;
        let mut mode = 0o644;
        defs.lines()
            .filter(|v| !(v.is_empty() || v.starts_with('#')))
            .filter_map(move |line| {
                if line.starts_with('m') {
                    match parse_mode(line) {
                        Ok(r) => {
                            uid = r.0;
                            gid = r.1;
                            mode = r.2;
                            None
                        }
                        Err(e) => Some(Err(e)),
                    }
                } else {
                    Some(FileDef::try_from(line).map(|mut def| {
                        def.uid = uid;
                        def.gid = gid;
                        def.mode = mode;
                        def
                    }))
                }
            })
    }
}

/// This is like a package database, mapping our test fixture files to package names
static OWNERS: Lazy<Vec<(Regex, &str)>> = Lazy::new(|| {
    [
        ("usr/lib/modules/.*/initramfs", "initramfs"),
        ("usr/lib/modules", "kernel"),
        ("usr/bin/(ba)?sh", "bash"),
        ("usr/lib.*/emptyfile.*", "bash"),
        ("usr/bin/hardlink.*", "testlink"),
        ("usr/etc/someconfig.conf", "someconfig"),
        ("usr/etc/polkit.conf", "a-polkit-config"),
        ("opt", "filesystem"),
        ("usr/lib/pkgdb", "pkgdb"),
        ("usr/lib/sysimage/pkgdb", "pkgdb"),
    ]
    .iter()
    .map(|(k, v)| (Regex::new(k).unwrap(), *v))
    .collect()
});

pub static CONTENTS_V0: &str = indoc::indoc! { r##"
r usr/lib/modules/5.10.18-200.x86_64/vmlinuz this-is-a-kernel
r usr/lib/modules/5.10.18-200.x86_64/initramfs this-is-an-initramfs
m 0 0 755
r usr/bin/bash the-bash-shell
l usr/bin/sh bash
m 0 0 644
# Some empty files
r usr/lib/emptyfile 
r usr/lib64/emptyfile2 
# Should be the same object
r usr/bin/hardlink-a testlink
r usr/bin/hardlink-b testlink
r usr/etc/someconfig.conf someconfig
m 10 10 644
r usr/etc/polkit.conf a-polkit-config
m 0 0 644
# See https://github.com/coreos/fedora-coreos-tracker/issues/1258
r usr/lib/sysimage/pkgdb some-package-database
r usr/lib/pkgdb/pkgdb some-package-database
m
d boot
d run
l opt var/opt
m 0 0 1755
d tmp
"## };
pub const CONTENTS_CHECKSUM_V0: &str =
    "f8c5c1ad93339fd6e928aec7819de79ecec4ec8a4d0cb3565bb1d127fd7f56db";
// 1 for ostree commit, 2 for max frequency packages, 3 as empty layer
pub const LAYERS_V0_LEN: usize = 3usize;
pub const PKGS_V0_LEN: usize = 7usize;

#[derive(Debug, PartialEq, Eq)]
enum SeLabel {
    Root,
    Usr,
    UsrLibSystemd,
    Boot,
    Etc,
    EtcSystemConf,
}

impl SeLabel {
    pub fn from_path(p: &Utf8Path) -> Self {
        let rootdir = p.components().find_map(|v| {
            if let Utf8Component::Normal(name) = v {
                Some(name)
            } else {
                None
            }
        });
        let rootdir = if let Some(r) = rootdir {
            r
        } else {
            return SeLabel::Root;
        };
        if rootdir == "usr" {
            if p.as_str().contains("systemd") {
                SeLabel::UsrLibSystemd
            } else {
                SeLabel::Usr
            }
        } else if rootdir == "boot" {
            SeLabel::Boot
        } else if rootdir == "etc" {
            if p.as_str().len() % 2 == 0 {
                SeLabel::Etc
            } else {
                SeLabel::EtcSystemConf
            }
        } else {
            SeLabel::Usr
        }
    }

    pub fn to_str(&self) -> &'static str {
        match self {
            SeLabel::Root => "system_u:object_r:root_t:s0",
            SeLabel::Usr => "system_u:object_r:usr_t:s0",
            SeLabel::UsrLibSystemd => "system_u:object_r:systemd_unit_file_t:s0",
            SeLabel::Boot => "system_u:object_r:boot_t:s0",
            SeLabel::Etc => "system_u:object_r:etc_t:s0",
            SeLabel::EtcSystemConf => "system_u:object_r:system_conf_t:s0",
        }
    }

    pub fn new_xattrs(&self) -> glib::Variant {
        vec![("security.selinux".as_bytes(), self.to_str().as_bytes())].to_variant()
    }
}

/// Generate directory metadata variant for root/root 0755 directory with an optional SELinux label
pub fn create_dirmeta(path: &Utf8Path, selinux: bool) -> glib::Variant {
    let finfo = gio::FileInfo::new();
    finfo.set_attribute_uint32("unix::uid", 0);
    finfo.set_attribute_uint32("unix::gid", 0);
    finfo.set_attribute_uint32("unix::mode", libc::S_IFDIR | 0o755);
    let label = if selinux {
        Some(SeLabel::from_path(path))
    } else {
        None
    };
    let xattrs = label.map(|v| v.new_xattrs());
    ostree::create_directory_metadata(&finfo, xattrs.as_ref())
}

/// Wraps [`create_dirmeta`] and commits it.
pub fn require_dirmeta(repo: &ostree::Repo, path: &Utf8Path, selinux: bool) -> Result<String> {
    let v = create_dirmeta(path, selinux);
    let r = repo.write_metadata(
        ostree::ObjectType::DirMeta,
        None,
        &v,
        gio::Cancellable::NONE,
    )?;
    Ok(r.to_hex())
}

fn ensure_parent_dirs(
    mt: &ostree::MutableTree,
    path: &Utf8Path,
    metadata_checksum: &str,
) -> Result<ostree::MutableTree> {
    let parts = relative_path_components(path)
        .map(|s| s.as_str())
        .collect::<Vec<_>>();
    mt.ensure_parent_dirs(&parts, metadata_checksum)
        .map_err(Into::into)
}

fn relative_path_components(p: &Utf8Path) -> impl Iterator<Item = Utf8Component> {
    p.components()
        .filter(|p| matches!(p, Utf8Component::Normal(_)))
}

/// Walk over the whole filesystem, and generate mappings from content object checksums
/// to the package that owns them.  
///
/// In the future, we could compute this much more efficiently by walking that
/// instead.  But this design is currently oriented towards accepting a single ostree
/// commit as input.
fn build_mapping_recurse(
    path: &mut Utf8PathBuf,
    dir: &gio::File,
    ret: &mut ObjectMeta,
) -> Result<()> {
    use std::collections::btree_map::Entry;
    let cancellable = gio::Cancellable::NONE;
    let e = dir.enumerate_children(
        "standard::name,standard::type",
        gio::FileQueryInfoFlags::NOFOLLOW_SYMLINKS,
        cancellable,
    )?;
    for child in e {
        let childi = child?;
        let name: Utf8PathBuf = childi.name().try_into()?;
        let child = dir.child(&name);
        path.push(&name);
        match childi.file_type() {
            gio::FileType::Regular | gio::FileType::SymbolicLink => {
                let child = child.downcast::<ostree::RepoFile>().unwrap();

                let owner = OWNERS
                    .iter()
                    .find_map(|(r, owner)| {
                        if r.is_match(path.as_str()) {
                            Some(Rc::from(*owner))
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| anyhow!("Unowned path {}", path))?;

                if !ret.set.contains(&*owner) {
                    ret.set.insert(ObjectSourceMeta {
                        identifier: Rc::clone(&owner),
                        name: Rc::clone(&owner),
                        srcid: Rc::clone(&owner),
                        change_time_offset: u32::MAX,
                        change_frequency: u32::MAX,
                    });
                }

                let checksum = child.checksum().to_string();
                match ret.map.entry(checksum) {
                    Entry::Vacant(v) => {
                        v.insert(owner);
                    }
                    Entry::Occupied(v) => {
                        let prev_owner = v.get();
                        if **prev_owner != *owner {
                            anyhow::bail!(
                                "Duplicate object ownership {} ({} and {})",
                                path.as_str(),
                                prev_owner,
                                owner
                            );
                        }
                    }
                }
            }
            gio::FileType::Directory => {
                build_mapping_recurse(path, &child, ret)?;
            }
            o => anyhow::bail!("Unhandled file type: {}", o),
        }
        path.pop();
    }
    Ok(())
}

#[derive(Debug)]
pub struct Fixture {
    // Just holds a reference
    tempdir: tempfile::TempDir,
    pub dir: Arc<Dir>,
    pub path: Utf8PathBuf,
    srcrepo: ostree::Repo,
    destrepo: ostree::Repo,

    pub selinux: bool,
    pub bootable: bool,
}

impl Fixture {
    #[context("Initializing fixture")]
    pub fn new_base() -> Result<Self> {
        // Basic setup, allocate a tempdir
        let tempdir = tempfile::tempdir_in("/var/tmp")?;
        let dir = Arc::new(cap_std::fs::Dir::open_ambient_dir(
            tempdir.path(),
            cap_std::ambient_authority(),
        )?);
        let path: &Utf8Path = tempdir.path().try_into().unwrap();
        let path = path.to_path_buf();

        // Create the src/ directory
        dir.create_dir("src")?;
        let srcdir_dfd = &dir.open_dir("src")?;

        // Initialize the src/gpghome/ directory
        let gpgtarname = "gpghome.tgz";
        srcdir_dfd.write(gpgtarname, OSTREE_GPG_HOME)?;
        let gpgtar = srcdir_dfd.open(gpgtarname)?;
        srcdir_dfd.remove_file(gpgtarname)?;
        srcdir_dfd.create_dir("gpghome")?;
        let gpghome = srcdir_dfd.open_dir("gpghome")?;
        let st = std::process::Command::new("tar")
            .cwd_dir(gpghome)
            .stdin(Stdio::from(gpgtar))
            .args(["-azxf", "-"])
            .status()?;
        assert!(st.success());

        let srcrepo = ostree::Repo::create_at_dir(
            srcdir_dfd.as_fd(),
            "repo",
            ostree::RepoMode::Archive,
            None,
        )
        .context("Creating src/ repo")?;

        dir.create_dir("dest")?;
        let destrepo = ostree::Repo::create_at_dir(
            dir.as_fd(),
            "dest/repo",
            ostree::RepoMode::BareUser,
            None,
        )?;
        Ok(Self {
            tempdir,
            dir,
            path,
            srcrepo,
            destrepo,
            selinux: true,
            bootable: true,
        })
    }

    pub fn srcrepo(&self) -> &ostree::Repo {
        &self.srcrepo
    }

    pub fn destrepo(&self) -> &ostree::Repo {
        &self.destrepo
    }

    pub fn new_shell(&self) -> Result<xshell::Shell> {
        let sh = xshell::Shell::new()?;
        sh.change_dir(&self.path);
        Ok(sh)
    }

    // Delete all objects in the destrepo
    pub fn clear_destrepo(&self) -> Result<()> {
        self.destrepo()
            .set_ref_immediate(None, self.testref(), None, gio::Cancellable::NONE)?;
        for (r, _) in self.destrepo().list_refs(None, gio::Cancellable::NONE)? {
            self.destrepo()
                .set_ref_immediate(None, &r, None, gio::Cancellable::NONE)?;
        }
        self.destrepo()
            .prune(ostree::RepoPruneFlags::REFS_ONLY, 0, gio::Cancellable::NONE)?;
        Ok(())
    }

    pub fn write_filedef(&self, root: &ostree::MutableTree, def: &FileDef) -> Result<()> {
        let parent_path = def.path.parent();
        let parent = if let Some(parent_path) = parent_path {
            let meta = require_dirmeta(&self.srcrepo, parent_path, self.selinux)?;
            Some(ensure_parent_dirs(root, &def.path, meta.as_str())?)
        } else {
            None
        };
        let parent = parent.as_ref().unwrap_or(root);
        let name = def.path.file_name().expect("file name");
        let label = if self.selinux {
            Some(SeLabel::from_path(&def.path))
        } else {
            None
        };
        let xattrs = label.map(|v| v.new_xattrs());
        let xattrs = xattrs.as_ref();
        let checksum = match &def.ty {
            FileDefType::Regular(contents) => self.srcrepo.write_regfile_inline(
                None,
                0,
                0,
                libc::S_IFREG | def.mode,
                xattrs,
                contents.as_bytes(),
                gio::Cancellable::NONE,
            )?,
            FileDefType::Symlink(target) => self.srcrepo.write_symlink(
                None,
                def.uid,
                def.gid,
                xattrs,
                target.as_str(),
                gio::Cancellable::NONE,
            )?,
            FileDefType::Directory => {
                let d = parent.ensure_dir(name)?;
                let meta = require_dirmeta(&self.srcrepo, &def.path, self.selinux)?;
                d.set_metadata_checksum(meta.as_str());
                return Ok(());
            }
        };
        parent.replace_file(name, checksum.as_str())?;
        Ok(())
    }

    pub fn commit_filedefs(&self, defs: impl IntoIterator<Item = Result<FileDef>>) -> Result<()> {
        let root = ostree::MutableTree::new();
        let cancellable = gio::Cancellable::NONE;
        let tx = self.srcrepo.auto_transaction(cancellable)?;
        for def in defs {
            let def = def?;
            self.write_filedef(&root, &def)?;
        }
        let root = self.srcrepo.write_mtree(&root, cancellable)?;
        let root = root.downcast_ref::<ostree::RepoFile>().unwrap();
        // You win internet points if you understand this date reference
        let ts = chrono::DateTime::parse_from_rfc2822("Fri, 29 Aug 1997 10:30:42 PST")?.timestamp();
        // Some default metadata fixtures
        let metadata = glib::VariantDict::new(None);
        metadata.insert(
            "buildsys.checksum",
            &"41af286dc0b172ed2f1ca934fd2278de4a1192302ffa07087cea2682e7d372e3",
        );
        metadata.insert("ostree.container-cmd", &vec!["/usr/bin/bash"]);
        metadata.insert("version", &"42.0");
        #[allow(clippy::explicit_auto_deref)]
        if self.bootable {
            metadata.insert(*ostree::METADATA_KEY_BOOTABLE, &true);
        }
        let metadata = metadata.to_variant();
        let commit = self.srcrepo.write_commit_with_time(
            None,
            None,
            None,
            Some(&metadata),
            root,
            ts as u64,
            cancellable,
        )?;
        self.srcrepo
            .transaction_set_ref(None, self.testref(), Some(commit.as_str()));
        tx.commit(cancellable)?;

        // Add detached metadata so we can verify it makes it through
        let detached = glib::VariantDict::new(None);
        detached.insert("my-detached-key", &"my-detached-value");
        let detached = detached.to_variant();
        self.srcrepo.write_commit_detached_metadata(
            commit.as_str(),
            Some(&detached),
            gio::Cancellable::NONE,
        )?;

        let gpghome = self.path.join("src/gpghome");
        self.srcrepo.sign_commit(
            &commit,
            TEST_GPG_KEYID_1,
            Some(gpghome.as_str()),
            gio::Cancellable::NONE,
        )?;

        Ok(())
    }

    pub fn new_v1() -> Result<Self> {
        let r = Self::new_base()?;
        r.commit_filedefs(FileDef::iter_from(CONTENTS_V0))?;
        Ok(r)
    }

    pub fn testref(&self) -> &'static str {
        TESTREF
    }

    #[context("Updating test repo")]
    pub fn update(
        &mut self,
        additions: impl Iterator<Item = Result<FileDef>>,
        removals: impl Iterator<Item = Cow<'static, Utf8Path>>,
    ) -> Result<()> {
        let cancellable = gio::Cancellable::NONE;

        // Load our base commit
        let rev = &self.srcrepo().require_rev(self.testref())?;
        let (commit, _) = self.srcrepo.load_commit(rev)?;
        let metadata = commit.child_value(0);
        let root = ostree::MutableTree::from_commit(self.srcrepo(), rev)?;
        // Bump the commit timestamp by one day
        let ts = chrono::Utc
            .timestamp_opt(ostree::commit_get_timestamp(&commit) as i64, 0)
            .single()
            .unwrap();
        let new_ts = ts
            .add(chrono::TimeDelta::try_days(1).expect("one day does not overflow"))
            .timestamp() as u64;

        // Prepare a transaction
        let tx = self.srcrepo.auto_transaction(cancellable)?;
        for def in additions {
            let def = def?;
            self.write_filedef(&root, &def)?;
        }
        for removal in removals {
            let filename = removal
                .file_name()
                .ok_or_else(|| anyhow!("Invalid path {}", removal))?;
            // Notice that we're traversing the whole path, because that's how the walk() API works.
            let p = relative_path_components(&removal);
            let parts = p.map(|s| s.as_str()).collect::<Vec<_>>();
            let parent = &root.walk(&parts, 0)?;
            parent.remove(filename, false)?;
            self.srcrepo.write_mtree(parent, cancellable)?;
        }
        let root = self
            .srcrepo
            .write_mtree(&root, cancellable)
            .context("Writing mtree")?;
        let root = root.downcast_ref::<ostree::RepoFile>().unwrap();
        let commit = self
            .srcrepo
            .write_commit_with_time(
                Some(rev),
                None,
                None,
                Some(&metadata),
                root,
                new_ts,
                cancellable,
            )
            .context("Writing commit")?;
        self.srcrepo
            .transaction_set_ref(None, self.testref(), Some(commit.as_str()));
        tx.commit(cancellable)?;
        Ok(())
    }

    /// Gather object metadata for the current commit.
    pub fn get_object_meta(&self) -> Result<crate::objectsource::ObjectMeta> {
        let cancellable = gio::Cancellable::NONE;

        // Load our base commit
        let root = self.srcrepo.read_commit(self.testref(), cancellable)?.0;

        let mut ret = ObjectMeta::default();
        build_mapping_recurse(&mut Utf8PathBuf::from("/"), &root, &mut ret)?;

        Ok(ret)
    }

    /// Unload all in-memory data, and return the underlying temporary directory without deleting it.
    pub fn into_tempdir(self) -> tempfile::TempDir {
        self.tempdir
    }

    #[context("Exporting tar")]
    pub fn export_tar(&self) -> Result<&'static Utf8Path> {
        let cancellable = gio::Cancellable::NONE;
        let (_, rev) = self.srcrepo.read_commit(self.testref(), cancellable)?;
        let path = "exampleos-export.tar";
        let mut outf = std::io::BufWriter::new(self.dir.create(path)?);
        #[allow(clippy::needless_update)]
        let options = crate::tar::ExportOptions {
            ..Default::default()
        };
        crate::tar::export_commit(&self.srcrepo, rev.as_str(), &mut outf, Some(options))?;
        outf.flush()?;
        Ok(path.into())
    }

    /// Convert the current ref into an imported container image in the *source* repo.
    #[context("Committing as container")]
    pub async fn commit_as_container(&self) -> Result<(ImageReference, Box<LayeredImageState>)> {
        let (tmpimg, tmpdigest) = self.export_container().await?;
        let tmpimg_ostree = OstreeImageReference {
            sigverify: crate::container::SignatureSource::ContainerPolicyAllowInsecure,
            imgref: tmpimg.clone(),
        };
        let mut imp =
            store::ImageImporter::new(self.srcrepo(), &tmpimg_ostree, Default::default()).await?;
        assert!(store::query_image(self.srcrepo(), &tmpimg)
            .unwrap()
            .is_none());
        let prep = match imp.prepare().await.context("Init prep derived")? {
            store::PrepareResult::AlreadyPresent(_) => panic!("should not be already imported"),
            store::PrepareResult::Ready(r) => r,
        };
        let r = imp.import(prep).await?;
        Ok((tmpimg, r))
    }

    /// Export the current ref as a container image.
    /// This defaults to using chunking.
    #[context("Exporting container")]
    pub async fn export_container(&self) -> Result<(ImageReference, String)> {
        let name = "oci-v1";
        let container_path = &self.path.join(name);
        if container_path.exists() {
            std::fs::remove_dir_all(container_path)?;
        }
        let imgref = ImageReference {
            transport: Transport::OciDir,
            name: container_path.as_str().to_string(),
        };
        let config = Config {
            labels: Some(
                [("foo", "bar"), ("test", "value")]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            ..Default::default()
        };
        let contentmeta = self.get_object_meta().context("Computing object meta")?;
        let contentmeta = ObjectMetaSized::compute_sizes(self.srcrepo(), contentmeta)
            .context("Computing sizes")?;
        let opts = ExportOpts {
            max_layers: std::num::NonZeroU32::new(PKGS_V0_LEN as u32),
            contentmeta: Some(&contentmeta),
            ..Default::default()
        };
        let digest = crate::container::encapsulate(
            self.srcrepo(),
            self.testref(),
            &config,
            Some(opts),
            &imgref,
        )
        .await
        .context("exporting")?;
        Ok((imgref, digest))
    }
}
