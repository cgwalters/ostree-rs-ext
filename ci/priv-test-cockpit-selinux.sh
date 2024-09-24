#!/bin/bash
# Assumes that the current environment is a privileged container
# with the host mounted at /run/host.  We can basically write
# whatever we want, however we can't actually *reboot* the host.
set -euo pipefail

sysroot=/run/host
repo=$sysroot/ostree/repo
image=registry.gitlab.com/fedora/bootc/tests/container-fixtures/cockpit
imgref=ostree-unverified-registry:${image}

cd $(mktemp -d -p /var/tmp)

set -x

if test '!' -e "${sysroot}/ostree"; then
    ostree admin init-fs --epoch=1 "${sysroot}"
    ostree config --repo $repo set sysroot.bootloader none
fi
ostree admin stateroot-init "${stateroot}" --sysroot "${sysroot}"
ostree-ext-cli container image deploy --sysroot "${sysroot}" \
    --stateroot "${stateroot}" --imgref "${imgref}"
ref=$(ostree --repo $repo refs ostree/container/image | head -1)
commit=$(ostree --repo $repo rev-parse $rev)
ostree --repo $repo ls -X /usr/lib/systemd/system|grep -i cockpit >out.txt
if ! grep -q :cockpit_unit_file_t:s0 $out.txt; then
    echo "failed to find cockpit_unit_file_t" 1>&2
    exit 1
fi

echo ok "derived selinux"
