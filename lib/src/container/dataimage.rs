//! Implementation of "data images" for ostree, which are single layer container
//! images that can carry state to be unpacked into /etc and /var.
use anyhow::{anyhow, Result};
use cap_std_ext::cap_std::fs::Dir;
use tokio::{io::AsyncBufRead, sync::watch::Sender};

use crate::container::fetch_layer_decompress;

/// We expect this label in the image
pub(crate) const LABEL: &str = "bootc.data";
pub(crate) const LABEL_VALUE: &str = "1";

/// Given an open image reference for a "dataimage", unpack its contents.
pub async fn unpack<'a>(
    etc: &Dir,
    var: &Dir,
    imgref: &super::ImageReference,
    proxy: &mut containers_image_proxy::ImageProxy,
    img: &containers_image_proxy::OpenedImage,
    progress: Option<&'a Sender<Option<super::store::LayerProgress>>>,
) -> Result<()> {
    let manifest = proxy.fetch_manifest(img).await?.1;
    let config = proxy.fetch_config(img).await?;
    let datavalue = super::labels_of(&config)
        .and_then(|labels| labels.get(LABEL))
        .ok_or_else(|| anyhow!("Missing label {LABEL} in data image"))?;
    if datavalue != LABEL_VALUE {
        anyhow::bail!("Unexpected value of {LABEL}; expected={LABEL_VALUE} found={datavalue}");
    }
    let layers = manifest.layers();
    let layer = match layers.as_slice() {
        [] => anyhow::bail!("No layers in data image"),
        [v] => v,
        o => anyhow::bail!(
            "Expected exactly one layer in data image, found: {}",
            o.len()
        ),
    };
    let layer_info = proxy.get_layer_info(img).await?;
    let (layer_contents, driver) = fetch_layer_decompress(
        proxy,
        img,
        &manifest,
        layer,
        progress,
        layer_info.as_ref(),
        imgref.transport,
    )
    .await?;

    unpack_layer_contents(layer_contents, etc, var).await
}

async fn unpack_layer_contents(
    layer_contents: Box<dyn AsyncBufRead + Send + Unpin>,
    etc: &Dir,
    var: &Dir,
) -> Result<()> {
    todo!()
}
