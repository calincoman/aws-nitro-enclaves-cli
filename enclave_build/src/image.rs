// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use std::collections::HashMap;

use crate::extract;

#[derive(Debug, PartialEq)]
pub enum Error {
    ImageRefError(oci_distribution::ParseError),
    ImageBuildError(String),
    ImageConvertError(extract::Error)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ImageRefError(err) => write!(f, "Conversion failed: {:?}", err),
            Error::ImageBuildError(msg) => write!(f, "Image struct creation failed: {}", msg),
            Error::ImageConvertError(err) => write!(f, "Failed to convert from pulled struct: {:?}", err)
        }
    }
}

impl From<extract::Error> for Error {
    fn from(err: extract::Error) -> Self {
        Error::ImageConvertError(err)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

// #[derive(Clone)]
/// This struct contains the image data, with the manifest and config as raw JSON strings.
/// 
/// This is the struct fetched from the cache, and later deserialized in order to create
/// an Image struct.
pub struct ImageCacheFetch {
    hash: String,

    /// The image layers are an array of raw byte arrays
    layers: Vec<Vec<u8>>,

    /// The image manifest, represented as a JSON string
    manifest: String,

    /// The image config, represented as a JSON string
    config: String,
}

impl ImageCacheFetch {
    pub fn new<S: AsRef<str>>(
        image_hash: S,
        layers: Vec<Vec<u8>>,
        manifest: String,
        config: String
    ) -> Self {
        Self {
            hash: image_hash.as_ref().to_string(),
            layers,
            manifest,
            config
        }
    }

    pub fn from(image_data: oci_distribution::client::ImageData) -> Result<Self> {
        let image_hash = extract::extract_image_hash(&image_data)?;

        let pulled_layers = extract::extract_layers(&image_data)?;
        let layers: Vec<Vec<u8>> = pulled_layers
            .into_iter()
            .map(|layer| layer.data)
            .collect();

        let pulled_config = extract::extract_config_json(&image_data)?;
        let pulled_manifest = extract::extract_manifest_json(&image_data)?;

        Ok(
            Self {
                hash: image_hash,
                layers: layers,
                manifest: pulled_manifest,
                config: pulled_config
            }
        )
    }
}

#[derive(Clone)]
/// Struct wrapping all data associated to an image: image layers, config, manifest etc.
pub struct Image {
    /// The simple name of the image, e.g. hello-world, ubuntu etc.
    name: String,
    /// The hash of an image, extracted from the 'Image' field of the image config.json
    hash: String,
    /// The reference of an image, e.g. "docker.io/library/hello-world:latest" for hello-world image
    uri: oci_distribution::Reference,

    /// The layers of the image, stored in an array
    layers: Vec<oci_distribution::client::ImageLayer>,
    /// The image manifest
    manifest: ImageManifest,
    /// The image config
    config: ImageConfig,
}

impl Image {
    pub fn new<S: AsRef<str>>(
        image_name: S,
        layers: Vec<oci_distribution::client::ImageLayer>,
        manifest: ImageManifest,
        config: ImageConfig,
    ) -> Result<Self> {
        // Get the image hash from the config
        let image_hash = config
            .image_hash()
            .ok_or_else(|| {
                Error::ImageBuildError("Image hash not found in config object.".to_string())
            })?
            .to_string();

        // Build the reference from the image name
        let image_uri = Image::image_reference(&image_name)
            .map_err(|err| Error::ImageBuildError(format!("{:?}", err)))?;

        Ok(Self {
            name: image_name.as_ref().to_string(),
            hash: image_hash,
            uri: image_uri,
            layers,
            manifest,
            config,
        })
    }

    /// Returns the image name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the image hash
    pub fn hash(&self) -> &str {
        &self.hash
    }

    /// Returns the image URI (image reference)
    pub fn uri(&self) -> &oci_distribution::Reference {
        &self.uri
    }

    /// Builds a docker image reference from the image name given as parameter.
    ///
    /// e.g. "hello-world" image has reference "docker.io/library/hello-world:latest"
    pub fn image_reference<S: AsRef<str>>(image_name: S) -> Result<oci_distribution::Reference> {
        let image_ref = image_name
            .as_ref()
            .parse()
            .map_err(|err| Error::ImageRefError(err))?;

        Ok(image_ref)
    }
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
/// The image manifest describes a configuration and set of layers for a single
/// container image for a specific architecture and operating system.
pub struct ImageManifest {
    schema_version: u32,
    media_type: Option<String>,

    /// A reference to the image config
    config: Descriptor,

    /// Array describing the image layers
    layers: Vec<Descriptor>,
    annotations: Option<HashMap<String, String>>,
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
/// The image configuration data, containing metadata of an image such as date created, author,
/// architecture, as well as runtime data such as bash commands and default arguments.
pub struct ImageConfig {
    created: Option<String>,

    author: Option<String>,

    architecture: Option<String>,

    os: Option<String>,

    #[serde(rename = "os.version")]
    os_version: Option<String>,

    #[serde(rename = "os.features")]
    os_features: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<Config>,

    /// Same structure as 'config' field, but usually this contains more detailed data
    #[serde(skip_serializing_if = "Option::is_none")]
    container_config: Option<Config>,

    #[serde(skip_serializing_if = "Option::is_none")]
    variant: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    rootfs: Option<RootFs>,

    history: Vec<LayerHistory>,
}

impl ImageConfig {
    /// Returns the image hash
    pub fn image_hash(&self) -> Option<&str> {
        match &self.config {
            Some(config) => config.image_hash(),
            None => None,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "PascalCase")]
/// The execution parameters which should be used when running a container from
/// the image.
pub struct Config {
    #[serde(rename = "Hostname")]
    host_name: Option<String>,

    #[serde(rename = "Domainname")]
    domain_name: Option<String>,

    attach_stdin: Option<bool>,
    attach_stdout: Option<bool>,
    attach_stderr: Option<bool>,
    tty: Option<bool>,
    open_stdin: Option<bool>,
    stdin_once: Option<bool>,

    user: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    exposed_ports: Option<Vec<String>>,

    image: Option<String>,

    env: Option<Vec<String>>,
    entrypoint: Option<Vec<String>>,
    cmd: Option<Vec<String>>,
    volumes: Option<Vec<String>>,
    working_dir: Option<String>,
    on_build: Option<Vec<String>>,
    labels: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    stop_signal: Option<String>,
}

impl Config {
    /// Returns the image hash.
    pub fn image_hash(&self) -> Option<&str> {
        match &self.image {
            Some(hash_with_prefix) =>
                // By default, the image hash is represented as "<hash_alg>:<hash_string>", so
                // remove the <hash_alg> prefix to get the actual hash. The hash algorithm
                // used is assumed to be sha256.
                match hash_with_prefix.strip_prefix("sha256:") {
                    Some(hash) => Some(hash),
                    None => None
                },
            None => None
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
/// References the layer content addresses used by the image.
pub struct RootFs {
    #[serde(rename = "type")]
    type_field: String,

    /// An array of layer content digest hashes.
    diff_ids: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
/// Describes the history and metadata of a layer.
pub struct LayerHistory {
    #[serde(skip_serializing_if = "Option::is_none")]
    created: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    author: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    created_by: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    comment: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    empty_layer: Option<bool>,
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
/// A Descriptor provides information about a specific content. It includes the type of the content,
/// a content identifier (digest), and the raw byte-size of the content.
pub struct Descriptor {
    #[serde(rename = "mediaType")]
    media_type: String,
    digest: String,
    size: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    urls: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    annotations: Option<HashMap<String, String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    platform: Option<Platform>,
}

#[derive(Clone, Serialize, Deserialize, Eq, PartialEq)]
/// Describes the minimum runtime requirements of the image.
pub struct Platform {
    architecture: String,
    os: String,

    #[serde(rename = "os.version")]
    #[serde(skip_serializing_if = "Option::is_none")]
    os_version: Option<String>,

    #[serde(rename = "os.features")]
    #[serde(skip_serializing_if = "Option::is_none")]
    os_features: Option<Vec<String>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    variant: Option<String>,
}
