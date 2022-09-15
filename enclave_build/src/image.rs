// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use serde::{
    Deserialize,
    Serialize,
    de::{DeserializeOwned}
};
use std::{
    collections::HashMap,
    convert::TryFrom,
    path::Path,
    io::{Read, BufReader, Write, BufWriter},
    fs::{OpenOptions, File}
};

use crate::{extract};

use oci_spec::image::{ImageConfiguration, ImageManifest};
use oci_distribution::{
    Reference,
    client::ImageData
};

#[derive(Debug)]
pub enum Error {
    ImageRef(oci_distribution::ParseError),
    ImageConvertExtract(extract::Error),
    FileIO(std::io::Error),
    Serde(serde_json::Error)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ImageRef(err) => write!(f, "Failed to find image reference: {:?}", err),
            Error::ImageConvertExtract(err) => write!(f, "Failed to build Image from pulled struct: {:?}", err),
            Error::FileIO(err) => write!(f, "File read/write error: {:?}", err),
            Error::Serde(err) => write!(f, "Serialization/Deserialization error: {:?}", err)
        }
    }
}

impl From<extract::Error> for Error {
    fn from(err: extract::Error) -> Self {
        Error::ImageConvertExtract(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serde(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::FileIO(err)
    }
}

impl std::error::Error for Error {}

#[derive(Clone)]
/// Struct wrapping all data associated to an image: image layers, config, manifest etc.
pub struct Image {
    /// The hash of the image, extracted from the 'Image' field of the image config.json
    hash: String,
    /// The layers of the image, stored as an array of bytes
    layers: Option<Vec<Vec<u8>>>,
    /// The image manifest
    manifest: ImageManifest,
    /// The image config
    config: ImageConfiguration,
}

impl Image {
    pub fn new<S: AsRef<str>>(
        image_hash: S,
        layers: Option<Vec<Vec<u8>>>,
        manifest: ImageManifest,
        config: ImageConfiguration,
    ) -> Self {
        Self {
            hash: image_hash.as_ref().to_string(),
            layers,
            manifest,
            config,
        }
    }

    /// Returns the image config.
    pub fn config(&self) -> &ImageConfiguration {
        &self.config
    }

    /// Builds a docker image reference from the image name given as parameter.
    ///
    /// e.g. "hello-world" image has reference "docker.io/library/hello-world:latest".
    pub fn image_reference<S: AsRef<str>>(image_name: S) -> Result<Reference, Error> {
        let image_ref = image_name
            .as_ref()
            .parse()
            .map_err(Error::ImageRef)?;

        Ok(image_ref)
    }
}

impl TryFrom<ImageData> for Image {
    type Error = self::Error;

    /// Try to build an Image struct from an oci_distribution ImageData struct.
    fn try_from(image_data: ImageData) -> Result<Self, Self::Error> {
        let image_hash = extract::extract_image_hash(&image_data)?;

        // Extract the layers as arrays of bytes
        let layers = extract::extract_layers(&image_data)?;
        let layers_data: Vec<Vec<u8>> = layers
            .into_iter()
            .map(|layer| layer.data)
            .collect();

        // Extract the manifest and config from the pulled struct as JSON Strings
        let config_json = extract::extract_config_json(&image_data)?;
        let manifest_json = extract::extract_manifest_json(&image_data)?;

        // Try to deserialize the JSON Strings into the oci_spec structs

        Ok(Self{
            hash: image_hash,
            layers: Some(layers_data),
            manifest: deserialize_from_reader(manifest_json.as_bytes())?,
            config: deserialize_from_reader(config_json.as_bytes())?
        })
    }
}

/// This is a copy of shiplift ImageDetails struct, used only for serialization.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ModelImageDetails {
    pub architecture: Option<String>,
    pub author: Option<String>,
    pub comment: Option<String>,
    pub config: Option<ModelConfig>,
    pub created: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docker_version: Option<String>,
    pub id: Option<String>,
    pub os: Option<String>,
    pub parent: Option<String>,
    pub repo_tags: Option<Vec<String>>,
    pub repo_digests: Option<Vec<String>>,
    pub size: Option<u64>,
    pub virtual_size: Option<u64>,
}

/// This is a copy of shiplift Config struct, used only for serialization.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ModelConfig {
    pub attach_stderr: Option<bool>,
    pub attach_stdin: Option<bool>,
    pub attach_stdout: Option<bool>,
    pub cmd: Option<Vec<String>>,
    pub domainname: Option<String>,
    pub entrypoint: Option<Vec<String>>,
    pub env: Option<Vec<String>>,
    pub exposed_ports: Option<HashMap<String, HashMap<String, String>>>,
    pub hostname: Option<String>,
    pub image: Option<String>,
    pub labels: Option<HashMap<String, String>>,
    pub on_build: Option<Vec<String>>,
    pub open_stdin: Option<bool>,
    pub stdin_once: Option<bool>,
    pub tty: Option<bool>,
    pub user: Option<String>,
    pub working_dir: Option<String>,
}

pub fn deserialize_from_file<P: AsRef<Path>, T: DeserializeOwned>(path: P) -> Result<T, Error> {
    let path = path.as_ref();
    let file =
        BufReader::new(File::open(path)?);
    let deserialized_obj = serde_json::from_reader(file)?;

    Ok(deserialized_obj)
}

pub fn deserialize_from_reader<R: Read, T: DeserializeOwned>(reader: R) -> Result<T, serde_json::Error> {
    let deserialized_obj = serde_json::from_reader(reader)?;

    Ok(deserialized_obj)
}

pub fn serialize_to_file<P: AsRef<Path>, T: Serialize>(
    path: P,
    object: &T,
    pretty: bool,
) -> Result<(), Error> {
    let path = path.as_ref();
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;

    let file = BufWriter::new(file);

    match pretty {
        true => serde_json::to_writer_pretty(file, object)?,
        false => serde_json::to_writer(file, object)?,
    };

    Ok(())
}

pub fn serialize_to_writer<W: Write, T: Serialize>(
    writer: &mut W,
    object: &T,
    pretty: bool,
) -> Result<(), serde_json::Error> {
    match pretty {
        true => {
            serde_json::to_writer_pretty(writer, object)?
        }
        false => serde_json::to_writer(writer, object)?,
    };

    Ok(())
}

pub fn serialize_to_string<T: Serialize>(object: &T, pretty: bool) -> Result<String, serde_json::Error> {
    Ok(match pretty {
        true => serde_json::to_string_pretty(object)?,
        false => serde_json::to_string(object)?,
    })
}
