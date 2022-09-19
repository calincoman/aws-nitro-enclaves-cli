// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::io::Read;

use sha2::Digest;

use oci_distribution::{client::ImageData, Reference};
use oci_spec::image::ImageConfiguration;

#[derive(Debug)]
pub enum Error {
    ImageRef(oci_distribution::ParseError),
    ImageHash(std::io::Error),
    ImageConvert(std::string::FromUtf8Error),
    FileIO(std::io::Error),
    Serde(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ImageRef(err) => write!(f, "Failed to find image reference: {:?}", err),
            Error::ImageHash(err) => write!(f, "Failed to calculate image hash: {:?}", err),
            Error::ImageConvert(err) => write!(
                f,
                "Failed to extract image details from pulled struct: {:?}",
                err
            ),
            Error::FileIO(err) => write!(f, "File read/write error: {:?}", err),
            Error::Serde(err) => write!(f, "Serialization/Deserialization error: {:?}", err),
        }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::ImageConvert(err)
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

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug)]
/// Struct representing the image metadata, like the image ID (hash) and config.
pub struct ImageDetails {
    /// The reference of an image, e.g. "docker.io/library/hello-world:latest".
    // Use a String, since the oci_distribution::Reference struct does not implement
    // Serialize/Deserialize
    pub uri: String,
    /// The image ID, calculated as the SHA256 digest hash of the image config.
    #[serde(rename = "Id")]
    pub hash: String,
    /// The image config.
    pub config: ImageConfiguration,
}

impl ImageDetails {
    pub fn new<S: AsRef<str>>(image_uri: S, image_hash: S, config: ImageConfiguration) -> Self {
        Self {
            uri: image_uri.as_ref().to_string(),
            hash: image_hash.as_ref().to_string(),
            config,
        }
    }

    /// Try to build an ImageDetails struct from an oci_distribution ImageData struct.
    //
    // The oci_distribution ImageData struct does not contain the image name or reference, so this
    // must be additionally passed to the function as well.
    pub fn from<S: AsRef<str>>(image_name: S, image_data: &ImageData) -> Result<Self, Error> {
        // Get the config JSON String from the pulled image
        let config_json = String::from_utf8(image_data.config.data.clone())?;

        // Calculate the image hash as the digest of the image config, as specified in the OCI image spec
        // https://github.com/opencontainers/image-spec/blob/main/config.md
        let image_hash = format!("{:x}", sha2::Sha256::digest(config_json.as_bytes()));

        // Calculate the image reference
        let image_ref = image_reference(&image_name)?;

        Ok(Self {
            uri: image_ref.whole(),
            hash: image_hash,
            config: deserialize_from_reader(config_json.as_bytes())?,
        })
    }
}

/// Wrapper for representing an image layer.
pub struct Layer {
    pub data: Vec<u8>,
}

impl Layer {
    pub fn new(layer_bytes: Vec<u8>) -> Self {
        Layer {
            data: layer_bytes.clone(),
        }
    }
}

/// Calculates the image ID (or image hash) as the SHA256 digest of the image config.
///
/// This method is described in the OCI image spec.
///
/// https://github.com/opencontainers/image-spec/blob/main/config.md
pub fn image_hash<R: Read>(mut image_config: R) -> Result<String, Error> {
    let mut config_bytes = Vec::new();
    image_config
        .read_to_end(&mut config_bytes)
        .map_err(Error::ImageHash)?;

    let hash = format!("{:x}", sha2::Sha256::digest(&config_bytes));

    Ok(hash)
}

/// Builds a docker image reference from the image name given as parameter.
///
/// e.g. "hello-world" image has reference "docker.io/library/hello-world:latest".
///
/// Uses the implementation from oci_distribution.
pub fn image_reference<S: AsRef<str>>(image_name: S) -> Result<Reference, Error> {
    let image_ref = image_name.as_ref().parse().map_err(Error::ImageRef)?;

    Ok(image_ref)
}

pub fn deserialize_from_reader<R: Read, T: DeserializeOwned>(
    reader: R,
) -> Result<T, serde_json::Error> {
    let deserialized_obj = serde_json::from_reader(reader)?;

    Ok(deserialized_obj)
}
