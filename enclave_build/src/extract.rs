// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use oci_distribution::client::{ImageData, ImageLayer};

use serde_json::Value;
use sha2::Digest;
use std::fmt;

/// This file contains the logic for the extraction of image data from an ImageData struct to be stored later
/// in the local cache
///
/// The ImageData struct represents the data of an image and is what the pull operation from
/// https://github.com/krustlet/oci-distribution returns

/// Errors when extracting data from an ImageData struct
#[derive(Debug, PartialEq)]
pub enum Error {
    LayerError,
    ManifestError,
    ConfigError(std::string::FromUtf8Error),
    ImageHashError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::LayerError => write!(f, "Failed to extract the layers of the image."),
            Error::ManifestError => write!(
                f,
                "Failed to extract the manifest JSON from the image data."
            ),
            Error::ConfigError(err) => write!(
                f,
                "Failed to extract the config JSON from the image data: {:?}",
                err
            ),
            Error::ImageHashError(msg) => write!(f, "{}", msg)
        }
    }
}

impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

/// Extract the layers as an array of ImageLayer structs
pub fn extract_layers(image_data: &ImageData) -> Result<Vec<ImageLayer>> {
    if image_data.layers.is_empty() {
        return Err(Error::LayerError);
    }
    Ok(image_data.layers.clone())
}

/// Extract the manifest of an image as a JSON string
pub fn extract_manifest_json(image_data: &ImageData) -> Result<String> {
    match &image_data.manifest {
        Some(image_manifest) => {
            let manifest_str = serde_json::to_string(&image_manifest)
                .map_err(|_| Error::ManifestError)?;
            Ok(manifest_str)
        }
        None => Err(Error::ManifestError),
    }
}

/// Extract the configuration file of an image as a JSON string
pub fn extract_config_json(image_data: &ImageData) -> Result<String> {
    String::from_utf8(image_data.config.data.clone()).map_err(Error::ConfigError)
}

/// Extract the image hash (digest) from an image
pub fn extract_image_hash(image_data: &ImageData) -> Result<String> {
    // Extract the config JSON from the image
    let config_json = extract_config_json(image_data)
        .map_err(|err| Error::ImageHashError(format!("{:?}", err)))?;

    // Try to parse the config JSON for the image hash
    let json_object: Value = serde_json::from_str(config_json.as_str())
        .map_err(|err| Error::ImageHashError(format!("{:?}", err)))?;

    // Get the image config    
    let config = json_object.get("config")
        .ok_or_else(|| Error::ImageHashError(format!("'config' field missing")))?;
    
    // Calculate the image hash as the digest of the image config, as specified in the OCI image spec
    // https://github.com/opencontainers/image-spec/blob/main/config.md
    let image_hash = format!("{:x}", sha2::Sha256::digest(config.to_string().as_bytes()));

    Ok(image_hash)
}

#[cfg(test)]
mod tests {
    use super::*;
    use oci_distribution::{client::Config, manifest::OciImageManifest};

    /// The manifest.json of the hello-world image used for testing
    const TEST_MANIFEST: &str = r#"
    {
        "schemaVersion":2,
        "mediaType":"application/vnd.docker.distribution.manifest.v2+json",
        "config":{
            "mediaType":"application/vnd.docker.container.image.v1+json",
            "digest":"sha256:feb5d9fea6a5e9606aa995e879d862b825965ba48de054caab5ef356dc6b3412",
            "size":1469
        },
        "layers":[
            {
                "mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip",
                "digest":"sha256:2db29710123e3e53a794f2694094b9b4338aa9ee5c40b930cb8063a1be392c54",
                "size":2479
            }
        ]
    }
    "#;

    // The config.json file of the hello-world image used for testing
    const TEST_CONFIG: &str = r##"
    {
        "architecture": "amd64",
        "config": {
            "Hostname": "",
            "Domainname": "",
            "User": "",
            "AttachStdin": false,
            "AttachStdout": false,
            "AttachStderr": false,
            "Tty": false,
            "OpenStdin": false,
            "StdinOnce": false,
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ],
            "Cmd": [
                "/hello"
            ],
            "Image": "sha256:b9935d4e8431fb1a7f0989304ec86b3329a99a25f5efdc7f09f3f8c41434ca6d",
            "Volumes": null,
            "WorkingDir": "",
            "Entrypoint": null,
            "OnBuild": null,
            "Labels": null
        },
        "container": "8746661ca3c2f215da94e6d3f7dfdcafaff5ec0b21c9aff6af3dc379a82fbc72",
        "container_config": {
            "Hostname": "8746661ca3c2",
            "Domainname": "",
            "User": "",
            "AttachStdin": false,
            "AttachStdout": false,
            "AttachStderr": false,
            "Tty": false,
            "OpenStdin": false,
            "StdinOnce": false,
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ],
            "Cmd": [
                "/bin/sh",
                "-c",
                "#(nop) ",
                "CMD [\"/hello\"]"
            ],
            "Image": "sha256:b9935d4e8431fb1a7f0989304ec86b3329a99a25f5efdc7f09f3f8c41434ca6d",
            "Volumes": null,
            "WorkingDir": "",
            "Entrypoint": null,
            "OnBuild": null,
            "Labels": {}
        },
        "created": "2021-09-23T23:47:57.442225064Z",
        "docker_version": "20.10.7",
        "history": [
            {
                "created": "2021-09-23T23:47:57.098990892Z",
                "created_by": "/bin/sh -c #(nop) COPY file:50563a97010fd7ce1ceebd1fa4f4891ac3decdf428333fb2683696f4358af6c2 in / "
            },
            {
                "created": "2021-09-23T23:47:57.442225064Z",
                "created_by": "/bin/sh -c #(nop)  CMD [\"/hello\"]",
                "empty_layer": true
            }
        ],
        "os": "linux",
        "rootfs": {
            "type": "layers",
            "diff_ids": [
                "sha256:e07ee1baac5fae6a26f30cabfe54a36d3402f96afda318fe0a96cec4ca393359"
            ]
        }
    }
    "##;

    /// ENV expressions from the hello-world image
    const TEST_ENV_EXPRESSIONS: [&'static str; 1] =
        ["PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"];

    /// CMD expressions from the hello-world image
    const TEST_CMD_EXPRESSIONS: [&'static str; 4] =
        ["/bin/sh", "-c", "#(nop)", "CMD[\\\"/hello\\\"]"];

    /// Image hash (digest) of the hello-world image
    const TEST_IMAGE_HASH: &str =
        "b9935d4e8431fb1a7f0989304ec86b3329a99a25f5efdc7f09f3f8c41434ca6d";

    /// Mock array of bytes used for testing representing an image layer
    const TEST_IMAGE_LAYER_BYTES: [u8; 100] = [0u8; 100];

    /// Builds a mock ImageData struct
    pub fn build_image_data() -> ImageData {
        // Use mock image layer bytes
        let image_layer_bytes = TEST_IMAGE_LAYER_BYTES.to_vec();
        let layer_obj = ImageLayer::new(image_layer_bytes, "".to_string(), None);

        // Use the config.json for testing
        let mut config_json = TEST_CONFIG.to_string();
        config_json = config_json.replace("\n", "");
        config_json = config_json.replace(" ", "");
        let config_obj = Config::new(config_json.as_bytes().to_vec(), "".to_string(), None);

        // Use the test manifest JSON
        let mut manifest_json = TEST_MANIFEST.to_string();
        manifest_json = manifest_json.replace("\n", "");
        manifest_json = manifest_json.replace(" ", "");
        let manifest_obj: OciImageManifest =
            serde_json::from_str(&manifest_json).expect("Manifest JSON parsing error.");

        let image_data = ImageData {
            layers: vec![layer_obj],
            digest: None,
            config: config_obj,
            manifest: Some(manifest_obj),
        };

        image_data
    }

    #[test]
    fn test_extract_layers() {
        let test_image_data = build_image_data();

        let test_layer_bytes = TEST_IMAGE_LAYER_BYTES.to_vec();

        let extracted_layers =
            extract_layers(&test_image_data).expect("Failed to extract image layers");

        // The hello-world image has only one layer, so get the first one (on index 0)
        assert_eq!(test_layer_bytes, extracted_layers.get(0).unwrap().data);
    }

    #[test]
    fn test_extract_manifest() {
        let test_image_data = build_image_data();

        let mut test_manifest = TEST_MANIFEST.to_string();
        test_manifest = test_manifest.replace("\n", "");
        test_manifest = test_manifest.replace(" ", "");

        let extracted_manifest =
            extract_manifest_json(&test_image_data).expect("Failed to extract image manifest");

        assert_eq!(test_manifest, extracted_manifest);
    }

    #[test]
    fn test_extract_config() {
        let test_image_data = build_image_data();

        let mut test_config = TEST_CONFIG.to_string();
        test_config = test_config.replace("\n", "");
        test_config = test_config.replace(" ", "");

        let extracted_config =
            extract_config_json(&test_image_data).expect("Failed to extract image config.");

        assert_eq!(test_config, extracted_config);
    }

    #[test]
    fn test_extract_image_hash() {
        let test_image_data = build_image_data();

        let test_image_hash = TEST_IMAGE_HASH.to_string();

        let extracted_image_hash =
            extract_image_hash(&test_image_data).expect("Failed to extract image hash.");

        assert_eq!(test_image_hash, extracted_image_hash);
    }
}
