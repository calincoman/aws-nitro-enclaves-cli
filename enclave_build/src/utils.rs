// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use oci_distribution::{Client, Reference};
use oci_distribution::client::{ImageLayer};
use oci_distribution::client::ClientProtocol;
use oci_distribution::client::ImageData;

use serde_json::Value;

use std::path::PathBuf;
use std::fs::File;
use std::io;
use std::error;
use std::fs;
use std::path::Path;
use std::env;
use std::io::{Write, BufWriter};

use crate::cache_manager::{CacheManager, CacheError};
use crate::constants;

#[derive(Debug, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum ExtractError {
    ImageError(String),
    ManifestError(String),
    LayerError(String),
    ConfigError(String),
    EnvCmdError(String),
    ImageHashError(String),
}

/// Builds a client which uses the protocol given as parameter
/// Client required for the https://github.com/krustlet/oci-distribution library
pub fn build_client(protocol: ClientProtocol) -> Client {
    let client_config = oci_distribution::client::ClientConfig {
        protocol,
        ..Default::default()
    };
    Client::new(client_config)
}

/// Contains the logic for the extraction of image data from an ImageData struct to be stored later
/// in the local cache
pub struct ExtractLogic {}

impl ExtractLogic {
    /// Extract the image itself (the layers) as a raw array of bytes
    pub fn extract_image(image_data: &ImageData) -> Result<Vec<u8>, ExtractError> {
        let image_bytes = image_data.clone()
            .layers
            .into_iter()
            .next()
            .map(|layer| layer.data)
            .expect("No data found.");

        match image_bytes.len() {
            0 => Err(ExtractError::ImageError("Failed to extract the image file.".to_string())),
            _ => Ok(image_bytes)
        }
    }

    /// Extract the layers as an array of ImageLayer structs
    pub fn extract_layers(image_data: &ImageData) -> Result<Vec<ImageLayer>, ExtractError> {
        match image_data.layers.len() {
            0 => Err(ExtractError::LayerError("Failed to extract the layers of the image.".to_string())),
            _ => Ok(image_data.layers.clone())
        }
    }

    /// Extract the manifest of an image as a JSON string
    pub fn extract_manifest_json(image_data: &ImageData) -> Result<String, ExtractError> {
        match &image_data.manifest {
            Some(image_manifest) => Ok(serde_json::to_string(&image_manifest).unwrap()),
            None => Err(ExtractError::ManifestError("Failed to extract the manifest from the image data.".to_string()))
        }
    }

    /// Extract the configuration file of an image as a JSON string
    pub fn extract_config_json(image_data: &ImageData) -> Result<String, ExtractError> {
        match String::from_utf8(image_data.config.data.clone()) {
            Ok(config_json) => Ok(config_json),
            Err(err) => Err(ExtractError::ConfigError(format!("Failed to extract the config JSON
                from the image data: {}", err)))
        }
    }

    /// Extract the ENV expressions from an image
    pub fn extract_env_expressions(image_data: &ImageData) -> Result<Vec<String>, ExtractError> {
        let config_string = String::from_utf8(image_data.config.data.clone())
            .map_err(|err| ExtractError::EnvCmdError(format!(
                "Failed to extract 'ENV' expressions: {:?}", err)))?;

        // Try to parse the JSON
        let json_object: Value = serde_json::from_str(config_string.as_str()).unwrap();
        let config_obj = json_object.get("container_config")
            .ok_or_else( || ExtractError::EnvCmdError(
                "'container config' field is missing in the configuration JSON.".to_string()))?;
        let env_obj = config_obj.get("Env")
            .ok_or_else(|| ExtractError::EnvCmdError(
                "'Env' field is missing in the configuration JSON.".to_string()))?;

        match env_obj.as_array() {
            None => Err(ExtractError::EnvCmdError("Failed to extract ENV expressions from image.".to_string())),
            Some(env_array) => {
                let env_strings: Vec<String> = env_array.iter().map(|json_value| json_value.to_string()).collect();
                Ok(env_strings)
            }
        }
    }

    /// Extract the CMD expressions from an image
    pub fn extract_cmd_expressions(image_data: &ImageData) -> Result<Vec<String>, ExtractError> {
        let config_string = String::from_utf8(image_data.config.data.clone())
            .map_err(|err| ExtractError::EnvCmdError(format!(
                "Failed to extract 'CMD' expressions: {:?}", err)))?;

        // Try to parse the JSON
        let json_object: Value = serde_json::from_str(config_string.as_str()).map_err(|err|
            ExtractError::EnvCmdError(format!("Failed to extract CMD expressions from an image: {:?}", err)))?;

        let config_obj = json_object.get("container_config")
            .ok_or_else(|| ExtractError::EnvCmdError(
                "'container config' field is missing in the configuration JSON.".to_string()))?;
        let cmd_obj = config_obj.get("Cmd")
            .ok_or_else(|| ExtractError::EnvCmdError(
                "'Cmd' field is missing in the configuration JSON.".to_string()))?;

        match cmd_obj.as_array() {
            None => Err(ExtractError::EnvCmdError("Failed to extract CMD expressions from image.".to_string())),
            Some(cmd_array) => {
                let cmd_strings: Vec<String> = cmd_array.iter().map(|json_value| json_value.to_string()).collect();
                Ok(cmd_strings)
            }
        }
    }

    /// Extract the image hash (digest) from an image
    pub fn extract_image_hash(image_data: &ImageData) -> Result<String, ExtractError> {
        // Extract the config JSON from the image
        let config_json = ExtractLogic::extract_config_json(image_data)
            .map_err(|err| ExtractError::ImageHashError(format!("{:?}", err)))?;

        // Try to parse the JSON for the image hash
        let json_object: Value = serde_json::from_str(config_json.as_str()).map_err(|err|
            ExtractError::EnvCmdError(format!("Failed to extract the image hash: {:?}", err)))?;

        let config_obj = json_object.get("config").ok_or_else(|| ExtractError::EnvCmdError(
            "'config' field is missing in the configuration JSON.".to_string()))?;

        let string = config_obj.get("Image").ok_or_else(|| ExtractError::EnvCmdError(
            "'Image' field is missing in the configuration JSON.".to_string()))?.to_string();

        // In the JSON, the hash is represented as "sha256: {HASH}"
        let arr: Vec<&str> = string.split(':').collect();
        
        let hash = arr.get(1).ok_or_else(|| ExtractError::EnvCmdError(
            "Failed to extract the image hash.".to_string()))?;

        let mut res = hash.to_string();
        // Eliminate the last '"' character
        res.pop();

        Ok(res)
    }
}

/// Wrapper struct which represents an image
/// 
/// reference: The image URI
/// data: The image data (layers, config etc.)
pub struct Image {
    reference: Reference,
    data: ImageData
}

impl Image {
    pub fn new(reference: Reference, data: ImageData) -> Image {
        Self { reference, data }
    }

    pub fn reference(&self) -> &Reference {
        &self.reference
    }

    pub fn data(&self) -> &ImageData {
        &self.data
    }

    /// Builds a docker image reference from the image name given as parameter
    /// The image reference struct is from https://github.com/krustlet/oci-distribution library
    pub fn build_image_reference(img_full_name: &str) -> Reference {
        img_full_name.parse().expect("Not a valid image reference")
    }

    pub fn get_image_name_from_ref(image_ref: &Reference) -> String {
        image_ref.repository().split('/').collect::<Vec<&str>>().get(1).unwrap().to_string()
    }

    /// Returns the digest hash of an image by looking first in the cache,
    /// then trying to extract it from an ImageData struct
    pub fn get_image_hash(image: &Image, cache_manager: &CacheManager) -> Result<String, ExtractError> {
        let image_hash = match cache_manager.get_image_hash(&image.reference().whole()) {
            Some(aux) => Ok(aux),
            None => match ExtractLogic::extract_image_hash(image.data()) {
                Ok(aux) => Ok(aux),
                Err(err) => Err(ExtractError::ImageHashError(format!("{:?}", err)))
            }
        };

        image_hash.map_err(|err| ExtractError::ImageHashError(format!("{:?}", err)))
    }
}

pub struct CachePath {}

impl CachePath {
    /// Returns the default root folder path of the cache
    pub fn get_default_cache_root_folder() -> Result<PathBuf, CacheError> {
        // For testing, the cache will be saved to the local directory
        if constants::TEST_MODE_ENABLED {
            return Ok(PathBuf::from("./test/container_cache/"));
        }

        // Use XDG_DATA_HOME as root
        let root = match env::var_os(constants::CACHE_ROOT_FOLDER) {
            Some(val) => val.into_string()
                .map_err(|err| CacheError::PathError(format!(
                    "Failed to determine the cache root folder: {:?}", err)))?,
            // If XDG_DATA_HOME is not set, use $HOME/.local/share as specified in
            // https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
            None => {
                let home_folder = env::var_os("HOME")
                    .ok_or_else(|| CacheError::PathError(
                        "Failed to determine the cache root folder: HOME environment variable is not set.".to_string()))?;
                format!("{}/.local/share/", home_folder.into_string()
                    .map_err(|err| CacheError::PathError(format!("{:?}", err)))?)
            }
        };

        let mut path = PathBuf::from(root);
        path.push(".nitro_cli/container_cache");

        Ok(path)
    }


    /// Returns the default path to the cache root folder of an image
    /// 
    /// e.g. {ROOT}/.nitro_cli/container_cache/{IMAGE_HASH}
    pub fn get_default_image_folder_path(image: &Image, cache_manager: &CacheManager) -> Result<PathBuf, CacheError> {
        // Get the image hash
        let hash = Image::get_image_hash(image, cache_manager).map_err(|err|
            CacheError::PathError(format!("Failed to determine the image folder path: {:?}", err)))?;

        // Get the cache root folder
        let mut cache_root = CachePath::get_default_cache_root_folder()
            .map_err(|err| CacheError::PathError(format!(
                "Failed to determine the image folder path: {:?}", err)))?;

        cache_root.push(hash);

        Ok(cache_root)
    }

    /// Determines the path of the image cache folder considering the cache as the path
    /// given as parameter
    pub fn get_custom_image_folder_path(image: &Image, cache_path: &PathBuf) -> Result<PathBuf, CacheError> {
        let mut image_folder_path = cache_path.clone();
        // Build the image folder path by appending the image hash at the end of the provided path
        image_folder_path.push(ExtractLogic::extract_image_hash(&image.data())
            .map_err(|err| CacheError::PathError(format!(
                "Failed to determine the image cache folder: {:?}", err)))?);

        Ok(image_folder_path)
    }

    /// Creates all folders from the path of the cache given as parameter
    pub fn create_cache_folder_path(cache_path: &PathBuf) -> Result<(), CacheError> {
        fs::create_dir_all(cache_path)
            .map_err(|err| CacheError::PathError(format!(
                "Failed to create the cache folder path: {:?}", err)))
    }

    /// Creates the cache folder where the image data will be stored (the folder is created
    /// in the directory given as parameter)
    /// 
    /// The function also creates all folders from the path that are not already created
    pub fn create_image_folder(image: &Image, cache_path: &PathBuf) -> Result<(), CacheError> {
        let image_folder_path = CachePath::get_custom_image_folder_path(image, cache_path).map_err(|err|
            CacheError::PathError(format!("Failed to create the image cache folder: {:?}", err)))?;

        fs::create_dir_all(image_folder_path).map_err(|err|
            CacheError::PathError(format!("Failed to create the image cache folder: {:?}", err)))
    }
}
