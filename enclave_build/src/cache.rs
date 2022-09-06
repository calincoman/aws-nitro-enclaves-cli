// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use oci_distribution::{Reference};
use oci_distribution::client::{ImageData, ImageLayer, Client, ClientProtocol};
use oci_distribution::ParseError;

use crate::cache_manager::{self, CacheManager, Image};
use crate::extract::{self, ExtractError};
use crate::constants::{self, CACHE_CONFIG_FILE_NAME, CACHE_MANIFEST_FILE_NAME};

use std::{
    io::{Read, Write, BufRead, BufReader, BufWriter},
    fs::{self, File},
    path::Path,
    fmt
};

#[derive(Debug, PartialEq)]
pub enum CacheManagerError {
    PathError(String),
    StoreError(String),
    RetrieveError(String),
}

#[derive(Debug, PartialEq)]
pub enum GlobalError {
    ConvertError(String)
}

#[derive(Debug, PartialEq)]
pub enum StoreError {
    ImageError(String),
    ImageLayersError(String),
    ConfigError(String),
    ManifestError(String),
    LayerError(String),
    EnvCmdError(String),
    IndexJsonError(String)
}

#[derive(Debug, PartialEq)]
pub enum FetchError {
    ImageError(String),
    LayerError(String),
    ConfigError(String),
    ManifestError(String),
    EnvCmdError(String),
    HashError(String),
}

impl fmt::Display for GlobalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GlobalError::ConvertError(msg) =>
                write!(f, "Conversion failed: {}", msg)
        }
    }
}

pub type StoreResult<T> = std::result::Result<T, StoreError>;
pub type FetchResult<T> = std::result::Result<T, FetchError>;

/// Builds a client which uses the protocol given as parameter
pub fn build_client(protocol: ClientProtocol) -> Client {
    let client_config = oci_distribution::client::ClientConfig {
        protocol,
        ..Default::default()
    };
    Client::new(client_config)
}

/// Trait for the cache store operation
/// 
/// Provides the 'store' method to be implemented
pub trait CacheStore {
    /// Stores the implementing object at the cache folder from cache_path
    /// 
    /// e.g. If ImageConfig is implementing this trait, then the config.json file will be stored at
    /// {cache_path}/config.json
    fn store<P: AsRef<Path>>(&self, cache_path: P) -> StoreResult<()>;
}

/// Trait for the cache fetch operation
/// 
/// Provides the 'fetch' method to be implemented
pub trait CacheFetch<T> {
    /// Fecthes the implementing object from cache_path in the cache
    /// 
    /// e.g. {cache_path} should be the path to an image cache folder containing the config and manifest files,
    /// layers etc.
    fn fetch<P: AsRef<Path>>(cache_path: P) -> FetchResult<T>;
}

pub struct ImageContents(Vec<u8>);

impl ImageContents {
    pub fn new(bytes: Vec<u8>) -> Self {
        ImageContents(bytes)
    }

    /// Get the bytes of an image (all layers combined)
    pub fn bytes(&self) -> &Vec<u8> {
        &self.0
    }

    pub fn from_image(image: &Image) -> Result<Self, GlobalError> {
        let image_contents = extract::extract_image(image.data())
            .map_err(|err| GlobalError::ConvertError(err.to_string()))?;
        
        Ok(ImageContents(image_contents))
    }
}

pub struct ImageLayers(Vec<ImageLayer>);

impl ImageLayers {
    pub fn new(image_layers: Vec<ImageLayer>) -> Self {
        ImageLayers(image_layers)
    }

    /// Get the layers of an image
    pub fn layers(&self) -> &Vec<ImageLayer> {
        &self.0
    }

    pub fn from_image(image: &Image) -> Result<Self, GlobalError> {
        let image_layers = extract::extract_layers(image.data())
            .map_err(|err| GlobalError::ConvertError(err.to_string()))?;

        Ok(ImageLayers(image_layers))
    }
}

pub struct ImageConfig(String);

impl ImageConfig {
    pub fn new(config_str: String) -> Self {
        ImageConfig(config_str)
    }

    /// Get the config data as a String
    pub fn config(&self) -> &String {
        &self.0
    }

    pub fn from_image(image: &Image) -> Result<Self, GlobalError> {
        let image_config = extract::extract_config_json(image.data())
            .map_err(|err| GlobalError::ConvertError(err.to_string()))?;

        Ok(ImageConfig(image_config))
    }
}

pub struct ImageManifest(String);

impl ImageManifest {
    pub fn new(manifest_str: String) -> Self {
        ImageManifest(manifest_str)
    }

    /// Get the manifest data as a String
    pub fn manifest(&self) -> &String {
        &self.0
    }

    pub fn from_image(image: &Image) -> Result<Self, GlobalError> {
        let image_manifest = extract::extract_manifest_json(image.data())
            .map_err(|err| GlobalError::ConvertError(err.to_string()))?;

        Ok(ImageManifest(image_manifest))
    }
}

pub struct EnvExpressions(Vec<String>);

impl EnvExpressions {
    pub fn new(env_expressions: Vec<String>) -> Self {
        EnvExpressions(env_expressions)
    }

    /// Get the ENV expressions
    pub fn env_expressions(&self) -> &Vec<String> {
        &self.0
    }

    pub fn from_image(image: &Image) -> Result<Self, GlobalError> {
        let env_expressions = extract::extract_env_expressions(image.data())
            .map_err(|err| GlobalError::ConvertError(err.to_string()))?;

        Ok(EnvExpressions(env_expressions))
    }
}

pub struct CmdExpressions(Vec<String>);

impl CmdExpressions {
    pub fn new(cmd_expressions: Vec<String>) -> Self {
        CmdExpressions(cmd_expressions)
    }

    /// Get the CMD expressions
    pub fn cmd_expressions(&self) -> &Vec<String> {
        &self.0
    }

    pub fn from_image(image: &Image) -> Result<Self, GlobalError> {
        let cmd_expressions = extract::extract_cmd_expressions(image.data())
            .map_err(|err| GlobalError::ConvertError(err.to_string()))?;

        Ok(CmdExpressions(cmd_expressions))
    }
}

impl CacheStore for ImageContents {
    fn store<P: AsRef<Path>>(&self, cache_path: P) -> StoreResult<()> {
        // Build the path where the image file will be stored
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::CACHE_IMAGE_FILE_NAME);
        
        // Create the cache image file (the directories in the path should already be created)
        // and write the image content bytes to it
        File::create(&path)
            .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?
            .write_all(&self.bytes())
                .map_err(|err| StoreError::ImageError(format!(
                    "Image file could not be written to cache: {:?}", err)))
    }
}

impl CacheStore for ImageLayers {
    fn store<P: AsRef<Path>>(&self, cache_path: P) -> StoreResult<()> {
        // Build the path where the image layers will be stored
        let mut path = cache_path.as_ref().to_path_buf();
        // Add the 'layers' directory to the path
        path.push("layers");

        // Create the 'layers' directory
        fs::create_dir_all(&path).map_err(|err| StoreError::ImageLayersError(format!(
            "Failed to create the folder for storing the image layers: {:?}", err)))?;
        
        // Iterate through the layers and for each layer, store it in a tar file in the cache
        for (index, layer) in self.layers().iter().enumerate() {
            // Build the path of the layer tar file
            let mut layer_path = path.clone();
            layer_path.push(layer.sha256_digest().strip_prefix("sha256:").unwrap());

            // Create the cache file containing the layer and write the layer bytes to it
            File::create(&layer_path)
                .map_err(|err| StoreError::ImageLayersError(format!(
                    "Failed to create an image layer cache file: {:?}", err)))?
                .write_all(&layer.data)
                    .map_err(|err| StoreError::ImageLayersError(format!(
                        "Failed to write layer to cache file for layer {} with digest {}: {:?}",
                        index, layer.sha256_digest().strip_prefix("sha256:").unwrap(), err)))?;
        }

        Ok(())
    }
}

impl CacheStore for ImageConfig {
    fn store<P: AsRef<Path>>(&self, cache_path: P) -> StoreResult<()> {
        // Build the path where the config.json will be stored
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(CACHE_CONFIG_FILE_NAME);

        // Create the cache config.json file (the directories in the path should already be created)
        // and write the config data to it
        File::create(&path)
            .map_err(|err| StoreError::ConfigError(format!(
                "Failed to create the configuration cache file: {:?}", err)))?
            .write_all(&self.config().as_bytes())
                .map_err(|err| StoreError::ConfigError(format!(
                    "Configuration JSON could not be written to cache: {:?}", err)))
    }
}

impl CacheStore for ImageManifest {
    fn store<P: AsRef<Path>>(&self, cache_path: P) -> StoreResult<()> {
        // Build the path where the manifest.json will be stored
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(CACHE_MANIFEST_FILE_NAME);

        // Create the cache manifest.json file (the directories in the path should already be created)
        // and write the manifest data to it
        File::create(&path)
            .map_err(|err| StoreError::ManifestError(format!(
                "Failed to create the manifest cache file: {:?}", err)))?
            .write_all(&self.manifest().as_bytes())
                .map_err(|err| StoreError::ManifestError(format!(
                    "Manifest file could not be written to cache: {:?}", err)))
    }
}

impl CacheStore for EnvExpressions {
    fn store<P: AsRef<Path>>(&self, cache_path: P) -> StoreResult<()> {
        // Build the path where the file containing the ENV expressions will be stored
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::ENV_CACHE_FILE_NAME);

        let env_file = File::create(&path).map_err(|err|
            StoreError::EnvCmdError(format!("Failed to create the file to store ENV expressions: {:?}", err)))?;

        // Use a BufWriter to write to the cache file, one expression on every new line
        let mut writer = BufWriter::new(&env_file);

        // Iterate through the ENV expressions and write each one of them on a new line
        self.env_expressions().iter()
            .try_for_each(|expr| writeln!(&mut writer, "{}", expr)
                .map_err(|err| StoreError::EnvCmdError(format!(
                    "Failed to write {} expression to the output cache file: {}", "ENV", err))))?;

        Ok(())
    }
}

impl CacheStore for CmdExpressions {
    fn store<P: AsRef<Path>>(&self, cache_path: P) -> StoreResult<()> {
        // Build the path where the file containing the CMD expressions will be stored
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::CMD_CACHE_FILE_NAME);

        let cmd_file = File::create(&path).map_err(|err|
            StoreError::EnvCmdError(format!("Failed to create the file to store CMD expressions: {:?}", err)))?;

        // Use a BufWriter to write to the cache file, one expression on every new line
        let mut writer = BufWriter::new(&cmd_file);

        // Iterate through the CMD expressions and write each one of them on a new line
        self.cmd_expressions().iter()
            .try_for_each(|expr| writeln!(&mut writer, "{}", expr)
                .map_err(|err| StoreError::EnvCmdError(format!(
                    "Failed to write {} expression to the output cache file: {}", "CMD", err))))?;

        Ok(())
    }
}

impl CacheFetch<Vec<u8>> for ImageContents {
    fn fetch<P: AsRef<Path>>(cache_path: P) -> FetchResult<Vec<u8>> {
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::CACHE_IMAGE_FILE_NAME);

        let mut image_contents = Vec::new();

        // Open the cached image file and read the bytes from it
       let bytes_read = File::open(&path)
            .map_err(|err|
                FetchError::ImageError(format!("Failed to open the cached image file: {:?}", err)))?
            .read_to_end(&mut image_contents)
                .map_err(|err| FetchError::ImageError(format!(
                    "Failed to read from cached image file: {:?}", err)))?;

        // If no bytes were read, throw an error
        if bytes_read == 0 {
            return Err(FetchError::ImageError("No data was read from the cached image file".to_string()));
        }

        Ok(image_contents)
    }
}

impl CacheFetch<Vec<Vec<u8>>> for ImageLayers {
    fn fetch<P: AsRef<Path>>(cache_path: P) -> FetchResult<Vec<Vec<u8>>> {
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::CACHE_LAYERS_FOLDER_NAME);

        // Get all the files from the layers folder of an image
        let layer_files = fs::read_dir(&path).map_err(|err|
            FetchError::LayerError("Failed to read cached layer files".to_string()))?;

        let mut layers = Vec::new();

        // Iterate through the found layer files
        for (index, layer_file) in layer_files.into_iter().enumerate() {
            match layer_file {
                Ok(file) => {
                    let mut buffer = Vec::new();

                    // Open the cached layer file and read the bytes from it
                    let bytes_read = File::open(file.path())
                        .map_err(|err|
                            FetchError::LayerError(format!(
                                "Failed to open the layer file with index {}: {:?}", index.to_string(), err)))?
                        .read_to_end(&mut buffer)
                            .map_err(|err| FetchError::LayerError(format!(
                                "Failed to read layer file with index {}: {:?}", index.to_string(), err)))?;

                    // If no bytes were read, throw an error
                    if bytes_read == 0 {
                        return Err(FetchError::LayerError(format!("No data was read from layer file.")));
                    }
                    layers.push(buffer);
                }
                Err(_) => {
                    return Err(FetchError::LayerError(format!("Layer file could not be read.")));
                }
            }
        }

        Ok(layers)
    }
}

impl CacheFetch<String> for ImageConfig {
    fn fetch<P: AsRef<Path>>(cache_path: P) -> FetchResult<String> {
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::CACHE_CONFIG_FILE_NAME);

        let mut config_str = String::new();

        // Open the cached config file and read the bytes from it
        let bytes_read = File::open(&path)
            .map_err(|err|
                FetchError::ConfigError(format!("Failed to open the cached config file: {:?}", err)))?
            .read_to_string(&mut config_str)
                .map_err(|err| FetchError::ConfigError(format!(
                    "Failed to read from cached config file: {:?}", err)))?;

        // If no bytes were read, throw an error
        if bytes_read == 0 {
            return Err(FetchError::ConfigError("No data was read from the cached config file".to_string()));
        }

        Ok(config_str)
    }
}

impl CacheFetch<String> for ImageManifest {
    fn fetch<P: AsRef<Path>>(cache_path: P) -> FetchResult<String> {
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::CACHE_MANIFEST_FILE_NAME);

        let mut manifest_str = String::new();

        // Open the cached manifest file and read the bytes from it
        let bytes_read = File::open(&path)
            .map_err(|err|
                FetchError::ManifestError(format!("Failed to open the cached manifest file: {:?}", err)))?
            .read_to_string(&mut manifest_str)
                .map_err(|err| FetchError::ManifestError(format!(
                    "Failed to read from cached manifest file: {:?}", err)))?;

        // If no bytes were read, throw an error
        if bytes_read == 0 {
            return Err(FetchError::ManifestError("No data was read from the cached manifest file.".to_string()));
        }

        Ok(manifest_str)
    }
}

impl CacheFetch<Vec<String>> for EnvExpressions {
    fn fetch<P: AsRef<Path>>(cache_path: P) -> FetchResult<Vec<String>> {
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::ENV_CACHE_FILE_NAME);

        // Open the env.sh file
        let file = File::open(&path).map_err(|err|
            FetchError::EnvCmdError(format!(
                "Failed to open the cached file containing 'ENV' expressions: {:?}", err)))?;

        let mut expr = Vec::new();

        // Use a BufReader to read the file line by line
        let reader = BufReader::new(file);

        // Go through each line and add it to the result
        for line in reader.lines() {
            match line {
                Ok(val) => {
                    expr.push(val);
                }
                Err(err) => {
                    return Err(FetchError::EnvCmdError(format!(
                        "Failed to read 'ENV' expression from cached file: {:?}", err)));
                }
            }
        }

        Ok(expr)
    }
}

impl CacheFetch<Vec<String>> for CmdExpressions {
    fn fetch<P: AsRef<Path>>(cache_path: P) -> FetchResult<Vec<String>> {
        let mut path = cache_path.as_ref().to_path_buf();
        path.push(constants::CMD_CACHE_FILE_NAME);

        // Open the cmd.sh file
        let file = File::open(&path).map_err(|err|
            FetchError::EnvCmdError(format!(
                "Failed to open the cached file containing 'CMD' expressions: {:?}", err)))?;

        let mut expr = Vec::new();

        // Use a BufReader to read the file line by line
        let reader = BufReader::new(file);

        // Go through each line and add it to the result
        for line in reader.lines() {
            match line {
                Ok(val) => {
                    expr.push(val);
                }
                Err(err) => {
                    return Err(FetchError::EnvCmdError(format!(
                        "Failed to read 'CMD' expression from cached file: {:?}", err)));
                }
            }
        }

        Ok(expr)
    }
}

// Stores the image data in the cache at the path provided as parameter
pub fn store_image_data<P: AsRef<Path>>(image: &Image, path: P) -> StoreResult<()> {
    let image_contents = ImageContents::from_image(&image)
        .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?;
    let image_layers = ImageLayers::from_image(&image)
        .map_err(|err| StoreError::ImageLayersError(format!("{:?}", err)))?;
    let image_config = ImageConfig::from_image(&image)
        .map_err(|err| StoreError::ConfigError(format!("{:?}", err)))?;
    let image_manifest = ImageManifest::from_image(&image)
        .map_err(|err| StoreError::ManifestError(format!("{:?}", err)))?;
    let env = EnvExpressions::from_image(&image)
        .map_err(|err| StoreError::EnvCmdError(format!("{:?}", err)))?;
    let cmd = CmdExpressions::from_image(&image)
        .map_err(|err| StoreError::EnvCmdError(format!("{:?}", err)))?;

    image_contents.store(&path)?;
    image_layers.store(&path)?;
    image_config.store(&path)?;
    image_manifest.store(&path)?;
    env.store(&path)?;
    cmd.store(&path)?;

    Ok(())
}

/// This module pulls an image from the docker registry and tests that the cache store / fetch works
/// THE SETUP_CACHE FUNCTION SHOULD BE RUN FIRST TO PULL AND STORE THE IMAGE
#[cfg(test)]
mod tests {
    use super::*;
    use oci_distribution::secrets::RegistryAuth;

    #[derive(Debug, PartialEq)]
    pub enum TestError {
        ImageCacheError(String),
        ImagePullError(String),
    }

    /// Name of the image to be pulled and cached
    const TEST_IMAGE_NAME: &str = "hello-world";

    pub fn create_test_cache_manager() -> CacheManager {
        // Get the root folder of the cache
        let cache_folder_path = CacheManager::get_default_cache_root_path()
            .expect("get cache root path");

        // Create the CacheManager
        let cache_manager = CacheManager::new(&cache_folder_path)
            .create_cache().expect("create cache folders")
            .create_index_file().expect("create index.json file")
            .populate_hashmap().expect("read index.json file");

        cache_manager
    }

    /// Used for setting up a test
    pub async fn setup_test() -> (Image, CacheManager) {
        // Name of the image used for testing
        let image_name = TEST_IMAGE_NAME.to_string();
        let image_ref = Image::build_image_reference(&image_name)
            .expect("Failed to build image reference from image name.");

        let cache_manager = create_test_cache_manager();

        // Download the image data from the remote registry
        let image_data = crate::pull::pull_image_data(&image_name)
            .await
            .expect("pull image");

        (Image::new(image_ref.clone(), image_data.clone()), cache_manager)
    }

    /// This should be run first to pull the image and store it to the cache so the other tests do not fail
    /// 
    /// The function pulls the TEST_IMAGE_NAME image
    #[tokio::test]
    async fn setup_test_cache() -> Result<(), TestError> {
        // Name of the image to be used for testing
        let image_name = TEST_IMAGE_NAME.to_string();

        let mut cache_manager = create_test_cache_manager();

        cache_manager.create_cache_folders();
        
        cache_manager.cache_image(&image_name)
            .await
            .map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        Ok(())
    }

    #[test]
    fn test_validate_layers() {
        let cache_manager = create_test_cache_manager();

        let test_image_digest = "b9935d4e8431fb1a7f0989304ec86b3329a99a25f5efdc7f09f3f8c41434ca6d".to_string();

        let res = cache_manager.validate_layers(&test_image_digest);

        assert_eq!(res.is_err(), false);
    }

    #[tokio::test]
    async fn test_validate_manifest() {
        let cache_manager = create_test_cache_manager();

        let test_image_digest = "b9935d4e8431fb1a7f0989304ec86b3329a99a25f5efdc7f09f3f8c41434ca6d".to_string();

        let image_ref = Image::build_image_reference(&TEST_IMAGE_NAME.to_string())
            .expect("get image reference");

        let (pulled_manifest, _) = crate::pull::pull_manifest(&image_ref)
            .await
            .expect("pull manifest");
        let pulled_manifest_str = serde_json::to_string(&pulled_manifest)
            .expect("serialize manifest");

        let val = cache_manager.validate_manifest(
            &test_image_digest,
            &pulled_manifest_str
        );
            
        assert_eq!(val.is_err(), false);
    }

    #[test]
    fn test_validate_config() {
        let cache_manager = create_test_cache_manager();

        let test_image_digest = "b9935d4e8431fb1a7f0989304ec86b3329a99a25f5efdc7f09f3f8c41434ca6d".to_string();

        let val = cache_manager.validate_config(&test_image_digest);

        assert_eq!(val.is_err(), false);
    }

    #[tokio::test]
    async fn test_cached_image() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let image_hash = image.get_image_hash().expect("extract image hash");
        let cached_image_bytes = ImageContents::fetch(
            cache_manager.get_image_folder_path(&image_hash)
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let image_bytes = extract::extract_image(image.data()).unwrap();
        assert_eq!(cached_image_bytes, image_bytes);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_layers() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let image_hash = image.get_image_hash().expect("extract image hash");
        let cached_layers = ImageLayers::fetch(
            cache_manager.get_image_folder_path(&image_hash)
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let layers = extract::extract_layers(image.data()).unwrap();

        for (cached_layer, layer) in cached_layers.iter().zip(layers.iter()) {
            assert_eq!(*cached_layer, layer.data);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_config() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let image_hash = image.get_image_hash().expect("extract image hash");
        let cached_config = ImageConfig::fetch(
            cache_manager.get_image_folder_path(&image_hash)
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let config_str = extract::extract_config_json(image.data()).unwrap();

        // Create JSON Values from the strings in order to ignore the whitespaces from the cached config file
        let cached_config_val: serde_json::Value = serde_json::from_str(cached_config.as_str())
            .expect("JSON parsing error.");
        let config_val: serde_json::Value = serde_json::from_str(config_str.as_str())
            .expect("JSON parsing error.");

        assert_eq!(cached_config_val, config_val);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_manifest() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;
        
        let image_hash = image.get_image_hash().expect("extract image hash");
        let cached_manifest = ImageManifest::fetch(
            cache_manager.get_image_folder_path(&image_hash)
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let manifest_str = extract::extract_manifest_json(image.data()).unwrap();

        // Create JSON Values from the strings in order to ignore the whitespaces from the cached manifest file
        let cached_manifest_val: serde_json::Value = serde_json::from_str(cached_manifest.as_str())
            .expect("JSON parsing error.");
        let manifest_val: serde_json::Value = serde_json::from_str(manifest_str.as_str())
            .expect("JSON parsing error.");

        assert_eq!(cached_manifest_val, manifest_val);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_env_expressions() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let image_hash = image.get_image_hash().expect("extract image hash");
        let cached_env_expr = EnvExpressions::fetch(
            cache_manager.get_image_folder_path(&image_hash)
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let env_expr = extract::extract_env_expressions(image.data()).unwrap();

        for (cached_expr, expr) in cached_env_expr.iter().zip(env_expr.iter()) {
            assert_eq!(cached_expr, expr);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_cmd_expressions() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let image_hash = image.get_image_hash().expect("extract image hash");
        let cached_cmd_expr = CmdExpressions::fetch(
            cache_manager.get_image_folder_path(&image_hash)
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let cmd_expr = extract::extract_cmd_expressions(image.data()).unwrap();

        for (cached_expr, expr) in cached_cmd_expr.iter().zip(cmd_expr.iter()) {
            assert_eq!(cached_expr, expr);
        }

        Ok(())
    }

    macro_rules! aw {
        ($e:expr) => {
            tokio_test::block_on($e)
        };
    }
}
