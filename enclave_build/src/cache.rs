// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use oci_distribution::{Reference};
use oci_distribution::client::{ImageData, ImageLayer};
use oci_distribution::ParseError;

use crate::cache_manager::{self, CacheManager};
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
    IndexJsonError(String),
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

/// Wrapper struct which represents an image
/// 
/// Fields:
/// - reference: The image URI (Reference struct is from https://github.com/krustlet/oci-distribution library)
/// 
/// - data: ImageData struct from https://github.com/krustlet/oci-distribution
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
    pub fn build_image_reference(image_name: &str) -> Result<Reference, ParseError> {
        let image_ref = image_name.parse().map_err(|err| {
            eprintln!("Failed to build image reference from image name.");
            ParseError::TagInvalidFormat
        })?;

        Ok(image_ref)
    }

    pub fn get_image_name_from_ref(image_ref: &Reference) -> String {
        image_ref.repository().split('/').collect::<Vec<&str>>().get(1).unwrap().to_string()
    }

    /// Returns the digest hash of an image by looking first in the cache,
    /// then trying to extract it from the struct
    pub fn get_image_hash(&self, cache_manager: &CacheManager) -> Result<String, ExtractError> {
        // Try to get the hash from the cache by looking up the image reference
        match cache_manager.get_image_hash(&self.reference.whole()) {
            Some(hash) => Ok(hash),
            // If not found in the cache, extract the image hash from the struct
            None => match extract::extract_image_hash(&self.data) {
                Ok(hash) => Ok(hash),
                Err(err) => Err(ExtractError::ImageHashError(format!("{:?}", err)))
            }
        }.map_err(|err| ExtractError::ImageHashError(format!("{:?}", err)))
    }
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
        let image_contents = extract::extract_image(&image.data)
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
        let image_layers = extract::extract_layers(&image.data)
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
        let image_config = extract::extract_config_json(&image.data)
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
        let image_manifest = extract::extract_manifest_json(&image.data)
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
        let env_expressions = extract::extract_env_expressions(&image.data)
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
        let cmd_expressions = extract::extract_cmd_expressions(&image.data)
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

#[derive(Debug, PartialEq)]
pub enum TestError {
    ImageCacheError(String),
    ImagePullError(String),
    CredentialsError(String),
}

/// This mod pulls an image from the docker registry and tests that the cache store / fetch works
/// THE SETUP_CACHE FUNCTION SHOULD BE RUN FIRST TO PULL AND STORE THE IMAGE
#[cfg(test)]
mod tests {
    use super::*;
    use oci_distribution::secrets::RegistryAuth;

    const TEST_IMAGE_NAME: &str = "hello-world";

    /// This should be run first to pull the image and store it to the cache so the other tests do not fail
    /// 
    /// The function pulls the TEST_IMAGE_NAME image
    #[tokio::test]
    async fn setup_test_cache() -> Result<(), TestError> {
        // Name of the image to be used for testing
        let image_name = TEST_IMAGE_NAME.to_string();
        let image_ref = Image::build_image_reference(&image_name)
            .expect("Failed to build image reference");  

        // Pull the image data from the remote registry
        let image_data = get_image_data(&image_name).await
            .map_err(|err| err)?;

        // Get the root folder of the cache
        let cache_folder_path = CacheManager::get_default_cache_root_folder().map_err(|err|
            TestError::ImageCacheError(format!("{:?}", err)))?;

        // Create the folders from the path
        CacheManager::create_cache_folder_path(&cache_folder_path).map_err(|err|
            TestError::ImageCacheError(format!("{:?}", err)))?;

        // Create the CacheManager
        let mut cache_manager = CacheManager::new(&cache_folder_path)
            .create_index_file()
                .map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?
            .populate_hashmap()
                .map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;


        let image = Image::new(image_ref.clone(), image_data.clone());
        
        cache_manager::cache_image(&mut cache_manager, &image)
            .map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        Ok(())
    }

    /// Used for setting up a test
    pub async fn setup_test() -> (Image, CacheManager) {
        // Name of the image used for testing
        let image_name = TEST_IMAGE_NAME.to_string();
        let image_ref = Image::build_image_reference(&image_name)
            .expect("Failed to build image reference from image name.");

        let cache_folder_path = CacheManager::get_default_cache_root_folder().unwrap();
        CacheManager::create_cache_folder_path(&cache_folder_path).unwrap();

        // Download the image data from the remote registry
        let image_data = get_image_data(&image_name).await.unwrap();

        // Create the cache manager for testing
        let cache_manager = CacheManager::new(&cache_folder_path)
            .create_index_file().unwrap()
            .populate_hashmap().unwrap();

        (Image::new(image_ref.clone(), image_data.clone()), cache_manager)
    }

    // Pulls an ImageData struct from the Docker remote registry
    // For testing purposes
    pub async fn get_image_data(image_name: &String) -> Result<ImageData, TestError> {
        let accepted_media_types =
            vec![
                oci_distribution::manifest::WASM_LAYER_MEDIA_TYPE,
                oci_distribution::manifest::IMAGE_DOCKER_LAYER_GZIP_MEDIA_TYPE
            ];

        let mut client = cache_manager::build_client(oci_distribution::client::ClientProtocol::Https);
        let image_ref = Image::build_image_reference(image_name)
            .expect("Failed to build image reference from image name.");
        let auth = docker_auth(&TEST_IMAGE_NAME.to_string())
            .map_err(|err| err)?;
        
        let image_data = client
            .pull(&image_ref, &auth, accepted_media_types)
            .await;
        
        match image_data {
            Ok(img_data) => Ok(img_data),
            Err(err) => Err(TestError::ImagePullError(format!(
                "Could not pull the image data from the remote registry: {:?}", err)))
        }
    }

    pub fn docker_auth(image_name: &String) -> Result<RegistryAuth, TestError> {
        println!("First you should provide docker credentials.");

        let mut credential_file_path = std::env::current_dir().unwrap();
        // Just for testing
        credential_file_path.push("credentials.txt");

        let auth = match File::open(&credential_file_path) {
            Ok(_) => {
                println!("Credentials found.");
                read_credentials(&credential_file_path)
            },
            Err(err) => {
                println!("No credential file supplied, performing anonymous pull: {:?}", err);
                RegistryAuth::Anonymous
            }
        };
    
        Ok(auth)
    }

    /// FOR TESTING
    /// Reads docker registry credentials from the file path given as parameter
    ///
    /// In that file, the DockerHub username should be on the first line and the password on the second
    /// 
    /// Credentials are used for pulling the image from the remote registry, but are not mandatory
    pub fn read_credentials<P: AsRef<Path>>(path: P) -> RegistryAuth {
        let mut file = File::open(path)
            .expect("File not found - a 'credentials.txt' file with the docker registry credentials should be\
                            in the local directory");

        let mut data = String::new();
        file.read_to_string(&mut data)
            .expect("Error while reading file");

        let words: Vec<&str> = data.split("\n").collect();
        if words.len() < 2 || words[0].len() == 0 || words[1].len() == 0 {
            return RegistryAuth::Anonymous;
        }
        RegistryAuth::Basic(words[0].to_string(), words[1].to_string())
    }

    pub fn get_docker_auth(image_name: &String) -> Result<RegistryAuth, TestError> {
        let image = image_name.clone();
        let host = if let Ok(uri) = url::Url::parse(&image) {
            uri.host().map(|s| s.to_string())
        } else {
            // Some Docker URIs don't have the protocol included, so just use
            // a dummy one to trick Url that it's a properly defined Uri.
            let uri = format!("dummy://{}", image);
            if let Ok(uri) = url::Url::parse(&uri) {
                uri.host().map(|s| s.to_string())
            } else {
                None
            }
        };

        match host.clone() {
            Some(val) => println!("{}", val),
            None => println!("No host found.") 
        };

        if let Some(registry_domain) = host {
            let config_file = get_config_file()?;

            let config_json: serde_json::Value = serde_json::from_reader(&config_file)
                .map_err(|err| TestError::CredentialsError(format!("JSON was not well-formatted: {}", err)))?;

            let auths = config_json.get("auths").ok_or_else(|| {
                TestError::CredentialsError("Could not find auths key in config JSON".to_string())
            })?;

            if let serde_json::Value::Object(auths) = auths {
                for (registry_name, registry_auths) in auths.iter() {
                    if !registry_name.to_string().contains(&registry_domain) {
                        continue;
                    }

                    let auth = registry_auths
                        .get("auth")
                        .ok_or_else(|| {
                            TestError::CredentialsError("Could not find auth key in config JSON".to_string())
                        })?
                        .to_string();

                    let auth = auth.replace(r#"""#, "");
                    let decoded = base64::decode(&auth).map_err(|err| {
                        TestError::CredentialsError(format!("Invalid Base64 encoding for auth: {}", err))
                    })?;
                    let decoded = std::str::from_utf8(&decoded).map_err(|err| {
                        TestError::CredentialsError(format!("Invalid utf8 encoding for auth: {}", err))
                    })?;

                    if let Some(index) = decoded.rfind(':') {
                        let (user, after_user) = decoded.split_at(index);
                        let (_, password) = after_user.split_at(1);

                        return Ok(RegistryAuth::Basic(user.to_string(), password.to_string()));
                    }
                }
            }
        }

        Err(TestError::CredentialsError(
            "No credentials found for the current image".to_string(),
        ))
    }

    fn get_config_file() -> Result<File, TestError> {
        if let Ok(file) = std::env::var("DOCKER_CONFIG") {
            let config_file = File::open(file).map_err(|err| {
                TestError::CredentialsError(format!(
                    "Could not open file pointed by env\
                     DOCKER_CONFIG: {}",
                    err
                ))
            })?;
            Ok(config_file)
        } else {
            if let Ok(home_dir) = std::env::var("HOME") {
                let default_config_path = format!("{}/.docker/config.json", home_dir);
                let config_path = Path::new(&default_config_path);
                if config_path.exists() {
                    let config_file = File::open(config_path).map_err(|err| {
                        TestError::CredentialsError(format!(
                            "Could not open file {:?}: {}",
                            config_path.to_str(),
                            err
                        ))
                    })?;
                    return Ok(config_file);
                }
            }
            Err(TestError::CredentialsError(
                "Config file not present, please set env \
                 DOCKER_CONFIG accordingly"
                    .to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn test_cached_image() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let cached_image_bytes = ImageContents::fetch(
                CacheManager::get_custom_image_folder_path(&image, cache_manager.cache_path())
                    .expect("Failed to get image folder path.")
            ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let image_bytes = extract::extract_image(image.data()).unwrap();
        assert_eq!(cached_image_bytes, image_bytes);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_layers() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let cached_layers = ImageLayers::fetch(
                CacheManager::get_custom_image_folder_path(&image, cache_manager.cache_path())
                    .expect("Failed to get image folder path.")
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

        let cached_config = ImageConfig::fetch(
            CacheManager::get_custom_image_folder_path(&image, cache_manager.cache_path())
                    .expect("Failed to get image folder path.")
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let config_str = extract::extract_config_json(image.data()).unwrap();

        assert_eq!(cached_config, config_str);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_manifest() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;
        
        let cached_manifest = ImageManifest::fetch(
            CacheManager::get_custom_image_folder_path(&image, cache_manager.cache_path())
                    .expect("Failed to get image folder path.")
        ).map_err(|err| TestError::ImageCacheError(format!("{:?}", err)))?;

        let manifest_str = extract::extract_manifest_json(image.data()).unwrap();

        assert_eq!(cached_manifest, manifest_str);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_env_expressions() -> Result<(), TestError> {
        let (image, cache_manager) = setup_test().await;

        let cached_env_expr = EnvExpressions::fetch(
            CacheManager::get_custom_image_folder_path(&image, cache_manager.cache_path())
                    .expect("Failed to get image folder path.")
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

        let cached_cmd_expr = CmdExpressions::fetch(
            CacheManager::get_custom_image_folder_path(&image, cache_manager.cache_path())
                    .expect("Failed to get image folder path.")
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
