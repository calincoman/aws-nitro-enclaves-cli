// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap};
use std::fs::File;
use std::io::{Read};
use std::path::{PathBuf, Path, self};
use std::fs;
use std::env;

use oci_distribution::{Reference, client::ImageData};
use sha2::Digest;

use crate::cache;
use crate::pull;
use crate::constants::{self, CACHE_MANIFEST_FILE_NAME, CACHE_CONFIG_FILE_NAME};
use crate::extract::{self, ExtractError, extract_image_hash};
use crate::cache::{StoreResult, StoreError};

#[derive(Debug, PartialEq)]
pub enum CacheManagerError {
    PathError(String),
    StoreError(String),
    RetrieveError(String),
    ValidateError(String),
    ManifestNotValid,
    ConfigNotValid,
}

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
    /// 
    /// e.g. "hello-world" image has reference "docker.io/library/hello-world:latest"
    pub fn build_image_reference(image_name: &str) -> Result<Reference, oci_distribution::ParseError> {
        let image_ref = image_name.parse().map_err(|err| {
            eprintln!("Failed to build image reference from image name.");
            oci_distribution::ParseError::ReferenceInvalidFormat
        })?;

        Ok(image_ref)
    }

    /// Calculates the image name (without tag) from the reference
    pub fn get_image_name_from_ref(image_ref: &Reference) -> String {
        image_ref.repository().split('/').collect::<Vec<&str>>().get(1).unwrap().to_string()
    }

    /// Returns the digest hash of an image by extracting it from the struct
    pub fn get_image_hash(&self) -> Result<String, ExtractError> {
        extract_image_hash(&self.data)
    }

    /// Returns the digest hash of an image by looking first in the cache,
    /// then trying to extract it from the Image struct
    pub fn get_image_hash_first_from_cache(&self, cache_manager: &CacheManager) -> Result<String, ExtractError> {
        // Try to get the hash from the cache by looking up the image reference
        match cache_manager.get_image_hash(&self.reference.whole()) {
            Some(hash) => Ok(hash),
            // If not found in the cache, extract the image hash from the struct
            None => match extract::extract_image_hash(&self.data) {
                Ok(hash) => Ok(hash),
                Err(err) => Err(ExtractError::ImageHashError(Some(err.to_string())))
            }
        }.map_err(|err| ExtractError::ImageHashError(Some(err.to_string())))
    }
}

/// Keeps a record of images stored in the cache and updates the index.json file
/// 
/// Also contains logic for updating and handling the cache
/// 
/// The index.json file is located in the cache root and keeps track of images stored in cache
/// 
/// The cache structure is:
/// 
/// {CACHE_ROOT_PATH}/index.json\
/// {CACHE_ROOT_PATH}/image_cache_folder1\
/// {CACHE_ROOT_PATH}/image_cache_folder2\
/// etc.
/// 
/// An image cache folder contains:
/// 
/// {IMAGE_FOLDER_PATH}/config.json\
/// {IMAGE_FOLDER_PATH}/manifest.json\
/// {IMAGE_FOLDER_PATH}/layers - folder containing all layers, each in a separate gzip compressed file\
/// {IMAGE_FOLDER_PATH}/image_file - gzip compressed file containing all layers combined, as an array of bytes\
/// {IMAGE_FOLDER_PATH}/env.sh - contains ENV expressions of the image\
/// {IMAGE_FOLDER_PATH}/cmd.sh - contains CMD expressions of the image
pub struct CacheManager {
    /// Represents the root path used as cache (the path at which the index.json file is stored)
    cache_path: PathBuf,
    /// Stores the (image URI <-> image hash) mappings from the index.json of the cache
    values: HashMap<String, String>,
}

impl CacheManager {
    /// Create a new CacheManager which uses the provided folder path as cache root
    pub fn new<P: AsRef<Path>>(cache_path: P) -> CacheManager {
        Self {
            cache_path: cache_path.as_ref().to_path_buf(),
            values: HashMap::new()
        }
    }

    pub fn cache_path(&self) -> &PathBuf {
        &self.cache_path
    }

    pub fn cache_path_mut(&mut self) -> &mut PathBuf {
        &mut self.cache_path
    }

    pub fn values(&self) -> &HashMap<String, String> {
        &self.values
    }

    pub fn values_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.values
    }

    /// Returns the image hash corresponding to the image URI, if available in the hashmap
    pub fn get_image_hash(&self, uri: &String) -> Option<String> {
        self.values.get(uri).map(|val| val.to_string())
    }

    /// Checks if an image is correctly cached
    pub async fn is_cached(&self, image_name: &String) -> Result<bool, CacheManagerError> {
        let image_ref = Image::build_image_reference(image_name)
            .map_err(|_| CacheManagerError::ValidateError("".to_string()))?;

        // If the image is not found in the index.json file, it is definitely not cached
        let image_hash = self.get_image_hash(&image_ref.whole());
        if image_hash.is_none() {
            return Ok(false);
        }
        let image_hash = image_hash.unwrap();

        // The hash was found, now check that the manifest and config are also cached and correct
        // Pull the image manifest in order to find the manifest's and config's digests
        let (pulled_manifest, _) = pull::pull_manifest(&image_ref)
            .await
            .map_err(|err| CacheManagerError::ValidateError(format!("{:?}", err)))?;
        let pulled_manifest_str = serde_json::to_string(&pulled_manifest)
            .map_err(|err| CacheManagerError::ValidateError(format!("{:?}", err)))?;

        // Validate the manifest
        if self.validate_manifest(&image_hash, &pulled_manifest_str).is_err() {
            return Ok(false);
        }

        // Validate the config
        if self.validate_config(&image_hash).is_err() {
            return Ok(false);
        }

        Ok(true)
    }

    /// Caches the image at the path provided in the CacheManager object
    /// 
    /// The cache root folder path should already be created
    /// 
    /// This function checks if the image is already cached, and, if not, pulls the image
    /// from the remote registry
    pub async fn cache_image(&mut self, image_name: &String) -> StoreResult<()> {
        // Check if the image is already cached
        let res = self.is_cached(image_name)
            .await
            .map_err(|err| StoreError::ImageError(format!("Error when validating cached image: {:?}", err)))?;
        
        // If the image is already cached, exit the function
        if res == true {
            println!("Image {} is already cached.", image_name);
            return Ok(());
        }

        // The image is not correctly cached or not cached at all, so pull it from the remote registry
        let image_data = pull::pull_image_data(&image_name)
            .await
            .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?;
        // Build the image reference
        let image_ref = Image::build_image_reference(image_name)
            .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?;

        let image = Image::new(image_ref, image_data);
        let image_hash = image.get_image_hash()
            .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?;

        // Determine the path of the folder where the image data will be stored
        let image_folder_path = self.get_image_folder_path(&image_hash);

        // Create the folder where the image data will be stored
        self.create_image_folder(&image_hash)
            .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?;

        // Store the image data in the cache
        let _ = cache::store_image_data(&image, &image_folder_path)?;

        // Add the image to the CacheManager's HashMap
        self.record_image(&image)
            .map_err(|err| StoreError::IndexJsonError(format!("{:?}", err)))?;

        // Serialize the CacheManager's hashmap to the index.json file
        self.write_index_file()
            .map_err(|err| StoreError::IndexJsonError(format!("{:?}", err)))?;

        Ok(())
    }

    /// Record the new image that was added to the cache (add it to the CacheManager's hashmap)
    pub fn record_image(&mut self, image: &Image) -> Result<(), CacheManagerError> {
        // Get the image hash
        let image_hash = extract::extract_image_hash(image.data())
            .map_err(|err| CacheManagerError::StoreError(format!(
                "Cache manager failed to record image: {:?}", err)))?;

        // Get the image URI
        let image_uri = image.reference().whole();

        self._add_entry(&image_uri, &image_hash);

        Ok(())
    }

    /// Add a new image URI <-> hash entry to the hashmap
    pub fn _add_entry(&mut self, uri: &String, hash: &String) {
        self.values.insert(uri.to_string(), hash.to_string());
    }

    /// Creates the cache index.json file, if not already created
    /// 
    /// The file is created at the location specified by the 'cache_path' field of the current
    /// CacheManager object
    ///
    /// Should be called when instantiating a new CacheManager object
    pub fn create_index_file(self) -> Result<Self, CacheManagerError> {
        let mut index_file_path = self.cache_path.clone();
        index_file_path.push(constants::CACHE_INDEX_FILE_NAME);

        match File::open(&index_file_path) {
            Ok(_) => { return Ok(self); },
            Err(_) => {
                match File::create(&index_file_path) {
                    Ok(_) => { return Ok(self); },
                    Err(err) => {
                        return Err(CacheManagerError::PathError(format!(
                            "Failed to create the index.json file: {:?}", err)));
                    }
                }
            }
        };
    }

    /// Populate the CacheManager's hashmap with the values from the index.json file which contains the mappings.
    ///
    /// Returns a CacheManager object with the hashmap updated
    /// 
    /// Should be called when instantiating a new CacheManager object, with an index.json file already created
    pub fn populate_hashmap(mut self) -> Result<Self, CacheManagerError> {
        let mut index_file_path = self.cache_path.clone();
        index_file_path.push(constants::CACHE_INDEX_FILE_NAME);

        // Open the JSON file
        let index_file = File::open(index_file_path);
        let mut json_file = index_file.map_err(|err|
            CacheManagerError::RetrieveError(format!("Failed to open the index.json file: {:?}", err)))?;

        // Read the JSON string from the file
        let mut json_string = String::new();
        json_file.read_to_string(&mut json_string).map_err(|err|
            CacheManagerError::RetrieveError(format!("Failed to read from index.json file: {:?}", err)))?;

        // If the cache index file is empty, exit the function to avoid error while parsing
        match json_string.is_empty() {
            true => { return Ok(self); },
            false => ()
        };

        // Try to deserialize the JSON into a HashMap
        let hashmap: HashMap<String, String> = serde_json::from_str(json_string.as_str())
            .map_err(|err| CacheManagerError::RetrieveError(format!(
                "Failed to deserialize the index.json file: {:?}", err)))?;

        self.values = hashmap;

        Ok(self)
    }

    /// Writes the content of the hashmap ((image URI <-> image hash) mappings) to the index.json file
    /// in the cache path provided in the CacheManager
    ///
    /// The already existing content of the file is overwritten
    pub fn write_index_file(&self) -> Result<(), CacheManagerError> {
        let mut index_file_path = self.cache_path.clone().to_path_buf();
        index_file_path.push(constants::CACHE_INDEX_FILE_NAME);

        // Create (or open if it's already created) the index.json file
        let index_file = File::create(index_file_path)
            .map_err(|err| CacheManagerError::StoreError(format!(
                "The cache index.json file could not be created: {:?}", err)))?;

        // Write the hashmap (the mappings) to the file
        serde_json::to_writer(index_file, &self.values)
            .map_err(|err| CacheManagerError::StoreError(format!(
                "Failed to write to the index.json file: {:?}", err)))
    }

    /// Calculates the cache path of the image layers
    pub fn get_layers_cache_path(&self, image_hash: &String) -> PathBuf {
        let mut path = self.get_image_folder_path(image_hash);
        path.push(constants::CACHE_LAYERS_FOLDER_NAME);

        path
    }

    /// Calculates the cache path of the image manifest
    pub fn get_manifest_cache_path(&self, image_hash: &String) -> PathBuf {
        let mut path = self.get_image_folder_path(image_hash);
        path.push(constants::CACHE_MANIFEST_FILE_NAME);

        path
    }
    
    /// Calculates the cache path of the image config
    pub fn get_config_cache_path(&self, image_hash: &String) -> PathBuf {
        let mut path = self.get_image_folder_path(image_hash);
        path.push(constants::CACHE_CONFIG_FILE_NAME);

        path
    }

    pub fn get_layers(&self, image_hash: &String) -> Result<Vec<File>, CacheManagerError> {
        let path = self.get_layers_cache_path(image_hash);

        let layers: Vec<File> =
            fs::read_dir(&path)
                .map_err(|err|
                    CacheManagerError::RetrieveError(format!("Failed to get image layers: {:?}", err)))?
                .into_iter()
                .filter(|entry| entry.is_ok())
                // Get only the files
                .filter(|entry| entry.as_ref().unwrap().path().is_file())
                .map(|file| File::open(&file.unwrap().path()))
                .filter_map(|file| file.ok().and_then(|f| Some(f)))
                .collect();

        Ok(layers)
    }

    /// Returns the cached image manifest as a file
    pub fn get_manifest_file(&self, image_hash: &String) -> Result<File, CacheManagerError> {
        let path = self.get_manifest_cache_path(image_hash);

        File::open(&path)
            .map_err(|err|
                CacheManagerError::RetrieveError(format!("Failed to get cached manifest file: {:?}", err)))
    }

    /// Returns the cached image config as a file
    pub fn get_config_file(&self, image_hash: &String) -> Result<File, CacheManagerError> {
        let path = self.get_config_cache_path(image_hash);

        File::open(&path)
            .map_err(|err|
                CacheManagerError::RetrieveError(format!("Failed to get cached config file: {:?}", err)))
    }

    /// Checks that the image manifest is cached
    pub fn validate_manifest(
        &self,
        image_digest: &String,
        pulled_manifest: &String
    ) -> Result<(), CacheManagerError> {
        let manifest_file = self.get_manifest_file(image_digest);

        // Read the manifest from the cache
        let mut cached_manifest = String::new();
        manifest_file
            .map_err(|_| CacheManagerError::ManifestNotValid)?
            .read_to_string(&mut cached_manifest)
                .map_err(|_| CacheManagerError::ManifestNotValid)?;

        // Calculate the sha256 digest of the cached manifest in order to validate the contents
        let cached_digest = format!("{:x}", sha2::Sha256::digest(cached_manifest.as_bytes()));

        // Calculate the sha256 digest of the cached manifest
        let pulled_digest = format!("{:x}", sha2::Sha256::digest(pulled_manifest.as_bytes()));

        // Check that the two digests are equal
        match *pulled_digest == cached_digest {
            true => Ok(()),
            false => Err(CacheManagerError::ManifestNotValid)
        }
    }

    // pub fn validate_layers(
    //     &self,
    //     image_digest: &String
    // ) -> Result<(), CacheManagerError> {
        
    // }

    /// Checks that the image config is cached
    pub fn validate_config(
        &self,
        image_digest: &String,
    ) -> Result<(), CacheManagerError> {
        let config_file = self.get_config_file(image_digest);
        let mut manifest_file = self.get_manifest_file(image_digest)
            .map_err(|err| CacheManagerError::ValidateError(format!("{:?}", err)))?;

        // Read the config from the cache
        let mut config_str = String::new();
        config_file
            .map_err(|_| CacheManagerError::ConfigNotValid)?
            .read_to_string(&mut config_str)
                .map_err(|_| CacheManagerError::ConfigNotValid)?;

        // Calculate the sha256 digest of the cached config in order to validate the contents
        let cached_digest = format!("{:x}", sha2::Sha256::digest(config_str.as_bytes()));

        // Now get the correct digest from the manifest and check that the two digests match
        let config_digest = self.get_config_digest_from_manifest(&mut manifest_file)
            .map_err(|err| CacheManagerError::RetrieveError(format!("{:?}", err)))?;

        // Check that the two digests are equal
        match config_digest == cached_digest {
            true => Ok(()),
            false => Err(CacheManagerError::ConfigNotValid)
        }
    }

    /// Determines the config digest of an image by reading from the manifest file provided as parameter
    pub fn get_config_digest_from_manifest(&self, manifest: &mut File) -> Result<String, CacheManagerError> {
        // Read the manifest string from the file
        let mut manifest_str = String::new();
        manifest
            .read_to_string(&mut manifest_str)
                .map_err(|err| CacheManagerError::RetrieveError(format!("{:?}", err)))?;

        let json_obj: serde_json::Value = serde_json::from_str(manifest_str.as_str())
                .map_err(|err| CacheManagerError::RetrieveError("Could not parse manifest JSON.".to_string()))?;

        // Extract the config digest hash
        let config_digest = json_obj
            .get("config").ok_or_else(||
                CacheManagerError::RetrieveError("'config' field missing from image manifest.".to_string()))?
            .get("digest").ok_or_else(||
                CacheManagerError::RetrieveError("'digest' field missing from image manifest.".to_string()))?
            .as_str().ok_or_else(||
                CacheManagerError::RetrieveError("Failed to get config digest from image manifest.".to_string()))?
            .strip_prefix("sha256:").ok_or_else(||
                CacheManagerError::RetrieveError("Failed to get config digest from image manifest.".to_string()))?
            .to_string();

        Ok(config_digest)
    }

    /// Returns the default root folder path of the cache
    /// 
    /// If the TEST_MODE_ENABLED constant is set to true, ./test_cache/container_cache
    /// is used as cache root
    pub fn get_default_cache_root_path() -> Result<PathBuf, CacheManagerError> {
        // For testing, the cache will be saved to the local directory
        if constants::TEST_MODE_ENABLED {
            let mut local_path = std::env::current_dir().unwrap();
            local_path.push("test_cache");
            local_path.push("container_cache");
            return Ok(local_path);
        }

        // Try to use XDG_DATA_HOME as default root
        let root = match env::var_os(constants::CACHE_ROOT_FOLDER) {
            Some(val) => val.into_string()
                .map_err(|err| CacheManagerError::PathError(format!(
                    "Failed to determine the cache root folder: {:?}", err)))?,
            // If XDG_DATA_HOME is not set, use {HOME}/.local/share as specified in
            // https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
            None => {
                let home_folder = env::var_os("HOME")
                    .ok_or_else(|| CacheManagerError::PathError(
                        "Failed to determine the cache root folder: HOME environment variable is not set.".to_string()))?;
                format!("{}/.local/share/", home_folder.into_string()
                    .map_err(|err| CacheManagerError::PathError(format!("{:?}", err)))?)
            }
        };

        let mut path = PathBuf::from(root);
        // Add the additional path to the root
        path.push(".nitro_cli/container_cache");

        Ok(path)
    }

    /// Returns the path to the cache root folder of an image by using the image hash provided
    /// as argument
    pub fn get_image_folder_path(&self, image_hash: &String) -> PathBuf {
        let mut path = self.cache_path.clone();
        path.push(image_hash);

        path
    }

    /// Creates all folders from the cache path provided in the current CacheManager object
    pub fn create_cache_folders(&self) -> Result<(), CacheManagerError> {
        fs::create_dir_all(&self.cache_path)
            .map_err(|err| CacheManagerError::PathError(format!(
                "Failed to create the cache folder path: {:?}", err)))
    }

    /// Creates the cache folder where the image data will be stored (the folder is created
    /// at the path provided in the current CacheManager object)
    /// 
    /// The function also creates all folders from the path that are not already created
    pub fn create_image_folder(&self, image_hash: &String) -> Result<(), CacheManagerError> {
        let mut path = self.get_image_folder_path(image_hash);
        
        fs::create_dir_all(&path).map_err(|err|
            CacheManagerError::PathError(format!("Failed to create the image cache folder: {:?}", err)))
    }

    /// Determines the path of the image cache folder considering the cache root as the path
    /// given as parameter
    pub fn get_custom_image_folder_path(image_hash: &String, cache_path: &PathBuf) -> PathBuf {
        let mut image_folder_path = cache_path.clone();
        // Build the image folder path by appending the image hash at the end of the provided path
        image_folder_path.push(&image_hash);

        image_folder_path
    }

    /// Creates all folders from the path given as parameter
    pub fn create_cache_folder_path(cache_path: &PathBuf) -> Result<(), CacheManagerError> {
        fs::create_dir_all(cache_path)
            .map_err(|err| CacheManagerError::PathError(format!(
                "Failed to create the cache folder path: {:?}", err)))
    }

    /// Creates the cache folder where the image data will be stored (the folder is created
    /// in the directory given as parameter)
    /// 
    /// The function also creates all folders from the path that are not already created
    pub fn create_image_folder_to_path(image_hash: &String, cache_path: &PathBuf) -> Result<(), CacheManagerError> {
        let image_folder_path = Self::get_custom_image_folder_path(image_hash, cache_path);

        fs::create_dir_all(&image_folder_path).map_err(|err|
            CacheManagerError::PathError(format!("Failed to create the image cache folder: {:?}", err)))
    }
}
