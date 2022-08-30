// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap};
use std::fs::File;
use std::io::{Read};
use std::path::{PathBuf, Path};
use std::fs;
use std::env;

use crate::constants;
use crate::extract;
use crate::cache::{
    Image,
    CacheManagerError,
    StoreResult,
    StoreError,

    ImageContents,
    ImageLayers,
    ImageConfig,
    ImageManifest,
    EnvExpressions,
    CmdExpressions,

    CacheStore,
};

use oci_distribution::client::{Client, ClientProtocol, ClientConfig};

/// Builds a client which uses the protocol given as parameter
/// Client required for the https://github.com/krustlet/oci-distribution library API
pub fn build_client(protocol: ClientProtocol) -> Client {
    let client_config = ClientConfig {
        protocol,
        ..Default::default()
    };
    Client::new(client_config)
}

/// Keeps a record of images stored in the cache and handles the index.json file parsing
/// 
/// Also contains logic for creating the cache folders
pub struct CacheManager {
    // Represents the folder path used as cache
    cache_path: PathBuf,
    // Stores the (image URI <-> image hash) mappings from the index.json of the cache
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

    /// Checks if the image with the specified uri is cached
    pub fn is_cached(&self, uri: &String) -> bool {
        match self.get_image_hash(uri) {
            Some(_) => true,
            None => false
        }
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

    /// Creates the cache index.json file, if not already created.
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

    /// Populate the CacheManager's hashmap with the values from the index.json file which contains the mappings
    /// The path given should be a folder containing an index.json file
    ///
    /// Returns a CacheManager object with the hashmap updated
    /// 
    /// Should be called when instantiating a new CacheManager object, with an index.json file already created
    pub fn populate_hashmap(mut self) -> Result<Self, CacheManagerError> {
        let mut index_file_path = self.cache_path.clone();
        index_file_path.push(constants::CACHE_INDEX_FILE_NAME);

        // Open the JSON file
        let index_file = File::open(index_file_path);
        let mut json_file = index_file.map_err(|err| CacheManagerError::RetrieveError(format!(
            "Failed to open the index.json file: {:?}", err)))?;

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
    /// in the cache path provided in the struct
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

    /// Returns the default root folder path of the cache
    /// 
    /// If the TEST_MODE_ENABLED constant is set to true, ./test_cache/container_cache
    /// is used as cache root
    pub fn get_default_cache_root_folder() -> Result<PathBuf, CacheManagerError> {
        // For testing, the cache will be saved to the local directory
        if constants::TEST_MODE_ENABLED {
            let mut local_path = std::env::current_dir().unwrap();
            local_path.push("test_cache");
            local_path.push("container_cache");
            return Ok(local_path);
        }

        // Use XDG_DATA_HOME as default root
        let root = match env::var_os(constants::CACHE_ROOT_FOLDER) {
            Some(val) => val.into_string()
                .map_err(|err| CacheManagerError::PathError(format!(
                    "Failed to determine the cache root folder: {:?}", err)))?,
            // If XDG_DATA_HOME is not set, use $HOME/.local/share as specified in
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
        path.push(".nitro_cli/container_cache");

        Ok(path)
    }


    /// Returns the default path to the cache root folder of an image:
    /// 
    /// {ROOT}/.nitro_cli/container_cache/{IMAGE_HASH}
    pub fn get_default_image_folder_path(image: &Image, cache_manager: &CacheManager) -> Result<PathBuf, CacheManagerError> {
        // Get the image hash
        let hash = Image::get_image_hash(image, cache_manager).map_err(|err|
            CacheManagerError::PathError(format!("Failed to determine the image folder path: {:?}", err)))?;

        // Get the cache root folder
        let mut cache_root = Self::get_default_cache_root_folder()
            .map_err(|err| CacheManagerError::PathError(format!(
                "Failed to determine the image folder path: {:?}", err)))?;

        cache_root.push(hash);

        Ok(cache_root)
    }

    /// Determines the path of the image cache folder considering the cache as the path
    /// given as parameter
    pub fn get_custom_image_folder_path(image: &Image, cache_path: &PathBuf) -> Result<PathBuf, CacheManagerError> {
        let mut image_folder_path = cache_path.clone();
        // Build the image folder path by appending the image hash at the end of the provided path
        image_folder_path.push(extract::extract_image_hash(&image.data())
            .map_err(|err| CacheManagerError::PathError(format!(
                "Failed to determine the image cache folder: {:?}", err)))?);

        Ok(image_folder_path)
    }

    /// Creates all folders from the path of the cache given as parameter
    pub fn create_cache_folder_path(cache_path: &PathBuf) -> Result<(), CacheManagerError> {
        fs::create_dir_all(cache_path)
            .map_err(|err| CacheManagerError::PathError(format!(
                "Failed to create the cache folder path: {:?}", err)))
    }

    /// Creates the cache folder where the image data will be stored (the folder is created
    /// in the directory given as parameter)
    /// 
    /// The function also creates all folders from the path that are not already created
    pub fn create_image_folder(image: &Image, cache_path: &PathBuf) -> Result<(), CacheManagerError> {
        let image_folder_path = Self::get_custom_image_folder_path(image, cache_path).map_err(|err|
            CacheManagerError::PathError(format!("Failed to create the image cache folder: {:?}", err)))?;

        fs::create_dir_all(image_folder_path).map_err(|err|
            CacheManagerError::PathError(format!("Failed to create the image cache folder: {:?}", err)))
    }
}

/// Caches the image at the path provided in the CacheManager given as argument
/// 
/// The cache root folder path should already be created
/// 
/// This function creates the cache image folder, stores the contents and updates the cache index.json
pub fn cache_image(cache_manager: &mut CacheManager, image: &Image) -> StoreResult<()> {
    // Check if the image is already cached
    if cache_manager.is_cached(&image.reference().whole()) {
        eprintln!("Image with URI {} is already cached", image.reference().whole());
        return Ok(())
    }

    // Determine the path of the folder where the image data will be stored
    let image_folder_path = CacheManager::get_custom_image_folder_path(image, cache_manager.cache_path())
        .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?;

    // Create the folder where the image data will be stored
    CacheManager::create_image_folder(image, cache_manager.cache_path())
        .map_err(|err| StoreError::ImageError(format!("{:?}", err)))?;

    // Store the image data in the cache
    let _ = _store_image_data(&image, image_folder_path)
        .map_err(|err| err);

    // Add the image to the CacheManager's HashMap
    cache_manager.record_image(image)
        .map_err(|err| StoreError::IndexJsonError(format!("{:?}", err)))?;

    // Serialize the CacheManager's hashmap to the index.json file
    cache_manager.write_index_file()
        .map_err(|err| StoreError::IndexJsonError(format!("{:?}", err)))?;

    Ok(())
}

// Stores the image data in the cache at the path provided as parameter
fn _store_image_data<P: AsRef<Path>>(image: &Image, path: P) -> StoreResult<()> {
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

    image_contents.store(&path).map_err(|err| err)?;
    image_layers.store(&path).map_err(|err| err)?;
    image_config.store(&path).map_err(|err| err)?;
    image_manifest.store(&path).map_err(|err| err)?;
    env.store(&path).map_err(|err| err)?;
    cmd.store(&path).map_err(|err| err)?;

    Ok(())
}
