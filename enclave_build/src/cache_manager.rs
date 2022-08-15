// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap};
use std::fs::File;
use std::io::{Read, BufReader, BufRead};
use std::path::{PathBuf, Path};
use std::fs;

use oci_distribution::client::ImageLayer;
use serde_json::Value;

use glob::glob;

use crate::constants;
use crate::utils::{ExtractLogic, Image};

#[derive(Debug, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum CacheError {
    StoreError(String),
    ArgumentError(String),
    PathError(String),
    RetrieveError(String),
}

#[derive(Debug, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum FetchError {
    ImageError(String),
    ConfigError(String),
    ManifestError(String),
    LayerError(String),
    EnvCmdError(String),
    HashError(String),
}

/// Keeps a record of images stored in the cache and handles the index file parsing
pub struct CacheManager {
    // Represents the folder path used as cache
    cache_path: PathBuf,
    // Stores the (image URI <-> image hash) mappings from the index.json
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

    pub fn get_cache_path(&self) -> &PathBuf {
        &self.cache_path
    }

    pub fn get_cache_path_mut(&mut self) -> &mut PathBuf {
        &mut self.cache_path
    }

    pub fn get_values(&self) -> &HashMap<String, String> {
        &self.values
    }

    pub fn get_values_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.values
    }

    /// Return the image hash corresponding to the image URI, if available in the hashmap
    pub fn get_image_hash(&self, uri: &String) -> Option<String> {
        self.values.get(uri).map(|val| val.to_string())
    }

    pub fn is_cached(&self, uri: &String) -> bool {
        match self.get_image_hash(uri) {
            Some(_) => true,
            None => false
        }
    }

    /// Record the new image that was added to the cache (add it to the CacheManager's hashmap)
    pub fn record_image(&mut self, image: &Image) -> Result<(), CacheError> {
        // Get the image hash
        let image_hash = ExtractLogic::extract_image_hash(image.data())
            .map_err(|err| CacheError::StoreError(format!(
                "Cache manager failed to record image: {:?}", err)))?;

        // Get the image URI
        let image_uri = image.reference().whole();

        self.add_entry(&image_uri, &image_hash);

        Ok(())
    }

    /// Add a new image URI <-> hash entry to the hashmap
    pub fn add_entry(&mut self, uri: &String, hash: &String) {
        self.values.insert(uri.to_string(), hash.to_string());
    }

    /// Populate the hashmap with the values from a JSON index file which contains the mappings
    /// 
    /// The path given should be a folder containing an index.json file
    /// 
    /// Return a CacheManager object with the hashmap updated
    pub fn populate_hashmap(mut self) -> Result<Self, CacheError> {
        let mut index_file_path = self.cache_path.clone();
        index_file_path.push(constants::CACHE_INDEX_FILE_NAME);

        // Open the JSON file
        let index_file = File::open(index_file_path);
        let mut json_file = index_file.map_err(|err| CacheError::RetrieveError(format!(
            "Failed to open the index JSON file: {:?}", err)))?;

        // Read the JSON string from the file
        let mut json_string = String::new();
        json_file.read_to_string(&mut json_string).map_err(|err|
            CacheError::RetrieveError(format!("Failed to read from index file: {:?}", err)))?;

        // If the cache index file is empty, exit the function to avoid error while parsing
        match json_string.is_empty() {
            true => { return Ok(self); },
            false => ()
        };

        // Try to deserialize the JSON into a HashMap
        let hashmap: HashMap<String, String> = serde_json::from_str(json_string.as_str())
            .map_err(|err| CacheError::RetrieveError(format!(
                "Failed to populate the hashmap from the JSON file: {:?}", err)))?;

        self.values = hashmap;

        Ok(self)
    }

    // Creates the cache index.json file, if absent
    pub fn create_index_file_if_absent(self) -> Result<Self, CacheError> {
        let mut index_file_path = self.cache_path.clone();
        index_file_path.push(constants::CACHE_INDEX_FILE_NAME);

        match File::open(&index_file_path) {
            Ok(_) => { return Ok(self); },
            Err(_) => {
                match File::create(&index_file_path) {
                    Ok(_) => { return Ok(self); },
                    Err(err) => {
                        return Err(CacheError::PathError(format!("Failed to create the index file: {:?}", err)));
                    }
                }
            }
        };
    }

    /// Writes the content of the hashmap ((image URI <-> image hash) mappings) to the index.json file
    /// in the specified path (folder path)
    /// 
    /// The already existing content of the file is overwritten
    pub fn write_index_file(&self) -> Result<(), CacheError> {
        let mut index_file_path = self.cache_path.clone().to_path_buf();
        index_file_path.push(constants::CACHE_INDEX_FILE_NAME);

        // Create (or open if it's already created) the index.json file
        let index_file = File::create(index_file_path)
            .map_err(|err| CacheError::StoreError(format!(
                "The cache index file could not be created: {:?}", err)))?;

        // Write the hashmap (the mappings) to the file
        serde_json::to_writer(index_file, &self.values)
            .map_err(|err| CacheError::StoreError(format!(
                "Failed to write hashmap to index JSON file: {:?}", err)))
    }
}

/// Struct which represents a cache search and fetch operation
pub struct CacheFetch {
    // These fields are given on struct instantiation
    image_uri: String,
    cache_path: PathBuf,

    // These fields are computed afterwards
    image_hash: Option<String>,
    image_folder_path: Option<PathBuf>,
}

impl CacheFetch {
    pub fn new(image_uri: String, cache_path: PathBuf) -> Self {
        Self {
            image_uri: image_uri,
            cache_path: cache_path,
            image_hash: None,
            image_folder_path: None,
        }
    }

    pub fn get_image_uri(&self) -> &String {
        &self.image_uri
    }

    pub fn get_cache_path(&self) -> &PathBuf {
        &self.cache_path
    }

    pub fn get_image_uri_mut(&mut self) -> &mut String {
        &mut self.image_uri
    }

    pub fn get_cache_path_mut(&mut self) -> &mut PathBuf {
        &mut self.cache_path
    }

    // Returns the image folder path if it is already calculated and
    // calculates and returns it otherwise
    pub fn get_image_folder_path(&mut self, cache_manager: &CacheManager) -> Result<PathBuf, FetchError> {
        match &self.image_folder_path {
            Some(val) => Ok(val.to_path_buf()),
            None => match self.find_image_folder_path(cache_manager) {
                Ok(val) => {
                    self.image_folder_path = Some(val.clone());
                    Ok(val)
                }
                Err(err) => Err(err)
            }
        }
    }

    // Fetches the image as an array of bytes from the cache
    pub fn fetch_image(&mut self, cache_manager: &CacheManager) -> Result<Vec<u8>, FetchError> {
        let mut target_path = self.get_image_folder_path(cache_manager)?;
        target_path.push(constants::IMAGE_FILE_NAME);
        
        let mut buffer = Vec::new();

        // Open the cached image file and read the bytes from it
        let bytes_read = File::open(target_path)
            .map_err(|err|
                FetchError::ImageError(format!("Failed to open the cached image file: {:?}", err)))?
            .read_to_end(&mut buffer)
                .map_err(|err| FetchError::ImageError(format!(
                    "Failed to read from cached image file: {:?}", err)))?;

        // If no bytes were read, throw an error
        if (bytes_read == 0) {
            return Err(FetchError::ImageError("No data was read from the cached image file".to_string()));
        }

        Ok(buffer)
    }

    // Fetches the image configuration JSON as a string from cache
    pub fn fetch_config(&mut self, cache_manager: &CacheManager) -> Result<String, FetchError> {
        let mut path = self.get_image_folder_path(cache_manager)?;
        path.push(constants::CACHE_CONFIG_FILE_NAME);

        let mut config_str = String::new();

        // Open the cached config file and read the bytes from it
        let bytes_read = File::open(path)
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

    // Fetches the image manifest JSON as a string from cache
    pub fn fetch_manifest(&mut self, cache_manager: &CacheManager) -> Result<String, FetchError> {
        let mut path = self.get_image_folder_path(cache_manager)?;
        path.push(constants::CACHE_MANIFEST_FILE_NAME);

        let mut manifest_str = String::new();

        // Open the cached manifest file and read the bytes from it
        let bytes_read = File::open(path)
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

    // Fetches the image layers as an array of byte arrays from the cache
    pub fn fetch_layers(&mut self, cache_manager: &CacheManager) -> Result<Vec<Vec<u8>>, FetchError> {
        let mut folder_path = self.get_image_folder_path(cache_manager)?;
        folder_path.push(constants::CACHE_LAYERS_FOLDER_NAME);

        // Get all the files from the layers folder of an image
        let layer_files = fs::read_dir(folder_path).map_err(|err|
            FetchError::LayerError("Failed to detect cached layer files".to_string()))?;

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
                                "Failed to open the layer file with index {}", index.to_string())))?
                        .read_to_end(&mut buffer)
                            .map_err(|err| FetchError::LayerError(format!(
                                "Failed to read layer file with index {}: {:?}", index.to_string(), err)))?;

                    // If no bytes were read, throw an error
                    if (bytes_read == 0) {
                        return Err(FetchError::LayerError(format!("
                            No data was read from layer file with index {}", index.to_string())));
                    }
                    layers.push(buffer);
                }
                Err(_) => {
                    return Err(FetchError::LayerError(format!(
                        "Layer file with index {} could not be read", index.to_string())));
                }
            }
        }
        Ok(layers)
    }

    // Fetches the 'ENV' expressions of an image which are stored in cache
    pub fn fetch_env_expressions(&mut self, cache_manager: &CacheManager) -> Result<Vec<String>, FetchError> {
        let mut path = self.get_image_folder_path(cache_manager)?;
        path.push(constants::ENV_CACHE_FILE_NAME);

        // Open the env.sh file
        let file = File::open(path).map_err(|err|
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

    pub fn fetch_cmd_expressions(&mut self, cache_manager: &CacheManager) -> Result<Vec<String>, FetchError> {
        let mut path = self.get_image_folder_path(cache_manager)?;
        path.push(constants::CMD_CACHE_FILE_NAME);

        // Open the cmd.sh file
        let file = File::open(path).map_err(|err|
            FetchError::EnvCmdError(format!(
                "Failed to open the cached file containing the 'CMD' expressions: {:?}", err)))?;

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

    fn find_image_folder_path(&self, cache_manager: &CacheManager) -> Result<PathBuf, FetchError> {
        // Get the hash of the image
        let hash = self.find_image_hash(cache_manager).map_err(|err|
            FetchError::ImageError(format!("{:?}", err)))?;

        // Build the path of the image cache folder
        let mut path = self.cache_path.clone();
        path.push(hash);
        
        Ok(path)
    }

    fn find_image_hash(&self, cache_manager: &CacheManager) -> Result<String, FetchError> {
        let hash = cache_manager.get_image_hash(&self.image_uri).ok_or_else(||
            FetchError::HashError(format!("Failed to get image hash from cache: image is not cached.")))?;

        Ok(hash)
    }
}