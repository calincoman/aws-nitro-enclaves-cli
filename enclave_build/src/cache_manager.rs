// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{HashMap, hash_map::Entry};
use std::fs::File;
use std::io::Read;

use oci_distribution::Reference;
use oci_distribution::client::ImageData;

use crate::utils::{ExtractLogic, Image};

#[derive(Debug, PartialEq)]
pub enum CacheError {
    FindImageError(String),
    ImageFileError(String),
    DataCacheError(String),
    ArgumentError(String),

    CacheCreationError(String),
    UriNotFoundError(String),
    IndexFileNotFound(String),
    IndexFileParseError(String),
}

/// (Idea)
/// Apart from keeping the (docker URI <-> image hash) mapping in a JSON file, my idea was to also use a
/// runtime-exclusive HashMap to store these mappings, in order to not open and read the JSON file for each query
pub struct CacheManager {
    values: HashMap<String, String>,
}

impl CacheManager {
    /// Create a new CacheManager
    pub fn new() -> CacheManager {
        Self {
            values: HashMap::new(),
        }
    }

    pub fn get_values(&self) -> &HashMap<String, String> {
        &self.values
    }

    pub fn get_values_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.values
    }

    /// Return the image hash corresponding to the image URI if available, or else return an error
    pub fn get_image_hash(&self, uri: &String) -> Result<String, CacheError> {
        let image_hash: Option<&String> = match self.values.get(uri) {
            Some(img_hash) => Some(img_hash),
            None => None,
        };

        match image_hash {
            Some(hash) => Ok(hash.to_string()),
            None => Err(CacheError::UriNotFoundError("Image URI was not found. Image is probably not cached.".to_string()))
        }
    }

    /// Record the new image that was added to the cache
    pub fn record_image(&mut self, image: &Image) -> Result<(), CacheError> {
        // Get the image's hash (digest) and URI
        let image_hash = match ExtractLogic::extract_image_hash(image.data()) {
            Ok(aux) => aux,
            Err(err) => {
                return Err(CacheError::DataCacheError(format!("Failed to record image to hash: {:?}", err)));
            }
        };
        let image_uri = image.reference().whole();

        self.add_entry(&image_uri, &image_hash);

        Ok(())
    }

    /// Add a new image URI <-> hash entry to the hashmap
    pub fn add_entry(&mut self, uri: &String, hash: &String) {
        self.values.insert(uri.to_string(), hash.to_string());
    }

    /// Populate the hashmap with the values from a JSON index file which contains the mappings
    pub fn populate_hashmap(&mut self, index_file_path: &String) -> Result<(), CacheError> {
        let index_file = File::open(index_file_path);
        match &index_file {
            Ok(file) => (),
            Err(err) => {
                return Err(CacheError::IndexFileNotFound(format!(
                    "A JSON file containing the mappings for the cache was not found: {}", err)));
            },
        };
        // Read the JSON string from the file
        let mut file = index_file.unwrap();
        let mut json_string = String::new();
        file.read_to_string(&mut json_string);

        // Try to deserialize the JSON into a HashMap
        let map: HashMap<String, String> = match serde_json::from_str(json_string.as_str()) {
            Ok(m) => m,
            Err(err) => {
                return Err(CacheError::IndexFileParseError(format!(
                    "The JSON mappings file could not be parsed: {}", err)));
            }
        };

        self.values = map;

        Ok(())
    }

}