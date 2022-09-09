// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    ops::Deref
};

use serde::{de::DeserializeOwned, Serialize};
use sha2::Digest;

use crate::image::{Image, ImageCacheFetch};
use crate::extract;

use oci_distribution::{client::ImageData, manifest::IMAGE_CONFIG_MEDIA_TYPE};

/// Root folder for the cache
pub const CACHE_ROOT_FOLDER: &str = "XDG_DATA_HOME";
/// For testing purposes, use $HOME as cache root folder
pub const HOME_ENV_VAR: &str = "HOME";
/// Name of the cache index file which stores the (image URI <-> image hash) mappings
pub const CACHE_INDEX_FILE_NAME: &str = "index.json";
/// The name of the folder used by the cache to store the image layers
pub const CACHE_LAYERS_FOLDER_NAME: &str = "layers";
/// The name of the image config file from the cache
pub const CACHE_CONFIG_FILE_NAME: &str = "config.json";
/// The name of the image manifest file from the cache
pub const CACHE_MANIFEST_FILE_NAME: &str = "manifest.json";
/// Length of SHA256 algorithm output hash
pub const SHA256_HASH_LEN: u64 = 64;
/// If true, enclave_build/cache/ will be used as cache
///
/// If false, the default path is used
pub const TEST_MODE_ENABLED: bool = true;

#[derive(Debug)]
pub enum Error {
    ReadWriteError(std::io::Error),
    /// Serialization/Deserialization error
    SerdeError(serde_json::Error),
    /// Error when creating the cache
    CacheBuildError(String),
    /// Error when storing image data to cache
    CacheStoreError(String),
    /// Error when fetching image data from cache
    CacheFetchError(String),
    /// Error thrown if there was an issue when calculating the default cache
    /// folder path
    DefaultCacheRootPathError(String),
    /// Error thrown if an image is not correctly cached
    ValidateError(String)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ReadWriteError(err) => write!(f, "Read/Write error: {:?}", err),
            Error::SerdeError(err) => {
                write!(f, "Serialization/Deserialization error: {:?}", err)
            },
            Error::CacheBuildError(msg) => write!(f, "Failed to build cache: {}", msg),
            Error::CacheFetchError(msg) => write!(f, "Failed to fetch image from cache: {}", msg),
            Error::CacheStoreError(msg) => write!(f, "Failed to store image in cache: {}", msg),
            Error::DefaultCacheRootPathError(msg) => write!(f,
                "Failed to determina the default cache root path: {}", msg),
            Error::ValidateError(msg) => write!(f, "Image is not correctly cached: {}", msg)
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Struct which provides operations with the local cache.
///
/// The index.json file is located in the cache root folder and keeps track of images stored in cache.
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
/// {IMAGE_FOLDER_PATH}/{MANIFEST_DIGEST_HASH} - the manifest.json of the image\
/// {IMAGE_FOLDER_PATH}/layers - folder containing all layers, each in a separate gzip compressed file\
/// {IMAGE_FOLDER_PATH}/image_file - gzip compressed file containing all layers combined, as an array of bytes\
/// {IMAGE_FOLDER_PATH}/env.sh - contains ENV expressions of the image\
/// {IMAGE_FOLDER_PATH}/cmd.sh - contains CMD expressions of the image
#[derive(Clone)]
pub struct CacheManager {
    /// The root folder of the cache
    root_path: PathBuf,

    /// A map storing the cached images, with the map entry format being (image_reference, image_hash)
    cached_images: HashMap<String, String>,
}

impl CacheManager {
    /// Creates a new CacheManager instance and returns it. As argument, a path to the root folder
    /// of the cache should be provided.
    /// 
    /// Apart from that, the function also creates (if not already created) all folders from the path
    /// specified as argument.
    /// 
    /// If an index.json file exists at the path, it loads the file's contents into the 'cached_images'
    /// field. If not, a new index.json file is created at that path.
    pub fn new<P: AsRef<Path>>(root_path: P) -> Result<Self> {
        // Create all missing folders, if not already created
        fs::create_dir_all(&root_path).map_err(|err| Error::CacheBuildError(format!("{:?}", err)))?;

        // Create the index.json file
        let _ = Self::create_index_file(&root_path).map_err(|err| Error::CacheBuildError(format!("{:?}", err)))?;

        // Read the index.json file into a hashmap
        let cached_images = Self::read_cached_images_from_index_file(&root_path)
            .map_err(|err| Error::CacheBuildError(format!("{:?}", err)))?;

        Ok(
            Self {
                root_path: root_path.as_ref().to_path_buf(),
                cached_images
            }
        )
    }

    /// Stores the image data provided as argument in the cache at the folder pointed
    /// by the 'root_path' field.
    pub fn store_image<S: AsRef<str>>(&mut self, image_name: S, image_data: &ImageData) -> Result<()> {
        let image_hash = extract::extract_image_hash(&image_data)
            .map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;

        // Create the folder where the image data will be stored
        let target_path = self.root_path.clone().join(&image_hash);
        fs::create_dir(&target_path).map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;

        // Create the 'layers' folder and store the layers in it
        let layers_path = target_path.clone().join(CACHE_LAYERS_FOLDER_NAME);
        let layers = extract::extract_layers(&image_data)
            .map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;
        for layer in layers {
            // Each layer file will be named after the layer's digest hash
            let layer_file_path = layers_path.join(layer.sha256_digest());
            File::create(&layer_file_path).map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?
                .write_all(&layer.data).map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;
        }

        // Store the manifest
        let manifest_json = extract::extract_manifest_json(&image_data)
            .map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;
        File::create(&target_path.join(CACHE_MANIFEST_FILE_NAME)).map_err(|err|
            Error::CacheStoreError(format!("{:?}", err)))?
            .write_all(manifest_json.as_bytes())
                .map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;

        // Store the config
        let config_json = extract::extract_config_json(&image_data)
            .map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;
        File::create(&target_path.join(CACHE_CONFIG_FILE_NAME)).map_err(|err|
            Error::CacheStoreError(format!("{:?}", err)))?
            .write_all(config_json.as_bytes())
                .map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;

        // If all image data was successfully stored, add the image to the index.json file and
        // the hashmap
        let image_ref = Image::image_reference(image_name)
            .map_err(|err| Error::CacheStoreError(format!("{:?}", err)))?;
        self.cached_images.insert(image_ref.whole(), image_hash);

        // Create (or open if it's already created) the index.json file
        let index_file = File::create(self.root_path.clone().join(CACHE_INDEX_FILE_NAME))
            .map_err(|err| Error::CacheStoreError(format!(
                "The cache index.json file could not be written: {:?}", err)))?;

        // Write the hashmap (the image URI <-> image hash mappings) to the index.json file
        serde_json::to_writer(index_file, &self.cached_images)
            .map_err(|err| Error::CacheStoreError(format!(
                "Failed to write to the index.json file: {:?}", err)))?;

        Ok(())
    }

    /// Fetches the image data from cache as an ImageCacheFetch struct. The image manifest
    /// and config are returned as raw JSON strings.
    /// 
    /// If the data is not correctly cached or a file is missing, it returns an error.
    /// 
    /// If the image is not cached, it does not attempt to pull the image from remote.
    pub fn fetch_image<S: AsRef<str>>(&self, image_name: S) -> Result<ImageCacheFetch> {
        // Find the path to the cache folder where the image is stored
        let target_path = self.get_image_folder_path(&image_name)?;
        // Get the hash of the image
        let image_hash = self.get_image_hash_from_name(&image_name)
            .ok_or_else(|| Error::CacheFetchError("Failed to determine image hash.".to_string()))?;

        // Get the image layers from cache
        let layers = Self::fetch_layers(&target_path)
            .map_err(|err| Error::CacheFetchError(format!("{:?}", err)))?;

        // Read the JSON string from the manifest file
        let manifest_path = target_path.clone().join(CACHE_MANIFEST_FILE_NAME);
        let mut manifest_json = String::new();
        File::open(&manifest_path).map_err(|err| Error::CacheFetchError(format!("{:?}", err)))?
                .read_to_string(&mut manifest_json).map_err(|err|
                    Error::CacheFetchError(format!("{:?}", err)))?;

        // Read the JSON string from the config file
        let config_path = target_path.clone().join(CACHE_CONFIG_FILE_NAME);
        let mut config_json = String::new();
        File::open(&config_path).map_err(|err| Error::CacheFetchError(format!("{:?}", err)))?
            .read_to_string(&mut config_json).map_err(|err|
                Error::CacheFetchError(format!("{:?}", err)))?;
        
        // If the manifest or config JSON strings are empty, return error
        if manifest_json.is_empty() || config_json.is_empty() {
            return Err(Error::CacheFetchError("Empty manifest or config file.".to_string()));
        }

        Ok(
            ImageCacheFetch::new(
                image_hash,
                layers,
                manifest_json,
                config_json
            )
        )
    }

    /// Returns the CMD and ENV expressions (in this order) of an image by extracting them
    /// from the cached image data
    pub fn fetch_expressions<S: AsRef<str>>(&self, image_name: S) -> Result<(Vec<String>, Vec<String>)> {
        let target_path = self.get_image_folder_path(&image_name)?;

        // The ENV and CMD expressions are located in the config file of the image, so fetch it from
        // the cache
        let mut config_json = String::new();
        File::open(target_path.join(CACHE_CONFIG_FILE_NAME)).map_err(|err|
            Error::CacheFetchError(format!("{:?}", err)))?
            .read_to_string(&mut config_json).map_err(|err| Error::CacheFetchError(
                format!("{:?}", err)))?;
        if config_json.is_empty() {
            return Err(Error::CacheFetchError("Config file is empty.".to_string()));
        }

        let env = self.get_expressions(&config_json, "ENV")?;
        let cmd = self.get_expressions(&config_json, "CMD")?;

        Ok((cmd, env))
    }

    /// Determines if an image is stored correctly in the cache represented by the current CacheManager object.
    pub fn is_cached<S: AsRef<str>>(&self, image_name: S) -> Result<()> {
        // If the image is not in the index.json file, then it is definitely not cached
        let image_hash = self.get_image_hash_from_name(&image_name);
        if image_hash.is_none() {
            return Err(Error::ValidateError("Image missing from index.json file.".to_string()));
        }
        // The image is theoretically cached, but check the manifest, config and layers to validate
        // that the image data is stored correctly

        let image_folder_path = self.root_path.clone().join(&image_hash.unwrap());

        // First validate the manifest
        // Since the struct pulled by the oci_distribution API does not contain the manifest digest,
        // and another HTTP request should be made to get the digest, just check that the manifest file
        // exists and is not empty
        let manifest_path = image_folder_path.join(CACHE_MANIFEST_FILE_NAME);
        let mut manifest_str = String::new();
        File::open(&manifest_path).map_err(|err| Error::ValidateError(format!("{:?}", err)))?
            .read_to_string(&mut manifest_str).map_err(|err|
                Error::ValidateError(format!("{:?}", err)))?;
        if manifest_str.is_empty() {
            return Err(Error::ValidateError("The manifest file is empty.".to_string()));
        }

        // The manifest is checked, so now validate the layers
        self.validate_layers(
            &image_folder_path.clone().join(CACHE_LAYERS_FOLDER_NAME),
            &manifest_str
        )?;

        // Finally, check that the config is correctly cached
        // This is done by applying a hash function on the config file contents and comparing the
        // result with the config digest from the manifest
        let config_path = image_folder_path.join(CACHE_CONFIG_FILE_NAME);
        let mut config_str = String::new();
        File::open(&config_path).map_err(|err| Error::ValidateError(format!("{:?}", err)))?
            .read_to_string(&mut config_str).map_err(|err| Error::ValidateError(format!("{:?}", err)))?;
        
        let manifest_obj: serde_json::Value = serde_json::from_str(manifest_str.as_str())
            .map_err(|_| Error::ValidateError("Could not parse manifest JSON.".to_string()))?;

        // Extract the config digest hash
        let config_digest = manifest_obj
            .get("config").ok_or_else(||
                Error::ValidateError("'config' field missing from image manifest.".to_string()))?
            .get("digest").ok_or_else(||
                Error::ValidateError("'digest' field missing from image manifest.".to_string()))?
            .as_str().ok_or_else(||
                Error::ValidateError("Failed to get config digest from image manifest.".to_string()))?
            .strip_prefix("sha256:").ok_or_else(||
                Error::ValidateError("Failed to get config digest from image manifest.".to_string()))?
            .to_string();
        // Compare the two digests
        if config_digest != format!("{:x}", sha2::Sha256::digest(config_str.as_bytes())) {
            return Err(Error::ValidateError("Config content digest and manifest digest do not match".to_string()));
        }

        Ok(())
    }

    /// Validates that the image layers are cached correctly by checking them with the layer descriptors
    /// from the image manifest.
    fn validate_layers<P: AsRef<Path>>(&self, layers_path: P, manifest_str: &String) -> Result<()> {
        let manifest_obj: serde_json::Value = serde_json::from_str(manifest_str.as_str())
            .map_err(|err| Error::ValidateError("Manifest serialization failed".to_string()))?;
        
        // Try to get the layer list from the manifest JSON
        let layers_vec: Vec<serde_json::Value> = manifest_obj
            .get("layers").ok_or_else(||
                Error::ValidateError(format!("'layers' field missing from manifest JSON.")))?
            .as_array()
            .ok_or_else(|| Error::ValidateError("Manifest deserialize error.".to_string()))?
            .to_vec();

        // Get the cached layers as a HashMap mapping a layer digest to the corresponding layer file
        let mut cached_layers: HashMap<String, File> = HashMap::new();

        fs::read_dir(layers_path)
            .map_err(|err|
                Error::ValidateError(format!("Failed to get image layers: {:?}", err)))?
            .into_iter()
            .filter(|entry| entry.is_ok())
            // Get only the files
            .filter(|entry| entry.as_ref().unwrap().path().is_file())
            .map(|file| (File::open(file.as_ref().unwrap().path()), file.unwrap().file_name()))
            .filter(|(file, name)| file.is_ok())
            .map(|(file, name)| (file.unwrap(), name.into_string().unwrap()))
            // Map a layer digest to the layer file
            .for_each(|(file, name)| {
                cached_layers.insert(name, file);
            });

        // Iterate through each layer found in the image manifest and validate that it is stored in
        // the cache by checking the digest
        for layer_obj in layers_vec {
            // Read the layer size from the manifest
            let layer_size: u64 = layer_obj
                .get("size").ok_or_else(||
                    Error::ValidateError("Image layer size not found in manifest.".to_string()))?
                .as_u64()
                .ok_or_else(|| Error::ValidateError("Layer info extract error.".to_string()))?;

            // Read the layer digest from the manifest
            let layer_digest: String = layer_obj
                .get("digest").ok_or_else(||
                    Error::ValidateError("Image layer digest not found in manifest".to_string()))?
                .as_str().ok_or_else(||
                    Error::ValidateError("Layer info extract error".to_string()))?
                .strip_prefix("sha256:").ok_or_else(||
                    Error::ValidateError("Layer info extract error".to_string()))?
                .to_string();

            // Get the cached layer file matching the digest
            // If not present, then a layer file is missing, so return Error
            let layer_file = cached_layers.get(&layer_digest)
                    .ok_or_else(|| Error::ValidateError("Layer missing from cache.".to_string()))?;

            // Get the size in bytes of the layer file
            let size = layer_file.deref()
                .metadata().map_err(|_|
                    Error::ValidateError("Failed to extract metadata from layer file.".to_string()))?
                .len();

            // Check that the sizes match
            if layer_size != size {
                return Err(Error::ValidateError("Layer not valid".to_string()));
            }
        }

        Ok(())
    }

    /// Extracts the specified expressions ("ENV" or "CMD") from the config JSON
    /// string given as parameter
    fn get_expressions<S: AsRef<str>, T: AsRef<str>>(&self, config_json: S, expr: T) -> Result<Vec<String>> {
        // Deserialize the config string into a JSON Value
        let config: serde_json::Value = serde_json::from_str(config_json.as_ref()).map_err(|err| {
            Error::CacheFetchError(format!(
                "Failed to extract ENV expressions from image: {:?}",
                err
            ))
        })?;

        let field = match expr.as_ref().to_string().as_str() {
            "ENV" => "Env",
            "CMD" => "Cmd",
            &_ => ""
        };

        // Try to parse the config JSON for the specified field
        let array = match config
            .get("container_config")
            .ok_or_else(|| {
                Error::CacheFetchError(
                    "'container_config' field is missing in the config JSON.".to_string(),
                )
            })?
            .get(field)
            .ok_or_else(|| {
                Error::CacheFetchError(format!("{} field is missing in the configuration JSON.", field))
            })?
            // Try to extract the array of expressions
            .as_array()
        {
            None => Err(Error::CacheFetchError(
                format!("Failed to extract {} expressions from image.", expr.as_ref().to_string()),
            )),
            Some(array) => {
                let strings: Vec<String> = array
                    .iter()
                    .map(|json_value| {
                        let mut string = json_value.to_string();
                        // Remove the quotes from the beginning and the end of the expression
                        string.pop();
                        string.remove(0);
                        string
                    })
                    .collect();
                Ok(strings)
            }
        };

        array
    }

    /// Returns the default root folder path of the cache.
    /// 
    /// If the TEST_MODE_ENABLED constant is set to true, ./cache/
    /// is used as cache root.
    pub fn get_default_cache_root_path() -> Result<PathBuf> {
        // For testing, the cache will be saved to the local directory
        if TEST_MODE_ENABLED {
            let mut local_path = std::env::current_dir().unwrap();
            local_path.push("cache");
            return Ok(local_path);
        }

        // Try to use XDG_DATA_HOME as default root
        let root = match std::env::var_os(CACHE_ROOT_FOLDER) {
            Some(val) => val.into_string()
                .map_err(|err| Error::DefaultCacheRootPathError(format!("{:?}", err)))?,
            // If XDG_DATA_HOME is not set, use {HOME}/.local/share as specified in
            // https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
            None => {
                let home_folder = std::env::var_os("HOME")
                    .ok_or_else(|| Error::DefaultCacheRootPathError(
                        "HOME environment variable is not set.".to_string()))?;
                format!("{}/.local/share/", home_folder.into_string()
                    .map_err(|err| Error::DefaultCacheRootPathError(format!("{:?}", err)))?)
            }
        };

        let mut path = PathBuf::from(root);
        // Add the additional path to the root
        path.push(".nitro_cli/container_cache");

        Ok(path)
    }

    /// Returns the image hash corresponding to the image URI, if available in the hashmap.
    fn get_image_hash<S: AsRef<str>>(&self, uri: S) -> Option<String> {
        self.cached_images.get(uri.as_ref()).map(|val| val.to_string())
    }

    /// Returns the image hash (if available in the CacheManager's hashmap) taking the image
    /// name as parameter.
    fn get_image_hash_from_name<S: AsRef<str>>(&self, name: S) -> Option<String> {
        let image_ref = Image::image_reference(&name);
        if image_ref.is_err() {
            return None;
        }

        self.get_image_hash(&image_ref.unwrap().whole())
    }

    /// Returns the path to an image folder in the cache.
    /// 
    /// This is achieved by looking up in the hashmap by the image reference in order
    /// to find the image hash.
    fn get_image_folder_path<S: AsRef<str>>(&self, image_name: S) -> Result<PathBuf> {
        let image_ref = Image::image_reference(image_name)
            .map_err(|err| Error::CacheFetchError(format!("{:?}", err)))?;

        let image_hash = self.get_image_hash(&image_ref.whole()).ok_or_else(||
            Error::CacheFetchError("Could not find image hash in index.json file.".to_string())    
        )?;

        Ok(self.root_path.clone().join(image_hash))
    }

    /// Opens and returns the index.json file at the path (parent folder)
    /// specified in the argument.
    /// 
    /// If the file is missing, it creates and returns it.
    fn create_index_file<P: AsRef<Path>>(path: P) -> Result<File> {
        let index_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(path.as_ref().to_path_buf().join(CACHE_INDEX_FILE_NAME))
            .map_err(|err| Error::CacheBuildError(format!("Failed to create index.json file: {:?}", err)))?;

        Ok(index_file)
    }

    /// Reads the index.json file from the path (parent folder) specified in the argument in a hashmap
    /// and returns it.
    /// 
    /// If no such file is encountered, it returns an error. If the file exists, but is empty,
    /// it returns an empty hashmap.
    fn read_cached_images_from_index_file<P: AsRef<Path>>(path: P) -> Result<HashMap<String, String>> {
        let mut contents = String::new();
        // Try to open the index.json file and read the contents
        OpenOptions::new()
            .read(true)
            .open(path.as_ref().to_path_buf().join(CACHE_INDEX_FILE_NAME))
            .map_err(|err| Error::CacheBuildError(format!("Failed to open the index.json file: {:?}", err)))?
            .read_to_string(&mut contents)
            .map_err(|err| Error::CacheBuildError(format!("Failed to read from the index.json file: {:?}", err)))?;

        // If the index.json file is empty, return an empty hashmap
        if contents.is_empty() {
            return Ok(HashMap::new());
        }

        // Try to deserialize the JSON string into a HashMap
        let hashmap: HashMap<String, String> = serde_json::from_str(contents.as_str())
            .map_err(|err| Error::CacheBuildError(format!("Failed to deserialize index.json file: {:?}", err)))?;

        Ok(hashmap)
    }

    /// Fetches the layer files from the cache image folder provided as argument
    fn fetch_layers<P: AsRef<Path>>(path: P) -> Result<Vec<Vec<u8>>> {
        let mut found_err: bool = false;

        // Get all layer files from the folder
        let layers_tmp = fs::read_dir(
                // Create the path to the layers folder
                path.as_ref().to_path_buf().join(CACHE_LAYERS_FOLDER_NAME)
            )
            .map_err(|err| Error::CacheFetchError(format!("Failed to get layers from cache: {:?}", err)))?
            .into_iter()
            .filter(|entry| entry.is_ok())
            // Gen only the files
            .filter(|entry| entry.as_ref().unwrap().path().is_file())
            // Additional check to verify that it is indeed a layer
            // The layer files are named after their hash
            .filter(|file| file.as_ref().unwrap().file_name().len() == SHA256_HASH_LEN as usize)
            // Open each file
            .map(move |file| {
                let res = File::open(file.as_ref().unwrap().path());
                if res.is_err() {
                    found_err = true;
                    res.map_err(|err| Error::CacheFetchError("Failed to open layer file".to_string()))
                } else {
                    Ok(res.unwrap())
                }
            });

        // If not all layer files could be opened, return error
        if !found_err {
            return Err(Error::CacheFetchError("Failed to get layers from cache.".to_string()));
        }

        // If the if statement from above was not entered, then we can unwrap all
        let layer_files = layers_tmp
            .into_iter()
            .map(|file| file.unwrap());


        let mut layers = Vec::new();

        // Iterate through the layer files
        for mut layer in layer_files {
            let mut data = Vec::new();
            let bytes_read = layer.read_to_end(&mut data)
                .map_err(|err| Error::CacheFetchError(format!("Failed to get layers from cache: {:?}", err)))?;

            // If no bytes were read, throw an error
            if bytes_read == 0 {
                return Err(Error::CacheFetchError("No bytes read from layer.".to_string()));
            }

            // Add this layer to the array of layers
            layers.push(data);
        }

        Ok(layers)
    }
}

fn deserialize_from_file<P: AsRef<Path>, T: DeserializeOwned>(path: P) -> Result<T> {
    let path = path.as_ref();
    let manifest_file =
        std::io::BufReader::new(fs::File::open(path).map_err(|err| Error::ReadWriteError(err))?);
    let manifest = serde_json::from_reader(manifest_file).map_err(|err| Error::SerdeError(err))?;

    Ok(manifest)
}

fn deserialize_from_reader<R: Read, T: DeserializeOwned>(reader: R) -> Result<T> {
    let manifest = serde_json::from_reader(reader).map_err(|err| Error::SerdeError(err))?;

    Ok(manifest)
}

fn serialize_to_file<P: AsRef<Path>, T: Serialize>(
    path: P,
    object: &T,
    pretty: bool,
) -> Result<()> {
    let path = path.as_ref();
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .map_err(|err| Error::ReadWriteError(err))?;

    let file = std::io::BufWriter::new(file);

    match pretty {
        true => serde_json::to_writer_pretty(file, object).map_err(|err| Error::SerdeError(err))?,
        false => serde_json::to_writer(file, object).map_err(|err| Error::SerdeError(err))?,
    };

    Ok(())
}

fn serialize_to_writer<W: Write, T: Serialize>(
    writer: &mut W,
    object: &T,
    pretty: bool,
) -> Result<()> {
    match pretty {
        true => {
            serde_json::to_writer_pretty(writer, object).map_err(|err| Error::SerdeError(err))?
        }
        false => serde_json::to_writer(writer, object).map_err(|err| Error::SerdeError(err))?,
    };

    Ok(())
}

fn serialize_to_string<T: Serialize>(object: &T, pretty: bool) -> Result<String> {
    Ok(match pretty {
        true => serde_json::to_string_pretty(object).map_err(|err| Error::SerdeError(err))?,
        false => serde_json::to_string(object).map_err(|err| Error::SerdeError(err))?,
    })
}
