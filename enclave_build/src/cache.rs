// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    ops::Deref
};

use sha2::Digest;

use crate::image::{self, Image};
use crate::extract;

use oci_distribution::{client::ImageData};

/// Root folder for the cache
pub const CACHE_ROOT_FOLDER: &str = "XDG_DATA_HOME";
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

#[derive(Debug)]
pub enum Error {
    CacheInitError(io::Error),
    ImageNotCached(String),
    ConfigEmpty,
    ManifestEmpty,
    HashMissing,
    FetchLayers(io::Error),
    FetchManifest(io::Error),
    FetchConfig(io::Error),
    /// Error when storing image data to cache
    CacheStoreError(io::Error),
    /// Error thrown if there was an issue when calculating the default cache folder path
    CacheRootPath(String),
    Serde(serde_json::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::CacheInitError(err) => write!(f, "CacheManager could not be initialized: {:?}", err),
            Error::ImageNotCached(msg) => write!(f, "Image is not correctly cached: {}", msg),
            Error::ConfigEmpty => write!(f, "The cached config file is empty."),
            Error::ManifestEmpty => write!(f, "The cached manifest file is empty."),
            Error::HashMissing => write!(f, "The image hash is missing from the cache index file."),
            Error::Serde(err) => write!(f, "Serialization/Deserialization error: {:?}", err),
            Error::FetchLayers(err) => write!(f, "Failed to fetch layers from cache: {:?}", err),
            Error::FetchManifest(err) => write!(f, "Failed to fetch manifest from cache: {:?}", err),
            Error::FetchConfig(err) => write!(f, "Failed to fetch config from cache: {:?}", err),
            Error::CacheStoreError(err) => write!(f, "Failed to store image in cache: {:?}", err),
            Error::CacheRootPath(msg) => write!(f,
                "Failed to determine the default cache root path: {}", msg)
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serde(err)
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
        fs::create_dir_all(&root_path).map_err(Error::CacheInitError)?;

        let mut contents = String::new();

        // Try to open the index.json file and read the contents.
        // If the file is missing, create it.
        OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(root_path.as_ref().to_path_buf().join(CACHE_INDEX_FILE_NAME))
            .map_err(Error::CacheInitError)?
            .read_to_string(&mut contents)
            .map_err(Error::CacheInitError)?;

        // If the index.json file is empty, return an empty hashmap
        if contents.is_empty() {
            return Ok(
                Self {
                    root_path: root_path.as_ref().to_path_buf(),
                    cached_images: HashMap::new()
                }
            );
        }

        // Try to deserialize the JSON string into a HashMap
        let cached_images: HashMap<String, String> = serde_json::from_str(contents.as_str())
            .map_err(|err| Error::CacheInitError(io::Error::new(std::io::ErrorKind::Other,
                format!("Failed to read the cached images map from index.json: {:?}", err))))?;

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
            .map_err(|err| Error::CacheStoreError(io::Error::new(
                io::ErrorKind::Other, format!("{:?}", err))))?;

        // Create the folder where the image data will be stored
        let target_path = self.root_path.clone().join(&image_hash);
        fs::create_dir_all(&target_path).map_err(Error::CacheStoreError)?;

        // Create the 'layers' folder and store the layers in it
        let layers_path = target_path.clone().join(CACHE_LAYERS_FOLDER_NAME);
        fs::create_dir_all(&layers_path).map_err(Error::CacheStoreError)?;

        let layers = extract::extract_layers(&image_data)
            .map_err(|err| Error::CacheStoreError(io::Error::new(
                io::ErrorKind::Other, format!("{:?}", err))))?;
        for layer in layers {
            // Each layer file will be named after the layer's digest hash
            let layer_file_path = layers_path.join(format!("{:x}", sha2::Sha256::digest(&layer.data)));
            File::create(&layer_file_path).map_err(Error::CacheStoreError)?
                .write_all(&layer.data).map_err( Error::CacheStoreError)?;
        }

        // Store the manifest
        let manifest_json = extract::extract_manifest_json(&image_data)
            .map_err(|err| Error::CacheStoreError(io::Error::new(io::ErrorKind::Other,
                format!("{:?}", err))))?;

        File::create(&target_path.join(CACHE_MANIFEST_FILE_NAME)).map_err(Error::CacheStoreError)?
            .write_all(manifest_json.as_bytes())
                .map_err(Error::CacheStoreError)?;

        // Store the config
        let config_json = extract::extract_config_json(&image_data)
            .map_err(|err| Error::CacheStoreError(io::Error::new(io::ErrorKind::Other,
                format!("{:?}", err))))?;
        File::create(&target_path.join(CACHE_CONFIG_FILE_NAME)).map_err(Error::CacheStoreError)?
            .write_all(config_json.as_bytes())
                .map_err(Error::CacheStoreError)?;

        // If all image data was successfully stored, add the image to the index.json file and
        // the hashmap
        let image_ref = Image::image_reference(image_name)
            .map_err(|err| Error::CacheStoreError(io::Error::new(io::ErrorKind::Other,
                format!("{:?}", err))))?;
        self.cached_images.insert(image_ref.whole(), image_hash);

        // Create (or open if it's already created) the index.json file
        let index_file = File::create(self.root_path.clone().join(CACHE_INDEX_FILE_NAME))
            .map_err(Error::CacheStoreError)?;

        // Write the hashmap (the image URI <-> image hash mappings) to the index.json file
        serde_json::to_writer(index_file, &self.cached_images)
            .map_err(|err| Error::CacheStoreError(io::Error::new(io::ErrorKind::Other,
                format!("{:?}", err))))?;

        Ok(())
    }

    /// Fetches the image data from cache as an Image struct.
    /// 
    /// If the 'with_layers' argument is true, the returning struct also contains the layers, otherwise
    /// it contains just the image hash, manifest and config.
    /// 
    /// If the data is not correctly cached or a file is missing, it returns an error.
    /// 
    /// If the image is not cached, it does not attempt to pull the image from remote.
    pub fn fetch_image<S: AsRef<str>>(&self, image_name: S, with_layers: bool) -> Result<Image> {
        // Check if the image is cached
        self.is_cached(&image_name)?;

        // Get the hash of the image
        let image_hash = self.get_image_hash_from_name(&image_name)
            .ok_or_else(|| Error::HashMissing)?;

        // Fetch the manifest JSON string from cache
        let manifest_json = self.fetch_manifest(&image_name)?;

        // Fetch the config JSON string from cache
        let config_json = self.fetch_config(&image_name)?;

        // Get the image layers from cache (only if the 'with_layers' parameter is true)
        let layers_ret = match with_layers {
            true => {
                let layers = self.fetch_layers(&image_name)?;
                Some(layers)
            }
            false => None
        };

        Ok(
            Image::new(
                image_hash,
                layers_ret,
                image::deserialize_from_reader(manifest_json.as_bytes())?,
                image::deserialize_from_reader(config_json.as_bytes())?
            )
        )
    }

     /// Determines if an image is stored correctly in the cache represented by the current CacheManager object.
     pub fn is_cached<S: AsRef<str>>(&self, image_name: S) -> Result<bool> {
        // If the image is not in the index.json file, then it is definitely not cached
        let image_hash = self.get_image_hash_from_name(&image_name);
        if image_hash.is_none() {
            return Err(Error::ImageNotCached("Image hash missing from index.json file.".to_string()));
        }
        // The image is theoretically cached, but check the manifest, config and layers to validate
        // that the image data is stored correctly

        let image_folder_path = self.root_path.clone().join(&image_hash.unwrap());

        // First validate the manifest
        // Since the struct pulled by the oci_distribution API does not contain the manifest digest,
        // and another HTTP request should be made to get the digest, just check that the manifest file
        // exists and is not empty
        let manifest_str = self.fetch_manifest(&image_name).map_err(|err|
            Error::ImageNotCached(format!("{:?}", err)))?;

        // The manifest is checked, so now validate the layers
        self.validate_layers(
            &image_folder_path.clone().join(CACHE_LAYERS_FOLDER_NAME),
            &manifest_str
        )?;

        // Finally, check that the config is correctly cached
        // This is done by applying a hash function on the config file contents and comparing the
        // result with the config digest from the manifest
        let config_str = self.fetch_config(&image_name)?;
        
        let manifest_obj: serde_json::Value = serde_json::from_str(manifest_str.as_str())
            .map_err(|_| Error::ImageNotCached("Could not parse manifest JSON.".to_string()))?;

        // Extract the config digest hash
        let config_digest = manifest_obj
            .get("config").ok_or_else(||
                Error::ImageNotCached("'config' field missing from image manifest.".to_string()))?
            .get("digest").ok_or_else(||
                Error::ImageNotCached("'digest' field missing from image manifest.".to_string()))?
            .as_str().ok_or_else(||
                Error::ImageNotCached("Failed to get config digest from image manifest.".to_string()))?
            .strip_prefix("sha256:").ok_or_else(||
                Error::ImageNotCached("Failed to get config digest from image manifest.".to_string()))?
            .to_string();
        // Compare the two digests
        if config_digest != format!("{:x}", sha2::Sha256::digest(config_str.as_bytes())) {
            return Err(Error::ImageNotCached("Config content digest and manifest digest do not match".to_string()));
        }

        Ok(true)
    }

    /// Returns the manifest JSON string from the cache.
    pub fn fetch_manifest<S: AsRef<str>>(&self, image_name: S) -> Result<String> {
        let target_path = self.get_image_folder_path(&image_name)?;

        // Read the JSON string from the manifest file
        let manifest_path = target_path.clone().join(CACHE_MANIFEST_FILE_NAME);
        let mut manifest_json = String::new();
        File::open(&manifest_path).map_err(Error::FetchManifest)?
                .read_to_string(&mut manifest_json).map_err(Error::FetchManifest)?;
        if manifest_json.is_empty() {
            return Err(Error::ManifestEmpty);
        }

        Ok(manifest_json)
    }

    /// Returns the config JSON string from the cache.
    pub fn fetch_config<S: AsRef<str>>(&self, image_name: S) -> Result<String> {
        let target_path = self.get_image_folder_path(&image_name)?;

        let mut config_json = String::new();
        File::open(target_path.join(CACHE_CONFIG_FILE_NAME)).map_err(Error::FetchConfig)?
            .read_to_string(&mut config_json).map_err(Error::FetchConfig)?;
        if config_json.is_empty() {
            return Err(Error::ConfigEmpty);
        }

        Ok(config_json)
    }

    /// Fetches the layer files from the cache.
    fn fetch_layers<S: AsRef<str>>(&self, image_name: S) -> Result<Vec<Vec<u8>>> {
        let target_path = self.get_image_folder_path(&image_name)?;

        // Get all layer files from the folder
        let mut layers_tmp = fs::read_dir(
                // Create the path to the layers folder
                target_path.join(CACHE_LAYERS_FOLDER_NAME)
            )
            .map_err(Error::FetchLayers)?
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
                    res.map_err(Error::FetchLayers)
                } else {
                    Ok(res.unwrap())
                }
            });

        // If not all layer files could be opened, return error
        if layers_tmp.any(|res| res.is_err()) {
            return Err(Error::FetchLayers(io::Error::new(io::ErrorKind::Other, "Not all layer
                files could be opened")));
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
                .map_err(Error::FetchLayers)?;

            // If no bytes were read, throw an error
            if bytes_read == 0 {
                return Err(Error::FetchLayers(io::Error::new(io::ErrorKind::Other, "No bytes read from layer.")));
            }

            // Add this layer to the array of layers
            layers.push(data);
        }

        Ok(layers)
    }

    /// Validates that the image layers are cached correctly by checking them with the layer descriptors
    /// from the image manifest.
    fn validate_layers<P: AsRef<Path>>(&self, layers_path: P, manifest_str: &String) -> Result<()> {
        let manifest_obj: serde_json::Value = serde_json::from_str(manifest_str.as_str())
            .map_err(|_| Error::ImageNotCached("Manifest serialization failed".to_string()))?;
        
        // Try to get the layer list from the manifest JSON
        let layers_vec: Vec<serde_json::Value> = manifest_obj
            .get("layers").ok_or_else(||
                Error::ImageNotCached(format!("'layers' field missing from manifest JSON.")))?
            .as_array()
            .ok_or_else(|| Error::ImageNotCached("Manifest deserialize error.".to_string()))?
            .to_vec();

        // Get the cached layers as a HashMap mapping a layer digest to the corresponding layer file
        let mut cached_layers: HashMap<String, File> = HashMap::new();

        fs::read_dir(layers_path)
            .map_err(|err|
                Error::ImageNotCached(format!("Failed to get image layers: {:?}", err)))?
            .into_iter()
            .filter(|entry| entry.is_ok())
            // Get only the files
            .filter(|entry| entry.as_ref().unwrap().path().is_file())
            .map(|file| (File::open(file.as_ref().unwrap().path()), file.unwrap().file_name()))
            .filter(|(file, _)| file.is_ok())
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
                    Error::ImageNotCached("Image layer size not found in manifest.".to_string()))?
                .as_u64()
                .ok_or_else(|| Error::ImageNotCached("Layer info extract error.".to_string()))?;

            // Read the layer digest from the manifest
            let layer_digest: String = layer_obj
                .get("digest").ok_or_else(||
                    Error::ImageNotCached("Image layer digest not found in manifest".to_string()))?
                .as_str().ok_or_else(||
                    Error::ImageNotCached("Layer info extract error".to_string()))?
                .strip_prefix("sha256:").ok_or_else(||
                    Error::ImageNotCached("Layer info extract error".to_string()))?
                .to_string();

            // Get the cached layer file matching the digest
            // If not present, then a layer file is missing, so return Error
            let layer_file = cached_layers.get(&layer_digest)
                    .ok_or_else(|| Error::ImageNotCached("Layer missing from cache.".to_string()))?;

            // Get the size in bytes of the layer file
            let size = layer_file.deref()
                .metadata().map_err(|_|
                    Error::ImageNotCached("Failed to extract metadata from layer file.".to_string()))?
                .len();

            // Check that the sizes match
            if layer_size != size {
                return Err(Error::ImageNotCached("Layer not valid".to_string()));
            }
        }

        Ok(())
    }

    // /// Extracts the specified expressions ("ENV", "CMD" or "ENTRYPOINT") from the config JSON
    // /// string given as parameter.
    // pub fn get_expressions<S: AsRef<str>, T: AsRef<str>>(config_json: S, expr: T) -> Result<Vec<String>> {
    //     // Deserialize the config string into a JSON Value
    //     let config: serde_json::Value = serde_json::from_str(config_json.as_ref()).map_err(|err| {
    //         Error::CacheFetchError(format!(
    //             "Failed to extract ENV expressions from image: {:?}",
    //             err
    //         ))
    //     })?;

    //     let field = match expr.as_ref().to_string().as_str() {
    //         "ENV" => "Env",
    //         "CMD" => "Cmd",
    //         "ENTRYPOINT" => "Entrypoint",
    //         &_ => ""
    //     };

    //     // Try to parse the config JSON for the specified field
    //     let array = match config
    //         .get("container_config")
    //         .ok_or_else(|| {
    //             Error::CacheFetchError(
    //                 "'container_config' field is missing in the config JSON.".to_string(),
    //             )
    //         })?
    //         .get(field)
    //         .ok_or_else(|| {
    //             Error::CacheFetchError(format!("{} field is missing in the configuration JSON.", field))
    //         })?
    //         // Try to extract the array of expressions
    //         .as_array()
    //     {
    //         None => Ok(Vec::new()),
    //         Some(array) => {
    //             let strings: Vec<String> = array
    //                 .iter()
    //                 .map(|json_value| {
    //                     let mut string = json_value.to_string();
    //                     // Remove the quotes from the beginning and the end of the expression
    //                     string.pop();
    //                     string.remove(0);
    //                     string
    //                 })
    //                 .collect();
    //             Ok(strings)
    //         }
    //     };

    //     array
    // }

    // /// Extracts the value matching the field given as argument from the config JSON
    // /// string given as parameter.
    // pub fn get_from_config_json<S: AsRef<str>, T: AsRef<str>>(config_json: S, field: T) -> Result<String> {
    //     // Deserialize the config string into a JSON Value
    //     let config: serde_json::Value = serde_json::from_str(config_json.as_ref()).map_err(|err| {
    //         Error::CacheFetchError(format!(
    //             "Deserialization error: {:?}",
    //             err
    //         ))
    //     })?;

    //     // Parse the architecture
    //     let arch = config
    //         .get(field.as_ref().to_string().as_str())
    //         .ok_or_else(|| Error::CacheFetchError(
    //             format!("{} field is missing from config JSON.", field.as_ref().to_string())
    //         ))?
    //         .to_string();

    //     Ok(arch)
    // }

    /// Returns the default root folder path of the cache.
    /// 
    /// If the "test" argument is true, ./cache/ is used as cache root.
    /// 
    /// The default cache path is {XDG_DATA_HOME}/.nitro_cli/container_cache, and if the env
    /// variable is not set, {HOME}/.local/share/.nitro_cli/container_cache is used.
    pub fn get_default_cache_root_path(test: bool) -> Result<PathBuf> {
        // For testing, the cache will be saved to the local directory
        if test {
            let mut local_path = std::env::current_dir().map_err(|err|
                Error::CacheRootPath(format!("{:?}", err)))?;
            local_path.push("cache");
            return Ok(local_path);
        }

        // Try to use XDG_DATA_HOME as default root
        let root = match std::env::var_os(CACHE_ROOT_FOLDER) {
            Some(val) => val.into_string()
                .map_err(|err| Error::CacheRootPath(format!("{:?}", err)))?,
            // If XDG_DATA_HOME is not set, use {HOME}/.local/share as specified in
            // https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
            None => {
                let home_folder = std::env::var_os("HOME")
                    .ok_or_else(|| Error::CacheRootPath(
                        "HOME environment variable is not set.".to_string()))?;
                format!("{}/.local/share/", home_folder.into_string()
                    .map_err(|err| Error::CacheRootPath(format!("{:?}", err)))?)
            }
        };

        let mut path = PathBuf::from(root);
        // Add the additional path to the root
        path.push(".nitro_cli/container_cache");

        Ok(path)
    }

    /// Returns the image hash (if available in the CacheManager's hashmap) taking the image
    /// name as parameter.
    fn get_image_hash_from_name<S: AsRef<str>>(&self, name: S) -> Option<String> {
        let image_ref = Image::image_reference(&name);
        if image_ref.is_err() {
            return None;
        }

        self.cached_images.get(&image_ref.unwrap().whole()).map(|val| val.to_string())
    }

    /// Returns the path to an image folder in the cache.
    /// 
    /// This is achieved by looking up in the hashmap by the image reference in order
    /// to find the image hash.
    fn get_image_folder_path<S: AsRef<str>>(&self, image_name: S) -> Result<PathBuf> {
        let image_hash = self.get_image_hash_from_name(&image_name).ok_or_else(||
            Error::HashMissing
        )?;

        Ok(self.root_path.clone().join(image_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    pub enum TestError {
        ImageCacheError(crate::cache::Error),
        ImagePullError(crate::pull::Error),
    }

    impl From<crate::cache::Error> for TestError {
        fn from(err: crate::cache::Error) -> Self {
            TestError::ImageCacheError(err)
        }
    }

    impl From<crate::pull::Error> for TestError {
        fn from(err: crate::pull::Error) -> Self {
            TestError::ImagePullError(err)
        }
    }

    impl std::fmt::Display for TestError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                TestError::ImagePullError(err) =>
                    write!(f, "Test Error: Could not pull image data from the remote registry: {:?}", err),
                TestError::ImageCacheError(err) =>
                    write!(f, "Test Error: Cache related error: {:?}", err)
            }
        }
    }

    impl std::error::Error for TestError {}

    pub type TestResult<T> = std::result::Result<T, TestError>;

    /// Name of the image to be pulled and cached for testing.
    const TEST_IMAGE_NAME: &str = "hello-world";

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

    /// The config.json file of the hello-world image used for testing
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
    const TEST_CMD_EXPRESSIONS: [&'static str; 1] =
        ["/hello"];

    /// Removes the whitespaces from the string given as parameter.
    /// 
    /// Used to format the manifest and config JSON strings for testing.
    fn format_test_string(str: &str) -> String {
        let mut string = str.to_string();
        string = string.replace("\n", "");
        string = string.replace(" ", "");

        string
    }

    /// This test should be run first to setup the test cache.
    #[tokio::test]
    async fn test_setup_cache() -> TestResult<()> {
        // Name of the image to be used for testing
        let image_name = TEST_IMAGE_NAME.to_string();

        let image_data = crate::pull::pull_image_data(&image_name).await?;

        let mut cache_manager = CacheManager::new(
            CacheManager::get_default_cache_root_path(true).unwrap()
        )?;

        cache_manager.store_image(&image_name, &image_data)?;

        Ok(())
    }

    #[test]
    fn test_image_is_cached() -> TestResult<()> {
        // Name of the image to be used for testing
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(
            CacheManager::get_default_cache_root_path(true).unwrap()
        )?;

        let res = cache_manager.is_cached(&image_name)?;

        assert_eq!(res, true);

        Ok(())
    }

    #[test]
    fn test_fetch_manifest() -> TestResult<()> {
        // Name of the image to be used for testing
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(
            CacheManager::get_default_cache_root_path(true).unwrap()
        )?;

        let cached_manifest = cache_manager.fetch_manifest(&image_name)?;

        assert_eq!(cached_manifest, format_test_string(TEST_MANIFEST));

        Ok(())
    }

    #[test]
    fn test_fetch_config() -> TestResult<()> {
        // Name of the image to be used for testing
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(
            CacheManager::get_default_cache_root_path(true).unwrap()
        )?;

        let cached_config = cache_manager.fetch_config(&image_name)?;

        assert_eq!(cached_config, format_test_string(TEST_CONFIG));

        Ok(())
    }

    // #[test]
    // fn test_get_expressions() -> TestResult<()> {
    //     let config = format_test_string(TEST_CONFIG);

    //     let cmd = CacheManager::get_expressions(&config, "CMD")?;
    //     let env = CacheManager::get_expressions(&config, "ENV")?;
    //     let entrypoint = CacheManager::get_expressions(&config, "ENTRYPOINT")?;

    //     let test_env_expressions: Vec<String> = TEST_ENV_EXPRESSIONS
    //         .iter()
    //         .map(|str| str.to_string())
    //         .collect();

    //     let test_cmd_expressions: Vec<String> = TEST_CMD_EXPRESSIONS
    //         .iter()
    //         .map(|str| str.to_string())
    //         .collect();

    //     let test_entrypoint_expressions: Vec<String> = Vec::new();

    //     assert_eq!(env, test_env_expressions);
    //     assert_eq!(cmd, test_cmd_expressions);
    //     assert_eq!(entrypoint, test_entrypoint_expressions);

    //     Ok(())
    // }



    // use oci_spec::image::ImageConfiguration;
    // #[test]
    // fn test_serde() -> TestResult<()> {
    //     // Name of the image to be used for testing
    //     let image_name = TEST_IMAGE_NAME.to_string();

    //     let cache_manager = CacheManager::new(
    //         CacheManager::get_default_cache_root_path(true).unwrap()
    //     )?;

    //     let cached_config = cache_manager.fetch_config(&image_name)?;

    //     let image_config: ImageConfiguration = deserialize_from_reader(cached_config.as_bytes())?;

    //     image_config.to_file("tmp-config.json").unwrap();

    //     Ok(())
    // }


    // macro_rules! aw {
    //     ($e:expr) => {
    //         tokio_test::block_on($e)
    //     };
    // }
}
