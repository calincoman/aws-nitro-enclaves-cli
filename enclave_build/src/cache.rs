// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// S&PDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

use sha2::Digest;

use crate::image::{self, ImageDetails};

use oci_distribution::client::ImageData;

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

#[derive(Debug)]
pub enum Error {
    CacheInitError(io::Error),
    ImageNotCached(String),
    ConfigEmpty,
    ManifestEmpty,
    HashMissing,
    FetchImageDetailsRef(crate::image::Error),
    FetchImageDetailsSerde(serde_json::Error),
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
            Error::CacheInitError(err) => {
                write!(f, "CacheManager could not be initialized: {:?}", err)
            }
            Error::ImageNotCached(msg) => write!(f, "Image is not correctly cached: {}", msg),
            Error::ConfigEmpty => write!(f, "The cached config file is empty."),
            Error::ManifestEmpty => write!(f, "The cached manifest file is empty."),
            Error::HashMissing => write!(f, "The image hash is missing from the cache index file."),
            Error::Serde(err) => write!(f, "Serialization/Deserialization error: {:?}", err),
            Error::FetchImageDetailsRef(err) => write!(f, "{:?}", err),
            Error::FetchImageDetailsSerde(err) => {
                write!(f, "Serialization/Deserialization error: {:?}", err)
            }
            Error::FetchManifest(err) => {
                write!(f, "Failed to fetch manifest from cache: {:?}", err)
            }
            Error::FetchConfig(err) => write!(f, "Failed to fetch config from cache: {:?}", err),
            Error::CacheStoreError(err) => write!(f, "Failed to store image in cache: {:?}", err),
            Error::CacheRootPath(msg) => write!(
                f,
                "Failed to determine the default cache root path: {}",
                msg
            ),
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
/// {IMAGE_FOLDER_PATH}/manifest.json - the manifest.json of the image\
/// {IMAGE_FOLDER_PATH}/layers - folder containing all layers, each in a separate gzip compressed tar file\
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
            return Ok(Self {
                root_path: root_path.as_ref().to_path_buf(),
                cached_images: HashMap::new(),
            });
        }

        // Try to deserialize the JSON string into a HashMap
        let cached_images: HashMap<String, String> = serde_json::from_str(contents.as_str())
            .map_err(|err| {
                Error::CacheInitError(io::Error::new(
                    std::io::ErrorKind::Other,
                    format!(
                        "Failed to read the cached images map from index.json: {:?}",
                        err
                    ),
                ))
            })?;

        Ok(Self {
            root_path: root_path.as_ref().to_path_buf(),
            cached_images,
        })
    }

    /// Stores the image data provided as argument in the cache at the folder pointed
    /// by the 'root_path' field.
    pub fn store_image_data<S: AsRef<str>>(
        &mut self,
        image_name: S,
        image_data: &ImageData,
    ) -> Result<()> {
        let image_hash = image::image_hash(image_data.config.data.as_slice()).map_err(|err| {
            Error::CacheStoreError(io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))
        })?;

        // Create the folder where the image data will be stored
        let target_path = self.root_path.clone().join(&image_hash);
        fs::create_dir_all(&target_path).map_err(Error::CacheStoreError)?;

        // Create the 'layers' folder and store the layers in it
        let layers_path = target_path.clone().join(CACHE_LAYERS_FOLDER_NAME);
        fs::create_dir_all(&layers_path).map_err(Error::CacheStoreError)?;

        for layer in image_data.layers.clone() {
            // Each layer file will be named after the layer's digest hash
            let layer_file_path =
                layers_path.join(format!("{:x}", sha2::Sha256::digest(&layer.data)));
            File::create(&layer_file_path)
                .map_err(Error::CacheStoreError)?
                .write_all(&layer.data)
                .map_err(Error::CacheStoreError)?;
        }

        // Store the manifest
        let manifest_json = match &image_data.manifest {
            Some(image_manifest) => {
                let manifest_str = serde_json::to_string(&image_manifest).map_err(|err| {
                    Error::CacheStoreError(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Failed to store manifest: {:?}", err),
                    ))
                })?;
                Ok(manifest_str)
            }
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("Manifest missing from pulled image data."),
            )),
        }
        .map_err(Error::CacheStoreError)?;

        File::create(&target_path.join(CACHE_MANIFEST_FILE_NAME))
            .map_err(Error::CacheStoreError)?
            .write_all(manifest_json.as_bytes())
            .map_err(Error::CacheStoreError)?;

        // Store the config
        let config_json = String::from_utf8(image_data.config.data.clone()).map_err(|err| {
            Error::CacheStoreError(io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to parse config: {:?}", err),
            ))
        })?;

        File::create(&target_path.join(CACHE_CONFIG_FILE_NAME))
            .map_err(Error::CacheStoreError)?
            .write_all(config_json.as_bytes())
            .map_err(Error::CacheStoreError)?;

        // If all image data was successfully stored, add the image to the index.json file and
        // the hashmap
        let image_ref = image::image_reference(image_name).map_err(|err| {
            Error::CacheStoreError(io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))
        })?;
        self.cached_images.insert(image_ref.whole(), image_hash);

        // Open the index.json file
        let index_file = File::options()
            .write(true)
            .open(self.root_path.clone().join(CACHE_INDEX_FILE_NAME))
            .map_err(Error::CacheStoreError)?;

        // Write the hashmap (the image URI <-> image hash mappings) to the index.json file
        serde_json::to_writer(index_file, &self.cached_images).map_err(|err| {
            Error::CacheStoreError(io::Error::new(io::ErrorKind::Other, format!("{:?}", err)))
        })?;

        Ok(())
    }

    /// Determines if an image is stored correctly in the cache represented by the current CacheManager object.
    pub fn is_cached<S: AsRef<str>>(&self, image_name: S) -> Result<bool> {
        // If the image is not in the index.json file, then it is definitely not cached
        let image_hash = self.get_image_hash_from_name(&image_name);
        if image_hash.is_none() {
            return Err(Error::ImageNotCached(
                "Image hash missing from index.json file.".to_string(),
            ));
        }
        // The image is theoretically cached, but check the manifest, config and layers to validate
        // that the image data is stored correctly

        let image_folder_path = self.root_path.clone().join(&image_hash.unwrap());

        // First validate the manifest
        // Since the struct pulled by the oci_distribution API does not contain the manifest digest,
        // and another HTTP request should be made to get the digest, just check that the manifest file
        // exists and is not empty
        let manifest_str = self
            .fetch_manifest(&image_name)
            .map_err(|err| Error::ImageNotCached(format!("{:?}", err)))?;

        // The manifest is checked, so now validate the layers
        self.validate_layers(
            &image_folder_path.clone().join(CACHE_LAYERS_FOLDER_NAME),
            &manifest_str,
        )?;

        // Finally, check that the config is correctly cached
        // This is done by applying a hash function on the config file contents and comparing the
        // result with the config digest from the manifest
        let config_str = self.fetch_config(&image_name)?;

        let manifest_obj: serde_json::Value = serde_json::from_str(manifest_str.as_str())
            .map_err(|_| Error::ImageNotCached("Could not parse manifest JSON.".to_string()))?;

        // Extract the config digest hash
        let config_digest = manifest_obj
            .get("config")
            .ok_or_else(|| {
                Error::ImageNotCached("'config' field missing from image manifest.".to_string())
            })?
            .get("digest")
            .ok_or_else(|| {
                Error::ImageNotCached("'digest' field missing from image manifest.".to_string())
            })?
            .as_str()
            .ok_or_else(|| {
                Error::ImageNotCached(
                    "Failed to get config digest from image manifest.".to_string(),
                )
            })?
            .strip_prefix("sha256:")
            .ok_or_else(|| {
                Error::ImageNotCached(
                    "Failed to get config digest from image manifest.".to_string(),
                )
            })?
            .to_string();
        // Compare the two digests
        if config_digest != format!("{:x}", sha2::Sha256::digest(config_str.as_bytes())) {
            return Err(Error::ImageNotCached(
                "Config content digest and manifest digest do not match".to_string(),
            ));
        }

        Ok(true)
    }

    /// Fetches the image metadata from cache as an ImageDetails struct.
    ///
    /// If the data is not correctly cached or a file is missing, it returns an error.
    ///
    /// If the image is not cached, it does not attempt to pull the image from remote.
    pub fn fetch_image_details<S: AsRef<str>>(&self, image_name: S) -> Result<ImageDetails> {
        // Check if the image is cached
        self.is_cached(&image_name)?;

        // Get the hash of the image from cache
        let image_hash = self
            .get_image_hash_from_name(&image_name)
            .ok_or_else(|| Error::HashMissing)?;

        // Fetch the config JSON string from cache
        let config_json = self.fetch_config(&image_name)?;

        Ok(ImageDetails::new(
            image::image_reference(&image_name)
                .map_err(Error::FetchImageDetailsRef)?
                .whole(),
            image_hash,
            image::deserialize_from_reader(config_json.as_bytes())
                .map_err(Error::FetchImageDetailsSerde)?,
        ))
    }

    /// Returns the manifest JSON string from the cache.
    pub fn fetch_manifest<S: AsRef<str>>(&self, image_name: S) -> Result<String> {
        let target_path = self.get_image_folder_path(&image_name)?;

        // Read the JSON string from the manifest file
        let manifest_path = target_path.clone().join(CACHE_MANIFEST_FILE_NAME);
        let mut manifest_json = String::new();
        File::open(&manifest_path)
            .map_err(Error::FetchManifest)?
            .read_to_string(&mut manifest_json)
            .map_err(Error::FetchManifest)?;
        if manifest_json.is_empty() {
            return Err(Error::ManifestEmpty);
        }

        Ok(manifest_json)
    }

    /// Returns the config JSON string from the cache.
    pub fn fetch_config<S: AsRef<str>>(&self, image_name: S) -> Result<String> {
        let target_path = self.get_image_folder_path(&image_name)?;

        let mut config_json = String::new();
        File::open(target_path.join(CACHE_CONFIG_FILE_NAME))
            .map_err(Error::FetchConfig)?
            .read_to_string(&mut config_json)
            .map_err(Error::FetchConfig)?;
        if config_json.is_empty() {
            return Err(Error::ConfigEmpty);
        }

        Ok(config_json)
    }

    /// Validates that the image layers are cached correctly by checking them with the layer descriptors
    /// from the image manifest.
    fn validate_layers<P: AsRef<Path>>(&self, layers_path: P, manifest_str: &String) -> Result<()> {
        let manifest_obj: serde_json::Value = serde_json::from_str(manifest_str.as_str())
            .map_err(|_| Error::ImageNotCached("Manifest serialization failed".to_string()))?;

        // Try to get the layer list from the manifest JSON
        let layers_vec: Vec<serde_json::Value> = manifest_obj
            .get("layers")
            .ok_or_else(|| {
                Error::ImageNotCached(format!("'layers' field missing from manifest JSON."))
            })?
            .as_array()
            .ok_or_else(|| Error::ImageNotCached("Manifest deserialize error.".to_string()))?
            .to_vec();

        // Get the cached layers as a HashMap mapping a layer digest to the corresponding layer file
        let mut cached_layers: HashMap<String, File> = HashMap::new();

        fs::read_dir(layers_path)
            .map_err(|err| Error::ImageNotCached(format!("Failed to get image layers: {:?}", err)))?
            .into_iter()
            .filter(|entry| entry.is_ok())
            // Get only the files
            .filter(|entry| entry.as_ref().unwrap().path().is_file())
            .map(|file| {
                (
                    File::open(file.as_ref().unwrap().path()),
                    file.unwrap().file_name(),
                )
            })
            .filter(|(file, _)| file.is_ok())
            .map(|(file, name)| (file.unwrap(), name.into_string().unwrap()))
            // Map a layer digest to the layer file
            .for_each(|(file, name)| {
                cached_layers.insert(name, file);
            });

        // Iterate through each layer found in the image manifest and validate that it is stored in
        // the cache by checking the digest
        for layer_obj in layers_vec {
            // Read the layer digest from the manifest
            let layer_digest: String = layer_obj
                .get("digest")
                .ok_or_else(|| {
                    Error::ImageNotCached("Image layer digest not found in manifest".to_string())
                })?
                .as_str()
                .ok_or_else(|| Error::ImageNotCached("Layer info extract error".to_string()))?
                .strip_prefix("sha256:")
                .ok_or_else(|| Error::ImageNotCached("Layer info extract error".to_string()))?
                .to_string();

            // Get the cached layer file matching the digest
            // If not present, then a layer file is missing, so return Error
            let mut layer_file = cached_layers
                .get(&layer_digest)
                .ok_or_else(|| Error::ImageNotCached("Layer missing from cache.".to_string()))?;
            let mut layer_bytes = Vec::new();
            layer_file
                .read_to_end(&mut layer_bytes)
                .map_err(|_| Error::ImageNotCached("Failed to read layer".to_string()))?;

            let calc_digest = format!("{:x}", sha2::Sha256::digest(layer_bytes.as_slice()));

            // Check that the digests match
            if calc_digest != layer_digest {
                return Err(Error::ImageNotCached("Layer not valid".to_string()));
            }
        }

        Ok(())
    }

    /// Returns the default root folder path of the cache.
    ///
    /// The default cache path is {XDG_DATA_HOME}/.nitro_cli/container_cache, and if the env
    /// variable is not set, {HOME}/.local/share/.nitro_cli/container_cache is used.
    pub fn get_default_cache_root_path() -> Result<PathBuf> {
        // Try to use XDG_DATA_HOME as default root
        let root = match std::env::var_os(CACHE_ROOT_FOLDER) {
            Some(val) => val
                .into_string()
                .map_err(|err| Error::CacheRootPath(format!("{:?}", err)))?,
            // If XDG_DATA_HOME is not set, use {HOME}/.local/share as specified in
            // https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
            None => {
                let home_folder = std::env::var_os("HOME").ok_or_else(|| {
                    Error::CacheRootPath("HOME environment variable is not set.".to_string())
                })?;
                format!(
                    "{}/.local/share/",
                    home_folder
                        .into_string()
                        .map_err(|err| Error::CacheRootPath(format!("{:?}", err)))?
                )
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
        let image_ref = image::image_reference(&name);
        if image_ref.is_err() {
            return None;
        }

        self.cached_images
            .get(&image_ref.unwrap().whole())
            .map(|val| val.to_string())
    }

    /// Returns the path to an image folder in the cache.
    ///
    /// This is achieved by looking up in the hashmap by the image reference in order
    /// to find the image hash.
    fn get_image_folder_path<S: AsRef<str>>(&self, image_name: S) -> Result<PathBuf> {
        let image_hash = self
            .get_image_hash_from_name(&image_name)
            .ok_or_else(|| Error::HashMissing)?;

        Ok(self.root_path.clone().join(image_hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oci_distribution::{
        client::{Config, ImageLayer},
        manifest::OciImageManifest,
    };

    /// Image name for testing.
    const TEST_IMAGE_NAME: &str = "test-hello-world";

    /// Hash of the image used for testing (hash of the hello-world image)
    const TEST_IMAGE_HASH: &str =
        "edde031e8146bb4f9a18fc3462ac6ddfd126db14ccd3b90006d90a820ccc653b";

    // Use test layers (instead of original hello-world image layers) in order to not pull from remote.
    // Layers are read/written as raw data bytes, so use strings for testing.
    const TEST_IMAGE_LAYER_1: &str = "Hello World 1";
    const TEST_IMAGE_LAYER_2: &str = "Hello World 2";

    /// The manifest.json used for testing. It contains the desription of the two test layers.
    ///
    /// The digest of the config matches the config used for testing, not the original hello-world image
    /// config.
    const TEST_MANIFEST: &str = r#"
    {
        "schemaVersion":2,
        "mediaType":"application/vnd.docker.distribution.manifest.v2+json",
        "config":{
            "mediaType":"application/vnd.docker.container.image.v1+json",
            "digest":"sha256:edde031e8146bb4f9a18fc3462ac6ddfd126db14ccd3b90006d90a820ccc653b",
            "size":1469
        },
        "layers":[
            {
                "mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip",
                "digest":"sha256:1aed4d8555515c961bffea900d5e7f1c1e4abf0f6da250d8bf15843106e0533b",
                "size":13
            },
            {
                "mediaType":"application/vnd.docker.image.rootfs.diff.tar.gzip",
                "digest":"sha256:3df75539dda4c512db688b3f1d86184c0d7b99cbea1eb87dec8385a2651ac1f3",
                "size":13
            }
        ]
    }
    "#;

    /// The config.json used for testing
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

    /// ImageDetails JSON used for testing
    const TEST_IMAGE_DETAILS_CONFIG: &str = r##"
    {
        "created": "2021-09-23T23:47:57.442225064Z",
        "architecture": "amd64",
        "os": "linux",
        "config": {
            "User": "",
            "Env": [
                "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
            ],
            "Cmd": [
                "/hello"
            ],
            "WorkingDir": ""
        },
        "rootfs": {
            "type": "layers",
            "diff_ids": [
                "sha256:e07ee1baac5fae6a26f30cabfe54a36d3402f96afda318fe0a96cec4ca393359"
            ]
        },
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
        ]
    }
    "##;

    /// Builds a mock ImageData struct in order to avoid pulling from remote.
    pub fn build_test_image_data() -> ImageData {
        // Use mock image layer bytes
        let layer_1 = ImageLayer::new(TEST_IMAGE_LAYER_1.as_bytes().to_vec(), "".to_string(), None);
        let layer_2 = ImageLayer::new(TEST_IMAGE_LAYER_2.as_bytes().to_vec(), "".to_string(), None);

        // Use the config.json for testing
        let json_val: serde_json::Value = serde_json::from_str(TEST_CONFIG).unwrap();
        let config_json = serde_json::to_string(&json_val).unwrap();
        let config_obj = Config::new(config_json.as_bytes().to_vec(), "".to_string(), None);

        // Use the test manifest JSON (the digests of the layers are the digests of the two test layers)
        let json_val: serde_json::Value = serde_json::from_str(TEST_MANIFEST).unwrap();
        let manifest_json = serde_json::to_string(&json_val).unwrap();
        let manifest_obj: OciImageManifest =
            serde_json::from_str(&manifest_json).expect("Manifest JSON parsing error.");

        let image_data = ImageData {
            layers: vec![layer_1, layer_2],
            digest: None,
            config: config_obj,
            manifest: Some(manifest_obj),
        };

        image_data
    }

    fn get_test_cache_path() -> PathBuf {
        // Use {PWD}/cache as test cache.
        std::env::current_dir()
            .expect("failed to get current directory path")
            .join("cache")
    }

    /// This test should be run first to setup the test cache.
    #[test]
    fn test_setup_cache() {
        let image_name = TEST_IMAGE_NAME.to_string();

        let image_data = build_test_image_data();

        let mut cache_manager = CacheManager::new(get_test_cache_path())
            .expect("Failed to initialize the cache manager.");

        cache_manager
            .store_image_data(&image_name, &image_data)
            .expect("store error");
    }

    #[test]
    fn test_image_is_cached() {
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(get_test_cache_path())
            .expect("Failed to initialize the cache manager.");

        let res = cache_manager.is_cached(&image_name);

        assert_eq!(res.is_ok(), true);
        assert_eq!(res.unwrap(), true);
    }

    #[test]
    fn test_fetch_manifest() {
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(get_test_cache_path())
            .expect("Failed to initialize the cache manager.");

        let cached_manifest = cache_manager
            .fetch_manifest(&image_name)
            .expect("failed to fetch image manifest from cache.");

        let val1: serde_json::Value = serde_json::from_str(cached_manifest.as_str()).unwrap();
        let val2: serde_json::Value = serde_json::from_str(TEST_MANIFEST).unwrap();
        assert_eq!(val1, val2);
    }

    #[test]
    fn test_fetch_config() {
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(get_test_cache_path())
            .expect("Failed to initialize the cache manager.");

        let cached_config = cache_manager
            .fetch_config(&image_name)
            .expect("failed to fetch image config from cache.");

        let val1: serde_json::Value = serde_json::from_str(cached_config.as_str()).unwrap();
        let val2: serde_json::Value = serde_json::from_str(TEST_CONFIG).unwrap();
        assert_eq!(val1, val2);
    }

    #[test]
    fn test_fetch_image_details() {
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(get_test_cache_path())
            .expect("Failed to initialize the cache manager.");

        let image_details = cache_manager
            .fetch_image_details(&image_name)
            .expect("failed to fetch image details from test cache.");

        let test_uri = "docker.io/library/test-hello-world:latest";
        let test_image_hash = TEST_IMAGE_HASH;
        let test_image_config: oci_spec::image::ImageConfiguration =
            serde_json::from_str(TEST_IMAGE_DETAILS_CONFIG)
                .expect("failed to deserialize test JSON string");
        let test_image_details = ImageDetails::new(test_uri, test_image_hash, test_image_config);

        assert_eq!(test_image_details, image_details);
    }

    #[test]
    fn test_get_image_hash_from_name() {
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(get_test_cache_path())
            .expect("Failed to initialize the cache manager.");

        let image_hash = cache_manager
            .get_image_hash_from_name(&image_name)
            .expect("failed to get image hash.");

        assert_eq!(image_hash, TEST_IMAGE_HASH.to_string());
    }

    #[test]
    fn test_get_default_cache_root_path() {
        let default_path = match std::env::var_os("XDG_DATA_HOME") {
            Some(val) => {
                PathBuf::from(val.to_str().unwrap().to_string()).join(".nitro_cli/container_cache")
            }
            None => {
                let home = std::env::var_os("HOME").expect("HOME env not set.");
                let append = format!(
                    "{}/.local/share/.nitro_cli/container_cache",
                    home.to_str().unwrap().to_string()
                );
                PathBuf::from(home).join(append)
            }
        };
        let calc_path = CacheManager::get_default_cache_root_path()
            .expect("failed to get default cache root path.");

        assert_eq!(default_path, calc_path);
    }

    #[test]
    fn test_get_image_folder_path() {
        let image_name = TEST_IMAGE_NAME.to_string();

        let cache_manager = CacheManager::new(get_test_cache_path())
            .expect("Failed to initialize the cache manager.");

        let test_image_folder_path = get_test_cache_path().join(TEST_IMAGE_HASH);
        let image_folder_path = cache_manager
            .get_image_folder_path(&image_name)
            .expect("failed to get image folder path.");

        assert_eq!(test_image_folder_path, image_folder_path);
    }

    #[test]
    fn test_destroy_cache() {
        let path = get_test_cache_path();

        fs::remove_dir_all(&path).expect("failed to delete test cache.");
    }
}
