// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::fs;
use std::path::Path;
use std::env;
use std::io::{Write, BufWriter};
use std::path::PathBuf;

use oci_distribution::{Reference};
use oci_distribution::client::ImageData;

use crate::cache_manager::{CacheManager, CacheFetch, CacheError};
use crate::constants;

use crate::utils::{ExtractLogic, Image, CachePath};

// Imports for testing
use oci_distribution::secrets::RegistryAuth;
use oci_distribution::{manifest, Client};
use oci_distribution::client::ClientProtocol;
use crate::utils;
use std::io::Read;

pub struct CacheStore {
    cache_path: PathBuf,
}

impl CacheStore {

    pub fn new<P: AsRef<Path>>(path: P) -> CacheStore {
        Self {
            cache_path: path.as_ref().to_path_buf(),
        }
    }

    /// Caches an image in the folder given in the constructor (stores the image data, creates the image folder
    /// and records the image in the CacheManager's hashmap)
    pub fn cache_image(&self, image: &Image, cache_manager: &mut CacheManager) -> Result<(), CacheError> {
        // Check if the image is already cached
        if cache_manager.is_cached(&image.reference().whole()) {
            eprintln!("Image with URI {} is already cached", image.reference().whole());
            return Ok(())
        }

        // Determine the path of the folder where the image data will be stored
        let image_folder_path = CachePath::get_custom_image_folder_path(image, &self.cache_path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        // Create the folder where the image data will be stored
        CachePath::create_image_folder(image, &self.cache_path).map_err(|err|
            CacheError::StoreError(format!("{:?}", err)))?;

        // Cache all the image data (layers, config, manifest etc.)
        self.cache_image_data(image, &image_folder_path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        // Record the new cached image
        cache_manager.record_image(image)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        Ok(())
    }

    /// Stores the image and its associated meta and config data in the cache folder given as parameter
    pub fn cache_image_data(&self, image: &Image, path: &PathBuf) -> Result<(), CacheError> {
        let image_data = image.data();

        self.cache_image_file(image_data, path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;
    
        self.cache_config(image_data, path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        self.cache_manifest(image_data, path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        self.cache_layers(image_data, path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        self.cache_expressions(image_data, &constants::ENV_EXPRESSION.to_string(), path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        self.cache_expressions(image_data, &constants::CMD_EXPRESSION.to_string(), path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        Ok(())
    }

    /// Store the actual image file to the cache folder
    pub fn cache_image_file(&self, image_data: &ImageData, path: &PathBuf) -> Result<(), CacheError> {

        // Try to extract the image file data bytes from the remotely pulled ImageData struct
        let image_bytes = ExtractLogic::extract_image(image_data)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        // Build the cache path of the image file
        let mut file_path = path.clone();
        file_path.push(constants::IMAGE_FILE_NAME);
        
        // Create the cached image file (the directories in the path should already be created)
        let mut output_file = File::create(&file_path)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        // Write the image file bytes to the cache image file
        output_file.write_all(&image_bytes)
            .map_err(|err| CacheError::StoreError(format!(
                "Image file could not be written to cache: {:?}", err)))
    }

    /// Store the config.json file of an image to the cache folder
    pub fn cache_config(&self, image_data: &ImageData, path: &PathBuf) -> Result<(), CacheError> {
        // Try to extract the configuration JSON string from the remotely pulled ImageData struct
        let config_json = ExtractLogic::extract_config_json(image_data)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        // Build the cache path of the config file
        let mut file_path = path.clone();
        file_path.push(constants::CACHE_CONFIG_FILE_NAME);

        // Create the cached config JSON file (the directories in the path should already be created)
        let mut output_file = File::create(&file_path)
            .map_err(|err| CacheError::StoreError(format!(
                "Failed to create the configuration cache file: {:?}", err)))?;

        // Write the JSON string to the cached config file
        output_file.write_all(config_json.as_bytes())
            .map_err(|err| CacheError::StoreError(format!(
                "Configuration JSON could not be written to cache: {:?}", err)))
    }

    /// Store the manifest.json file of an image to the cache folder
    pub fn cache_manifest(&self, image_data: &ImageData, path: &PathBuf) -> Result<(), CacheError> {
        // Try to extract the manifest JSON from the remotely pulled ImageData struct
        let manifest_json = ExtractLogic::extract_manifest_json(image_data)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;
        
        // Build the cache path of the manifest file
        let mut file_path = path.clone();
        file_path.push(constants::CACHE_MANIFEST_FILE_NAME);

        // Create the cached manifest JSON file (the directories in the path should already be created)
        let mut output_file = File::create(&file_path)
            .map_err(|err| CacheError::StoreError(format!(
                "Failed to create the manifest cache file: {:?}", err)))?;

        // Write the manifest data to the manifest.json file
        output_file.write_all(manifest_json.as_bytes())
            .map_err(|err| CacheError::StoreError(format!(
                "Manifest file could not be written to cache: {:?}", err)))?;

        Ok(())
    }

    /// Store the image layers (as tar files) of an image to the cache folder, each layer
    /// in a different file
    pub fn cache_layers(&self, image_data: &ImageData, path: &PathBuf) -> Result<(), CacheError> {
        // Try to extract the image layers from the remotely pulled ImageData struct
        let image_layers = ExtractLogic::extract_layers(image_data)
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        let mut output_path = path.clone();
        // Add the 'layers' directory to the path
        output_path.push("layers");

        // Create the 'layers' directory
        fs::create_dir_all(&output_path).map_err(|err| CacheError::StoreError(format!(
            "Failed to create the folder for storing the image layers: {:?}", err)))?;
        
        // Iterate through the layers and for each layer, store it in a tar file in the cache
        for (index, layer) in image_layers.iter().enumerate() {
            // Build the path of the layer tar file
            let mut file_path = output_path.clone();
            file_path.push(vec!["layer".to_string(), index.to_string()].concat());

            // Create the cache file containing the layer
            let mut output_file = File::create(&file_path)
                .map_err(|err| CacheError::StoreError(format!(
                    "Failed to create an image layer cache file: {:?}", err)))?;

            // Write the layer bytes to the cache file
            output_file.write_all(&layer.data)
                .map_err(|err| CacheError::StoreError(format!(
                    "Failed to write layer to cache file for layer {} with digest {}: {:?}",
                    index, layer.sha256_digest(), err)))?;
        }

        Ok(())
    }

    /// Store the 'ENV' or 'CMD' expressions in the env.sh or cmd.sh files to the cache folder
    /// 
    /// The required expression is given in the 'expression_name' parameter
    pub fn cache_expressions(&self, image_data: &ImageData, expression_name: &String, path: &PathBuf) -> Result<(), CacheError> {
        let expressions_res = match expression_name.as_str() {
            "ENV" => ExtractLogic::extract_env_expressions(image_data),
            "CMD" => ExtractLogic::extract_cmd_expressions(image_data),
            _ => {
                return Err(CacheError::ArgumentError(
                    "Function argument 'expression_name' should be 'CMD' or 'ENV'".to_string()));
            }
        };

        let path = path.clone();

        let expressions = expressions_res
            .map_err(|err| CacheError::StoreError(format!("{:?}", err)))?;

        // Build the path of the cache file containing the expressions
        let output_path = vec![path.clone().into_os_string().into_string().unwrap(),
            "/".to_string(), match expression_name.as_str() {
                "ENV" => constants::ENV_CACHE_FILE_NAME.to_string(),
                "CMD" => constants::CMD_CACHE_FILE_NAME.to_string(),
                // This case was already handled above
                _ => {
                    return Err(CacheError::ArgumentError("".to_string()));
                },
            }].concat();

        // Create the file
        let output_file = File::create(output_path)
            .map_err(|err| CacheError::StoreError(format!(
                "Failed to create the output flle: {:?}", err)))?;                              

        // Use a BufWriter to write to the cache file, one expression on every new line
        let mut writer = BufWriter::new(&output_file);

        // Iterate through the expressions and write each one of them on a new line
        expressions.iter()
            .try_for_each(|expr| writeln!(&mut writer, "{}", expr)
                .map_err(|err| CacheError::StoreError(format!(
                    "Failed to write {} expression to the output cache file: {}", expression_name, err))))?;
        Ok(())
    }
}

// TESTING

#[derive(Debug, PartialEq)]
pub enum TestError {
    ImageDataPull(String),
    ImageCache(String),
}

/// This mod pulls an image from the docker registry and tests that the caching / cache fetch works
/// THE SETUP_CACHE FUNCTION SHOULD BE RUN FIRST TO PULL AND STORE THE IMAGE
#[cfg(test)]
mod tests {
    use crate::{cache_manager::{CacheFetch, FetchError, self}, constants::CACHE_INDEX_FILE_NAME};

    use super::*;
    use std::{io::Read, ops::Deref};

    // This should be run first to pull the image and store it so the other tests do not fail
    #[tokio::test]
    async fn setup_cache() -> Result<(), TestError> {
        // Image to pull
        let image_name = "hello-world".to_string();
        let image_ref = Image::build_image_reference(&image_name);
        let image_data = match get_image_data(&image_name).await {
            Ok(aux) => aux,
            Err(err) => {
                return Err(err);
            }
        };

        let cache_folder_path = CachePath::get_default_cache_root_folder().map_err(|err|
            TestError::ImageCache(format!("{:?}", err)))?;

        CachePath::create_cache_folder_path(&cache_folder_path).map_err(|err|
            TestError::ImageCache(format!("{:?}", err)))?;

        // println!("{}", cache_folder_path.display());

        let mut cache_manager = CacheManager::new(&cache_folder_path)
            .create_index_file_if_absent()
                .map_err(|err| TestError::ImageCache(format!("{:?}", err)))?
            .populate_hashmap()
                .map_err(|err| TestError::ImageCache(format!("{:?}", err)))?;


        let image = Image::new(image_ref.clone(), image_data.clone());
        let cache_store = CacheStore::new(&cache_folder_path);
        match cache_store.cache_image(&image, &mut cache_manager) {
            Ok(()) => Ok(()),
            Err(err) => Err(TestError::ImageCache(format!("{:?}", err)))
        }?;

        cache_manager.write_index_file().map_err(|err| 
            TestError::ImageCache(format!("{:?}", err)))?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_image() -> Result<(), TestError> {
        let (image_data, cache_manager, mut cache_fetch) = setup_test().await;

        let cached_image_bytes = cache_fetch.fetch_image(&cache_manager).map_err(|err|
            TestError::ImageCache(format!("Image file fetch failed: {:?}", err)))?;

        let image_bytes = ExtractLogic::extract_image(&image_data).unwrap();
        assert_eq!(cached_image_bytes, image_bytes);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_config() -> Result<(), TestError> {
        let (image_data, cache_manager, mut cache_fetch) = setup_test().await;

        let cached_config_str = cache_fetch.fetch_config(&cache_manager).map_err(|err|
            TestError::ImageCache(format!("Image config data fetch failed: {:?}", err)))?;

        let config_str = ExtractLogic::extract_config_json(&image_data).unwrap();
        assert_eq!(cached_config_str, config_str);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_manifest() -> Result<(), TestError> {
        let (image_data, cache_manager, mut cache_fetch) = setup_test().await;
        
        let cached_manifest_str = cache_fetch.fetch_manifest(&cache_manager).map_err(|err|
            TestError::ImageCache(format!("Image manifest data fetch failed: {:?}", err)))?;

        let manifest_str = ExtractLogic::extract_manifest_json(&image_data).unwrap();
        assert_eq!(cached_manifest_str, manifest_str);

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_layers() -> Result<(), TestError> {
        let (image_data, cache_manager, mut cache_fetch) = setup_test().await;

        let cached_layers = cache_fetch.fetch_layers(&cache_manager).map_err(|err|
            TestError::ImageCache(format!("Image layers fetch failed: {:?}", err)))?;

        let layers = ExtractLogic::extract_layers(&image_data).unwrap();
        for (cached_layer, layer) in cached_layers.iter().zip(layers.iter()) {
            assert_eq!(cached_layer.deref(), layer.data);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_env_expressions() -> Result<(), TestError> {
        let (image_data, cache_manager, mut cache_fetch) = setup_test().await;

        let cached_env_expr = cache_fetch.fetch_env_expressions(&cache_manager).map_err(|err|
            TestError::ImageCache(format!("'ENV' expressions fetch failed: {:?}", err)))?;

        let env_expr = ExtractLogic::extract_env_expressions(&image_data).unwrap();
        for (cached_expr, expr) in cached_env_expr.iter().zip(env_expr.iter()) {
            assert_eq!(cached_expr, expr);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_cached_cmd_expressions() -> Result<(), TestError> {
        let (image_data, cache_manager, mut cache_fetch) = setup_test().await;

        let cached_cmd_expr = cache_fetch.fetch_cmd_expressions(&cache_manager).map_err(|err|
            TestError::ImageCache(format!("'CMD' expressions fetch failed: {:?}", err)))?;

        let cmd_expr = ExtractLogic::extract_cmd_expressions(&image_data).unwrap();
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

pub async fn setup_test() -> (ImageData, CacheManager, CacheFetch) {
    let image_name = "hello-world".to_string();
    let image_ref = Image::build_image_reference(&image_name);
    let cache_folder_path = CachePath::get_default_cache_root_folder().unwrap();

    CachePath::create_cache_folder_path(&cache_folder_path).unwrap();

    let image_data = get_image_data(&image_name).await.unwrap();
    let cache_manager = CacheManager::new(CachePath::get_default_cache_root_folder().unwrap())
        .create_index_file_if_absent().unwrap()
        .populate_hashmap().unwrap();
    let cache_fetch = CacheFetch::new(image_ref.whole(), cache_folder_path.clone());

   (image_data, cache_manager, cache_fetch)
}

// Pulls an ImageData struct from the Docker remote registry
// For testing purposes
pub async fn get_image_data(image_name: &String) -> Result<ImageData, TestError> {
    let accepted_media_types =
        vec![manifest::WASM_LAYER_MEDIA_TYPE,
             manifest::IMAGE_DOCKER_LAYER_GZIP_MEDIA_TYPE];

    let mut client = utils::build_client(ClientProtocol::Https);
    let image_ref = Image::build_image_reference(image_name);
    let auth = docker_auth();
    
    let image_data = client
        .pull(&image_ref, &auth, accepted_media_types)
        .await;
    
    match image_data {
        Ok(img_data) => Ok(img_data),
        Err(err) => Err(TestError::ImageDataPull(format!(
            "Could not pull the image data from the remote registry: {:?}", err)))
    }
}

// FOR TESTING
// Reads docker registry credentials from the credentials.txt file stored in the enclave_build crate
//
// In that file, the docker username should be on the first line and the password on the second
pub fn read_credentials() -> RegistryAuth {

    let mut file = File::open("credentials.txt")
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

pub fn docker_auth() -> RegistryAuth {
    println!("First you must provide docker credentials.");
    let auth = read_credentials();

    match auth {
        RegistryAuth::Anonymous => panic!("Authentication requires both a username and a password"),
        RegistryAuth::Basic(_, _) => ()
    }

    println!("\nCredentials found:");
    match &auth {
        RegistryAuth::Basic(username, password)
            => println!("Username: {}\nPassword: {}", username, password),
        _ => println!("Error")
    }
    println!("");

    auth
}