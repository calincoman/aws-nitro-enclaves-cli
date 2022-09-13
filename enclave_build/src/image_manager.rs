// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::write;
use std::{
    path::Path
};

use sha2::Digest;
use tokio::runtime::Runtime;
use serde_json::json;

use crate::cache::CacheManager;
use crate::image::{self, ImageCacheFetch, ShipliftImageDetails};

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    ImagePullError(crate::pull::Error),
    CacheError(crate::cache::Error),
    ImageRefError,
    Other(String)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ImagePullError(err) =>
                write!(f, "Could not pull image data from the remote registry: {:?}", err),
            Error::CacheError(err) =>
                write!(f, "{:?}", err),
            Error::ImageRefError =>
                write!(f, "Failed to find image reference."),
            Error::Other(msg) =>
                write!(f, "{}", msg)
        }
    }
}

impl From<crate::pull::Error> for Error {
    fn from(err: crate::pull::Error) -> Self {
        Error::ImagePullError(err)
    }
}

impl From<crate::cache::Error> for Error {
    fn from(err: crate::cache::Error) -> Self {
        Error::CacheError(err)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub struct ImageManager {
    cache: Option<CacheManager>
}

impl ImageManager {
    pub fn new(cache: Option<CacheManager>) -> Self {
        Self { cache }
    }

    pub fn cache(&self) -> Option<&CacheManager> {
        self.cache.as_ref()
    }

    pub fn cache_mut(&mut self) -> Option<&mut CacheManager> {
        (&mut self.cache).as_mut()
    }

    /// Creates an image cache at the path specified as parameter.
    /// 
    /// The parameter should be a path to a folder. If any folders from
    /// the path are not already created, this function call creates them.
    /// 
    /// If the cache is already existing, the cached images names are loaded
    /// into the CacheManager
    pub fn add_cache<P: AsRef<Path>>(&mut self, root_path: P) -> Result<()> {
        let cache_manager = CacheManager::new(&root_path)?;
        // Update the struct field
        self.cache = Some(cache_manager);

        Ok(())
    }

    /// Returns a struct containing image data. If the 'with_layers' argument is true, the layers
    /// are also returned.
    /// 
    /// If the image is cached correctly, the function tries to fetch the image from the cache.
    /// 
    /// If the image is not cached or a cache was not created (the 'cache' field is None),
    /// it pulls the image, caches it (if the 'cache' field is not None) and returns it.
    /// 
    /// If the pull succedeed but the caching failed, just returns the pulled image.
    pub async fn get_image<S: AsRef<str>>(&mut self, image_name: S, with_layers: bool) -> Result<ImageCacheFetch> {
        // If a cache was created / added
        if self.cache.is_some() {
            let local_cache = self.cache_mut().unwrap();
            // If the image is cached, fetch and return it
            if local_cache.is_cached(&image_name).is_ok() {
                let image = local_cache.fetch_image(&image_name, with_layers)?;
                println!("Image found in local cache.");
                return Ok(image);
            } else {
                // The image is not cached, so try to pull and then cache it
                println!("Image is not cached, attempting to pull it.");
                let image_data = crate::pull::pull_image_data(&image_name).await?;
                println!("Image pulled successfully.");

                // Store the image to cache
                let res = local_cache.store_image(&image_name, &image_data);
                if res.is_err() {
                    println!("Failed to store image to cache: {}", format!("{:?}", res.err().unwrap()));
                }

                let image = ImageCacheFetch::from(image_data)
                    .map_err(|err| Error::Other(format!("{:?}", err)))?;

                // Even if the caching failed, return the image
                return Ok(image);
            }
        } else {
            println!("Cache not found in current ImageManager object, pulling and returning image.");
            let image_data = crate::pull::pull_image_data(&image_name).await?;
            println!("Image pulled successfully.");

            let image = ImageCacheFetch::from(image_data)
                    .map_err(|err| Error::Other(format!("{:?}", err)))?;

            return Ok(image);
        }
    }

    /// Attempts to fetch the ENV, CMD and ENTRYPOINT expressions (in this order) from the cached image.
    /// 
    /// If the image is not cached, it tries to pull the image, cache it and then return the expressions.
    /// If caching fails but the pulling was successful, it extracts and returns the expressions from the pulled
    /// image.
    pub async fn get_expressions<S: AsRef<str>>(&mut self, image_name: S)
        -> Result<(Vec<String>, Vec<String>, Vec<String>)> {

            let image = self.get_image(&image_name, false).await?;
        
            let config_json = image.config().to_string();

            let env = CacheManager::get_expressions(&config_json, "ENV")?;
            let cmd = CacheManager::get_expressions(&config_json, "CMD")?;
            let entrypoint = CacheManager::get_expressions(&config_json, "ENTRYPOINT")?;

            Ok((env, cmd, entrypoint))
    }

    /// Attempts to fetch the architecture on which the binaries of the image are built to run on
    /// from the cached image.
    /// 
    /// If the image is not cached, it tries to pull the image, cache it and then return the architecture.
    /// If caching fails but the pulling was successful, it extracts and returns the architecture from the pulled
    /// image.
    pub async fn get_architecture<S: AsRef<str>>(&mut self, image_name: S) -> Result<String> {
        let image = self.get_image(&image_name, false).await?;

        let config_json = image.config().to_string();

        let architecture = CacheManager::get_from_config_json(&config_json, "architecture")?;

        Ok(architecture)
    }

    pub async fn get_image_details<S: AsRef<str>>(&mut self, image_name: S)
    -> Result<serde_json::Value> {
        let image = self.get_image(&image_name, false).await?;
        let config_json = image.config().to_string();

        // Extract the config JSON into the shiplift Config struct
        let tmp: serde_json::Value = serde_json::from_str(&config_json.as_str())
            .map_err(|err| Error::Other(format!("{:?}", err)))?;
        
        let config_val = tmp.get("container_config")
            .ok_or_else(|| Error::Other(format!("'container_config' field missing from config JSON.")))?;

        let config: image::ShipliftConfig = serde_json::from_value(config_val.clone())
            .map_err(|err| Error::Other(format!("Deserialization failed: {:?}", err)))?;

        let arch = CacheManager::get_from_config_json(&config_json, "architecture")?;
        let created = CacheManager::get_from_config_json(&config_json, "created")?;
        let docker_version = CacheManager::get_from_config_json(&config_json, "docker_version")?;
        let os = CacheManager::get_from_config_json(&config_json, "os")?;    

        // The 'id' is the digest of the config string.
        let id = format!("sha256:{:x}", sha2::Sha256::digest(config_json.as_bytes()));

        let image_details = 
            ShipliftImageDetails {
                architecture: arch,
                // not found in pulled config
                author: "".to_string(),
                // not found in pulled config
                comment: "".to_string(),
                config: config,
                created: created,
                docker_version: docker_version,
                id: id,
                os: os,
                // not found in pulled config
                parent: "".to_string(),
                repo_tags: None,
                // not found in pulled config
                repo_digests: None,
                // not found in pulled config
                size: 0,
                // not found in pulled config
                virtual_size: 0
            };

        Ok(json!(image_details))
       
        // Kept this comment just as a model
        // pub struct ShipliftImageDetails {
        //     pub architecture: String,
        //     pub author: String,
        //     pub comment: String,
        //     pub config: ShipliftConfig,
        //     pub created: String,
        //     pub docker_version: String,
        //     pub id: String,
        //     pub os: String,
        //     pub parent: String,
        //     pub repo_tags: Option<Vec<String>>,
        //     pub repo_digests: Option<Vec<String>>,
        //     pub size: u64,
        //     pub virtual_size: u64,
        // }
    }
}
