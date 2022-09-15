// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::convert::TryFrom;
use std::fmt::write;
use std::{
    path::Path
};

use sha2::Digest;
use tokio::runtime::Runtime;
use serde_json::json;

use crate::cache::CacheManager;
use crate::image::{self, Image, ModelImageDetails, ModelConfig};

#[derive(Debug)]
pub enum Error {
    ImagePull(crate::pull::Error),
    Cache(crate::cache::Error),
    ImageConvert(crate::image::Error),
    Other(String)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ImagePull(err) =>
                write!(f, "Could not pull image data from the remote registry: {:?}", err),
            Error::Cache(err) =>
                write!(f, "Cache error: {:?}", err),
            Error::ImageConvert(err) =>
                write!(f, "Image convert error: {:?}", err),
            Error::Other(msg) =>
                write!(f, "{}", msg)
        }
    }
}

impl From<crate::pull::Error> for Error {
    fn from(err: crate::pull::Error) -> Self {
        Error::ImagePull(err)
    }
}

impl From<crate::cache::Error> for Error {
    fn from(err: crate::cache::Error) -> Self {
        Error::Cache(err)
    }
}

impl From<crate::image::Error> for Error {
    fn from(err: crate::image::Error) -> Self {
        Error::ImageConvert(err)
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
    pub async fn get_image<S: AsRef<str>>(&mut self, image_name: S, with_layers: bool) -> Result<Image> {
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

                let image = Image::try_from(image_data)?;

                // Even if the caching failed, return the image
                return Ok(image);
            }
        } else {
            println!("Cache not set in current ImageManager object, pulling and returning image.");
            let image_data = crate::pull::pull_image_data(&image_name).await?;
            println!("Image pulled successfully.");

            let image = Image::try_from(image_data)?;

            return Ok(image);
        }
    }

    pub async fn get_image_details<S: AsRef<str>>(&mut self, image_name: S)
    -> Result<serde_json::Value> {
        let mut config_json = String::new();
        if self.cache.is_some() {
            if self.cache().unwrap().is_cached(&image_name).is_ok() {
                config_json = self.cache().unwrap().fetch_config(&image_name)?;
                println!("Image found in local cache - fetching config.");
            } else {
                // The image is not cached, so try to pull and then cache it
                println!("Image is not cached, attempting to pull it.");
                let image_data = crate::pull::pull_image_data(&image_name).await?;
                println!("Image pulled successfully.");

                // Store the image to cache
                let res = self.cache_mut().unwrap().store_image(&image_name, &image_data);
                if res.is_err() {
                    println!("Failed to store image to cache: {}", format!("{:?}", res.err().unwrap()));
                }

                config_json = crate::extract::extract_config_json(&image_data).map_err(|err|
                    Error::Other(format!("{:?}", err)))?;
            }
        } else {
            println!("Cache not set in current ImageManager object, pulling and returning image.");
            let image_data = crate::pull::pull_image_data(&image_name).await?;
            println!("Image pulled successfully.");

            config_json = crate::extract::extract_config_json(&image_data).map_err(|err|
                Error::Other(format!("{:?}", err)))?;
        }

        // Deserialize the config JSON String
        let mut config: ModelImageDetails = serde_json::from_str(&config_json.as_str())
            .map_err(|_| Error::Other("Could not serialize the config JSON.".to_string()))?; 

        // The 'Id' field represents the hash of the image and is later used, so set this field in particular
        // The image hash is the SHA256 digest of the image config, as specified in the OCI image-spec
        // https://github.com/opencontainers/image-spec/blob/main/config.md
    
        let image_hash = format!("{:x}", sha2::Sha256::digest(config_json.as_bytes()));
        config.id = Some(image_hash);

        Ok(json!(config))
    }
}
