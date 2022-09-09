// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fmt::write;
use std::{
    path::Path
};

use tokio::runtime::Runtime;

use crate::cache::CacheManager;
use crate::image::ImageCacheFetch;

#[derive(Debug)]
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
    /// If the cache creation failed, an error is returned.
    pub fn add_cache<P: AsRef<Path>>(&mut self, root_path: P) -> Result<()> {
        let cache_manager = CacheManager::new(&root_path)?;
        // Update the struct field
        self.cache = Some(cache_manager);

        Ok(())
    }

    /// Returns a struct containing image data.
    /// 
    /// If the image is cached correctly, the function tries to fetch the image from the cache.
    /// 
    /// If the image is not cached or a cache was not created (the 'cache' field is None),
    /// it pulls the image, caches it (if the 'cache' field is not None) and returns it.
    /// 
    /// If the pull succedeed but the caching failed, just returns the pulled image.
    pub async fn get_image<S: AsRef<str>>(&mut self, image_name: S) -> Result<ImageCacheFetch> {
        let image_ref = crate::image::Image::image_reference(&image_name)
            .map_err(|_| Error::ImageRefError)?;

        // If a cache was created / added
        if self.cache.is_some() {
            let local_cache = self.cache_mut().unwrap();
            // If the image is cached, fetch and return it
            if local_cache.is_cached(&image_name).is_ok() {
                let image = local_cache.fetch_image(&image_name)?;
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
            println!("Cache not found in current ImageManager object, pulling and returning image");
            let image_data = crate::pull::pull_image_data(&image_name).await?;
            println!("Image pulled successfully.");

            let image = ImageCacheFetch::from(image_data)
                    .map_err(|err| Error::Other(format!("{:?}", err)))?;

            return Ok(image);
        }
    }
}
