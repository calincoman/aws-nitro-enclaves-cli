// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use crate::cache::CacheManager;
use crate::image::ImageDetails;

#[derive(Debug)]
pub enum Error {
    ImagePull(crate::pull::Error),
    Cache(crate::cache::Error),
    ImageConvert(crate::image::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ImagePull(err) => write!(
                f,
                "Could not pull image data from the remote registry: {:?}",
                err
            ),
            Error::Cache(err) => write!(f, "Cache error: {:?}", err),
            Error::ImageConvert(err) => write!(f, "Image convert error: {:?}", err),
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

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub struct ImageManager {
    // Have the cache as an option in order to not stop the cli if there is a cache creation
    // error (the image will be pulled in this case, instead of being fetched from cache)
    cache: Option<CacheManager>,
}

impl ImageManager {
    pub fn new<P: AsRef<Path>>(root_path: Option<P>) -> Self {
        // If the cache root path could not be determined, then the cache can not be initialized
        if root_path.is_none() {
            return Self { cache: None };
        }

        let cache_manager_res = CacheManager::new(&root_path.unwrap());
        // If the cache could not be created, log the error
        if cache_manager_res.is_err() {
            eprintln!("{:?}", cache_manager_res.as_ref().err().unwrap());
        }

        Self {
            cache: match cache_manager_res {
                Ok(val) => Some(val),
                Err(_) => None,
            },
        }
    }

    /// Returns a struct containing image metadata.
    ///
    /// If the image is cached correctly, the function tries to fetch the image from the cache.
    ///
    /// If the image is not cached or a cache was not created (the 'cache' field is None),
    /// it pulls the image, caches it (if the 'cache' field is not None) and returns its metadata.
    ///
    /// If the pull succedeed but the caching failed, it returns the pulled image metadata.
    pub async fn get_image_details<S: AsRef<str>>(
        &mut self,
        image_name: S,
    ) -> Result<ImageDetails> {
        let local_cache = (&mut self.cache).as_mut();

        // If the image is cached, fetch and return its metadata
        if local_cache.is_some() && local_cache.as_ref().unwrap().is_cached(&image_name).is_ok() {
            let image_details = local_cache.unwrap().fetch_image_details(&image_name)?;
            println!("Image found in local cache.");
            return Ok(image_details);
        } else {
            // The image is not cached, so try to pull and then cache it
            println!("Image is not cached, attempting to pull it.");
            let image_data = crate::pull::pull_image_data(&image_name).await?;
            println!("Image pulled successfully.");

            // Store the image to cache
            if local_cache.is_some() {
                let res = local_cache
                    .unwrap()
                    .store_image_data(&image_name, &image_data);
                if res.is_err() {
                    println!(
                        "Failed to store image to cache: {}",
                        format!("{:?}", res.err().unwrap())
                    );
                }
            }

            let image_details =
                ImageDetails::from(&image_name, &image_data).map_err(Error::ImageConvert)?;

            // Even if the caching failed, return the image
            return Ok(image_details);
        }
    }
}
