// Copyright 2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::result;

use crate::cache::CacheManager;
use crate::image::ImageDetails;

use tokio::runtime::Runtime;
use tempfile::NamedTempFile;

#[derive(Debug)]
pub enum Error {
    ImagePull(crate::pull::Error),
    Cache(crate::cache::Error),
    Extract(String),
    ImageConvert(crate::image::Error),
    ImageConfigMissing,
    EntryPoint,
    Tempfile,
    Runtime,
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
            Error::Extract(msg) => write!(f, "Failed to extract expressions from image: {}", msg),
            Error::ImageConvert(err) => write!(f, "Image convert error: {:?}", err),
            Error::ImageConfigMissing => write!(f, "Image configuration could not be extracted."),
            Error::EntryPoint => write!(f, "Entrypoint missing."),
            Error::Tempfile => write!(f, "Error when handling a tempfile"),
            Error::Runtime => write!(f, "Runtime error."),
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

/// Trait which provides an interface for handling images.
pub trait ImageManager {
    /// Pulls the image from remote and stores it in the local cache.
    fn pull_image(&mut self)
        -> result::Result<(), Box<dyn std::error::Error>>;
    /// Builds an image locally (from a Dockerfile in case of Docker).
    fn build_image(&self, dockerfile_dir: String)
        -> result::Result<(), Box<dyn std::error::Error>>;
    /// Inspects the image and returns its metadata in the form of a JSON Value.
    fn inspect_image(&mut self)
        -> result::Result<serde_json::Value, Box<dyn std::error::Error>>;
    /// Returns the architecture of the image.
    fn architecture(&mut self)
        -> result::Result<String, Box<dyn std::error::Error>>;
    /// Returns two temp files containg the CMD and ENV expressions extracted from the image,
    /// in this order.
    fn load(&mut self)
        -> result::Result<(NamedTempFile, NamedTempFile), Box<dyn std::error::Error>>;
}

pub type Result<T> = std::result::Result<T, Error>;

pub struct OciImageManager {
    /// Name of the container image.
    image_name: String,
    /// Have the cache as an option in order to not stop the CLI if there is a cache creation
    /// error (the image will simply be pulled in this case, instead of being fetched from cache)
    cache: Option<CacheManager>,
}

impl ImageManager for OciImageManager {
    /// Pulls the image from remote and attempts to cache it locally.
    fn pull_image(&mut self) -> result::Result<(), Box<dyn std::error::Error>> {
        let image_name = self.image_name.clone();
        let act = async {
            // Attempt to pull and store the image in the local cache
            self.get_image_details(&image_name)
                .await?;

            Ok(())
        };

        let runtime = Runtime::new().map_err(|_| Error::Runtime)?;
        runtime.block_on(act)
    }

    fn build_image(&self, _: String)
        -> result::Result<(), Box<dyn std::error::Error>> {
        todo!();
    }

    /// Inspect the image and return its description as a JSON String.
    fn inspect_image(&mut self) -> result::Result<serde_json::Value, Box<dyn std::error::Error>> {
        let image_name = self.image_name.clone();
        let act = async {
            let image_details = self
                .get_image_details(&image_name)
                .await?;

            // Serialize to a serde_json::Value
            serde_json::to_value(&image_details).map_err(|err| err.into())
        };

        let runtime = Runtime::new().map_err(|_| Error::Runtime)?;
        runtime.block_on(act)
    }

    /// Extracts the CMD and ENV expressions from the image and returns them each in a
    /// temporary file
    fn load(&mut self) -> result::Result<(NamedTempFile, NamedTempFile), Box<dyn std::error::Error>> {
        let (cmd, env) = self.extract_image()?;

        let cmd_file = crate::docker::write_config(cmd).map_err(|_| Error::Tempfile)?;
        let env_file = crate::docker::write_config(env).map_err(|_| Error::Tempfile)?;

        Ok((cmd_file, env_file))
    }

    /// Returns architecture information of the image.
    fn architecture(&mut self) -> result::Result<String, Box<dyn std::error::Error>> {
        let image_name = self.image_name.clone();
        let act_get_image = async {
            let image = self
                .get_image_details(&image_name)
                .await?;

            Ok(format!("{}", image.config.architecture()))
        };

        let runtime = Runtime::new().map_err(|_| Error::Runtime)?;
        runtime.block_on(act_get_image)
    }
}


impl OciImageManager {
    /// When calling this constructor, it also tries to create / initialize the cache at
    /// the default path.\
    /// If this fails, the ImageManager is still created, but the 'cache'
    /// field is set to 'None'.
    pub fn new<S: AsRef<str>>(image_name: S) -> Self {
        // Add the default ":latest" tag if the image tag is missing
        let image_name = check_tag(&image_name);

        // The docker daemon is not used, so a local cache needs to be created
        // Get the default cache root path
        let root_path = CacheManager::get_default_cache_root_path();

        // If the cache root path could not be determined, then the cache can not be initialized
        if root_path.is_err() {
            return Self {
                image_name,
                cache: None,
            };
        }

        // Try to create / read the cache
        let cache_manager_res = CacheManager::new(&root_path.unwrap());
        // If the cache could not be created, log the error
        if cache_manager_res.is_err() {
            eprintln!("{:?}", cache_manager_res.as_ref().err().unwrap());
        }

        Self {
            image_name,
            cache: match cache_manager_res {
                Ok(val) => Some(val),
                Err(_) => None,
            }
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
    async fn get_image_details<S: AsRef<str>>(
        &mut self,
        image_name: S,
    ) -> Result<ImageDetails> {
        let image_name = check_tag(&image_name);

        let local_cache = (&mut self.cache).as_mut();

        // If the image is cached, fetch and return its metadata
        if local_cache.is_some() && local_cache.as_ref().unwrap().is_cached(&image_name).is_ok() {
            // Try to fetch the image from the cache
            let image_details = local_cache.unwrap()
                .fetch_image_details(&image_name)
                .map_err(|err| {
                    // Log the fetching error
                    eprintln!("{:?}", err);
                });

            // If the fetching failed, pull it from remote
            if image_details.is_err() {
                // Pull the image from remote
                let image_data = crate::pull::pull_image_data(&image_name).await?;

                // Get the image metadata from the pulled struct
                let image_details =
                    ImageDetails::from(&image_name, &image_data).map_err(Error::ImageConvert)?;

                return Ok(image_details);
            }

            Ok(image_details.unwrap())
        } else {
            // The image is not cached, so try to pull and then cache it
            let image_data = crate::pull::pull_image_data(&image_name).await?;

            // Try to store the image to cache, if the cache was initialized
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

            // Get the image metadata from the pulled struct
            let image_details =
                ImageDetails::from(&image_name, &image_data).map_err(Error::ImageConvert)?;

            // Even if the caching failed, return the image details
            Ok(image_details)
        }
    }

    /// Extracts from the image and returns the CMD and ENV expressions (in this order).
    ///
    /// If there are no CMD expressions found, it tries to locate the ENTRYPOINT command.
    fn extract_image(&mut self) -> Result<(Vec<String>, Vec<String>)> {
        let image_name = self.image_name.clone();
        // Try to get the image details
        let act_get_image = async {
            self.get_image_details(&image_name)
                .await
        };
        let check_get_image = Runtime::new()
            .map_err(|_| Error::Runtime)?
            .block_on(act_get_image);
        if check_get_image.is_err() {
            return Err(Error::Extract(
                format!("{:?}", check_get_image.err().unwrap())
            ));
        }
        let image = check_get_image.unwrap();

        // Check if the image config exists
        if image.config.config().is_none() {
            return Err(Error::ImageConfigMissing);
        }

        // Get the expressions from the image
        let cmd = image.config.config().as_ref().unwrap().cmd();
        let env = image.config.config().as_ref().unwrap().env();
        let entrypoint = image.config.config().as_ref().unwrap().entrypoint();

        // If no CMD instructions are found, try to locate an ENTRYPOINT command
        if cmd.is_none() || env.is_none() {
            if entrypoint.is_none() {
                return Err(Error::EntryPoint);
            }
            return Ok((
                entrypoint.as_ref().unwrap().to_vec(),
                env.as_ref()
                    .ok_or_else(Vec::<String>::new)
                    .unwrap()
                    .to_vec(),
            ));
        }

        Ok((
            cmd.as_ref().unwrap().to_vec(),
            env.as_ref().unwrap().to_vec(),
        ))
    }
}

/// Adds the default ":latest" tag to an image if it is untagged
fn check_tag<S: AsRef<str>>(image_name: S) -> String {
    let name = image_name.as_ref().to_string();
    match name.contains(":") {
        true => name,
        false => format!("{}:latest", name)
    }
}
