// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use serde::{de::DeserializeOwned, Serialize};

use crate::image::Image;

/// Name of the cache index file which stores the (image URI <-> image hash) mappings
pub const CACHE_INDEX_FILE_NAME: &str = "index.json";

#[derive(Debug)]
pub enum Error {
    ReadWriteError(std::io::Error),
    SerdeError(serde_json::Error),
    CacheBuildError(String)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::ReadWriteError(err) => write!(f, "Read/Write error: {:?}", err),
            Error::SerdeError(err) => {
                write!(f, "Serialization/Deserialization error: {:?}", err)
            },
            Error::CacheBuildError(msg) => write!(f, "Failed to build cache: {}", msg)
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
struct CacheManager {
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
