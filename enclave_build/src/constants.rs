// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use oci_distribution::manifest;

/// Docker inspect architecture constants
pub const DOCKER_ARCH_ARM64: &str = "arm64";
pub const DOCKER_ARCH_AMD64: &str = "amd64";

/// Root folder for the cache
pub const CACHE_ROOT_FOLDER: &str = "XDG_DATA_HOME";

/// For testing purposes, use $HOME as cache root folder
pub const HOME_ENV_VAR: &str = "HOME";

/// The name of the folder used by the cache to store the image layers
pub const CACHE_LAYERS_FOLDER_NAME: &str = "layers";

/// The name of the actual image file from the image cache folder
pub const CACHE_IMAGE_FILE_NAME: &str = "image_file";

/// The name of the iamge config file from the cache
pub const CACHE_CONFIG_FILE_NAME: &str = "config.json";

/// The name of the iamge manifest file from the cache
pub const CACHE_MANIFEST_FILE_NAME: &str = "manifest.json";

/// Name of the cache file where 'ENV' expressions are stored
pub const ENV_CACHE_FILE_NAME: &str = "env.sh";

/// Name of the cache file where 'CMD' expressions are stored
pub const CMD_CACHE_FILE_NAME: &str = "cmd.sh";

/// Name of the file which stores the (image URI <-> image hash) mappings
pub const CACHE_INDEX_FILE_NAME: &str = "index.json";

pub const CMD_EXPRESSION: &str = "CMD";
pub const ENV_EXPRESSION: &str = "ENV";

pub const ACCEPTED_MEDIA_TYPES: [&'static str; 2] = [
    manifest::WASM_LAYER_MEDIA_TYPE,
    manifest::IMAGE_DOCKER_LAYER_GZIP_MEDIA_TYPE
];

/// If true, enclave_build/test_cache/container_cache will be used as cache
/// 
/// If false, the default path is used
pub const TEST_MODE_ENABLED: bool = true;
