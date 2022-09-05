// Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::path::Path;

use oci_distribution::{
    Reference,
    RegistryOperation,
    client::{
        Client,
        ClientProtocol,
        ClientConfig,
        ImageData
    },
    secrets::{RegistryAuth},
    manifest::OciImageManifest
};

use crate::cache_manager::{Image};
use crate::constants;

#[derive(Debug)]
pub enum PullError {
    ImagePullError(String),
    ManifestPullError(String),
    DockerConfigFileError(String),
    CredentialsError(String),
    InvalidCredentials,
}

impl std::fmt::Display for PullError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PullError::ImagePullError(msg) =>
                write!(f, "Could not pull image data from remote registry: {}", msg),
            PullError::ManifestPullError(msg) =>
                write!(f, "Failed to pull image manifest: {}", msg),
            PullError::DockerConfigFileError(msg) =>
                write!(f, "{}", msg),
            PullError::CredentialsError(msg) =>
                write!(f, "{}", msg),
            PullError::InvalidCredentials =>
                write!(f, "Credentials from the Docker config file are not correct.")
        }
    }
}

impl std::error::Error for PullError {}

pub type PullResult<T> = std::result::Result<T, PullError>;

/// Builds a client which uses the protocol given as parameter
/// Client required for the https://github.com/krustlet/oci-distribution library API
pub fn build_client(protocol: ClientProtocol) -> Client {
    let client_config = ClientConfig {
        protocol,
        ..Default::default()
    };
    Client::new(client_config)
}

/// Returns the docker config file by searching first at the path pointed by DOCKER_CONFIG,
/// and if this env is not set, at {HOME}/.docker/config.json
fn get_docker_config_file() -> Result<File, PullError> {
    // First check the DOCKER_CONFIG env variable for the path to the docker config file
    if let Ok(file) = std::env::var("DOCKER_CONFIG") {
        let config_file = File::open(file).map_err(|err| {
            PullError::DockerConfigFileError(format!("Could not open file pointed by env variable\
                DOCKER_CONFIG: {}", err))
        })?;
        Ok(config_file)
    } else {
        // If DOCKER_CONFIG is not set, try to get the config file from the default path
        // {HOME}/.docker/config.json
        if let Ok(home_dir) = std::env::var("HOME") {
            let default_config_path = format!("{}/.docker/config.json", home_dir);
            let config_path = Path::new(&default_config_path);
            if config_path.exists() {
                let config_file = File::open(config_path).map_err(|err| {
                    PullError::DockerConfigFileError(format!(
                        "Could not open file {:?}: {:?}", config_path.to_str(), err))
                })?;
                return Ok(config_file);
            }
        }
        Err(PullError::DockerConfigFileError("Config file not present, please set env variable \
             DOCKER_CONFIG accordingly".to_string()))
    }
}

/// Returns the Docker credentials by reading from the Docker config.json file
/// 
/// The assumed format of the file is:\
/// {\
///        "auths": {\
///            "https://index.docker.io/v1/": {\
///                    "auth": "<token_string>"\
///            }\
///        }\
/// }
pub fn parse_credentials() -> Result<RegistryAuth, PullError> {
    let config_file = get_docker_config_file()?;

    let config_json: serde_json::Value = serde_json::from_reader(&config_file)
        .map_err(|err| PullError::CredentialsError(format!("JSON was not well-formatted: {}", err)))?;

    let auths = config_json.get("auths").ok_or_else(|| {
        PullError::CredentialsError("Could not find auths key in config JSON".to_string())
    })?;

    if let serde_json::Value::Object(auths) = auths {
        for (_, registry_auths) in auths.iter() {
            let auth = registry_auths
                .get("auth")
                .ok_or_else(|| {
                    PullError::CredentialsError("Could not find auth key in config JSON".to_string())
                })?
                .to_string();

            let auth = auth.replace('"', "");
            // Decode the auth token
            let decoded = base64::decode(&auth).map_err(|err| {
                PullError::CredentialsError(format!("Invalid Base64 encoding for auth: {}", err))
            })?;
            let decoded = std::str::from_utf8(&decoded).map_err(|err| {
                PullError::CredentialsError(format!("Invalid utf8 encoding for auth: {}", err))
            })?;

            // Try to get the username and the password
            if let Some(index) = decoded.rfind(':') {
                let (username, after_user) = decoded.split_at(index);
                let (_, password) = after_user.split_at(1);
                
                return Ok(RegistryAuth::Basic(username.to_string(), password.to_string()));
            }
        }
    }

    // If the auth token is missing, return error
    Err(PullError::CredentialsError("Credentials not found".to_string()))
}

/// Determines the authentication for the pull operation
pub fn docker_auth() -> RegistryAuth {
    match parse_credentials() {
        Ok(registry_auth) => {
            println!("Credentials found.");
            registry_auth
        }
        Err(err) => {
            println!("Credential error: {:?}, performing anonymous pull", err);
            RegistryAuth::Anonymous
        }
    }
}

/// Pulls an image (all blobs - layers, manifest and config) from the Docker remote registry
pub async fn pull_image_data(image_name: &String) -> PullResult<ImageData> {
    // Build the client required for the pulling - uses HTTPS protocol
    let mut client = build_client(ClientProtocol::Https);

    // Build the image reference from the image name
    let image_ref = Image::build_image_reference(image_name)
        .map_err(|err| PullError::ImagePullError(err.to_string()))?;
    
    // Try to get the credentials from the Docker config file
    let auth = docker_auth();
    
    // Pull the ImageData struct containing the layers, manifest and configuration file
    let image_data = client
        .pull(&image_ref, &auth, constants::ACCEPTED_MEDIA_TYPES.to_vec())
        .await;

    match image_data {
        Ok(img_data) => Ok(img_data),
        Err(err) => Err(PullError::ImagePullError(err.to_string()))
    }
}

pub async fn pull_manifest(image_ref: &Reference) -> PullResult<(OciImageManifest, String)> {
    // Build the client required for the pulling - uses HTTPS protocol
    let mut client = build_client(ClientProtocol::Https);

    // Try to get the credentials from the Docker config file
    let auth = docker_auth();

    // Pull the manifest and its digest from the remote registry
    let (manifest, digest) = client.pull_image_manifest(&image_ref, &auth)
        .await
        .map_err(|err| PullError::ManifestPullError(format!("{:?}", err)))?;

    Ok((manifest, digest))
}
