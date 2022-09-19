// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

use futures::stream::StreamExt;
use log::{error, info};
use shiplift::{BuildOptions, Docker};
use std::io::Write;
use tempfile::NamedTempFile;
use tokio::runtime::Runtime;

use crate::cache::CacheManager;
use crate::image_manager::ImageManager;

/// Docker inspect architecture constants
pub const DOCKER_ARCH_ARM64: &str = "arm64";
pub const DOCKER_ARCH_AMD64: &str = "amd64";

#[derive(Debug)]
pub enum DockerError {
    BuildError,
    RuntimeError,
    TempfileError,
    UnsupportedEntryPoint,
    /// Error coming from the image manager
    ImageManagerError(crate::image_manager::Error),
    /// The image configuration could not be found
    ImageConfigMissing,
    // Serde error,
    Serde(serde_json::Error),
}

impl From<crate::image_manager::Error> for DockerError {
    fn from(err: crate::image_manager::Error) -> Self {
        DockerError::ImageManagerError(err)
    }
}

/// Struct exposing the Docker functionalities to the EIF builder
pub struct DockerUtil {
    docker: Docker,
    docker_image: String,
    image_manager: ImageManager,
}

impl DockerUtil {
    /// Constructor that takes as argument a tag for the docker image to be used
    pub fn new(docker_image: String) -> Self {
        let mut docker_image = docker_image;

        if !docker_image.contains(':') {
            docker_image.push_str(":latest");
        }

        // Get the default cache root path
        let root_path = match CacheManager::get_default_cache_root_path() {
            Ok(path) => Some(path),
            Err(err) => {
                eprintln!("{:?}", err);
                None
            }
        };

        DockerUtil {
            // DOCKER_HOST environment variable is parsed inside
            // if docker daemon address needs to be substituted.
            // By default it tries to connect to 'unix:///var/run/docker.sock'
            docker: Docker::new(),
            docker_image,
            image_manager: ImageManager::new(root_path),
        }
    }

    pub fn image_manager_mut(&mut self) -> &mut ImageManager {
        &mut self.image_manager
    }

    /// Pull the image, with the tag provided in constructor, from the remote registry and
    /// store it in the local cache.
    pub fn pull_image(&mut self) -> Result<(), DockerError> {
        let act = async {
            let image_name = self.docker_image.clone();

            // Attempt to pull and store the image in the local cache
            self.image_manager_mut()
                .get_image_details(&image_name)
                .await?;

            Ok(())
        };

        let runtime = Runtime::new().map_err(|_| DockerError::RuntimeError)?;

        runtime.block_on(act)
    }

    /// Build an image locally, with the tag provided in constructor, using a
    /// directory that contains a Dockerfile
    pub fn build_image(&self, dockerfile_dir: String) -> Result<(), DockerError> {
        let act = async {
            let mut stream = self.docker.images().build(
                &BuildOptions::builder(dockerfile_dir)
                    .tag(self.docker_image.clone())
                    .build(),
            );

            loop {
                if let Some(item) = stream.next().await {
                    match item {
                        Ok(output) => {
                            let msg = &output;

                            if let Some(err_msg) = msg.get("error") {
                                error!("{:?}", err_msg.clone());
                                break Err(DockerError::BuildError);
                            } else {
                                info!("{}", msg);
                            }
                        }
                        Err(e) => {
                            error!("{:?}", e);
                            break Err(DockerError::BuildError);
                        }
                    }
                } else {
                    break Ok(());
                }
            }
        };

        let runtime = Runtime::new().map_err(|_| DockerError::RuntimeError)?;

        runtime.block_on(act)
    }

    /// Inspect the image and return its description as a JSON String.
    pub fn inspect_image(&mut self) -> Result<serde_json::Value, DockerError> {
        let image_name = self.docker_image.clone();

        let act = async {
            let image_details = self
                .image_manager_mut()
                .get_image_details(&image_name)
                .await
                .map_err(DockerError::ImageManagerError)?;

            // Serialize to a serde_json::Value
            serde_json::to_value(&image_details).map_err(DockerError::Serde)
        };

        let runtime = Runtime::new().map_err(|_| DockerError::RuntimeError)?;
        runtime.block_on(act)
    }

    /// Extract from an image and return the CMD and ENV expressions (in this order).
    ///
    /// If there are no CMD expressions found, it tries to locate the ENTRYPOINT command.
    fn extract_image(&mut self) -> Result<(Vec<String>, Vec<String>), DockerError> {
        let image_name = self.docker_image.clone();

        // Try to get the image
        let act_get_image = async {
            self.image_manager_mut()
                .get_image_details(&image_name)
                .await
        };
        let check_get_image = Runtime::new()
            .map_err(|_| DockerError::RuntimeError)?
            .block_on(act_get_image);
        if check_get_image.is_err() {
            return Err(DockerError::ImageManagerError(
                check_get_image.err().unwrap(),
            ));
        }
        let image = check_get_image.unwrap();
        // Check if the image config exists
        if image.config.config().is_none() {
            return Err(DockerError::ImageConfigMissing);
        }

        // Get the expressions from the image
        let cmd = image.config.config().as_ref().unwrap().cmd();
        let env = image.config.config().as_ref().unwrap().env();
        let entrypoint = image.config.config().as_ref().unwrap().entrypoint();

        // If no CMD instructions are found, try to locate an ENTRYPOINT command
        if cmd.is_none() || env.is_none() {
            if entrypoint.is_none() {
                return Err(DockerError::UnsupportedEntryPoint);
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

    /// The main function of this struct. This needs to be called in order to
    /// extract the necessary configuration values from the docker image with
    /// the tag provided in the constructor
    pub fn load(&mut self) -> Result<(NamedTempFile, NamedTempFile), DockerError> {
        let (cmd, env) = self.extract_image()?;

        let cmd_file = write_config(cmd)?;
        let env_file = write_config(env)?;

        Ok((cmd_file, env_file))
    }

    /// Fetch architecture information from an image
    pub fn architecture(&mut self) -> Result<String, DockerError> {
        let act_get_image = async {
            let image_name = self.docker_image.clone();

            let image = self
                .image_manager_mut()
                .get_image_details(&image_name)
                .await?;

            Ok(format!("{}", image.config.architecture()))
        };

        let runtime = Runtime::new().map_err(|_| DockerError::RuntimeError)?;

        runtime.block_on(act_get_image)
    }
}

fn write_config(config: Vec<String>) -> Result<NamedTempFile, DockerError> {
    let mut file = NamedTempFile::new().map_err(|_| DockerError::TempfileError)?;

    for line in config {
        file.write_fmt(format_args!("{}\n", line))
            .map_err(|_| DockerError::TempfileError)?;
    }

    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Read;

    /// Test extracted configuration is as expected
    #[test]
    fn test_config() {
        #[cfg(target_arch = "x86_64")]
        let mut docker = DockerUtil::new(String::from(
            "667861386598.dkr.ecr.us-east-1.amazonaws.com/enclaves-samples:vsock-sample-server-x86_64",
        ));
        #[cfg(target_arch = "aarch64")]
        let mut docker = DockerUtil::new(String::from(
            "667861386598.dkr.ecr.us-east-1.amazonaws.com/enclaves-samples:vsock-sample-server-aarch64",
        ));

        let (cmd_file, env_file) = docker.load().unwrap();
        let mut cmd_file = File::open(cmd_file.path()).unwrap();
        let mut env_file = File::open(env_file.path()).unwrap();

        let mut cmd = String::new();
        cmd_file.read_to_string(&mut cmd).unwrap();
        assert_eq!(
            cmd,
            "/bin/sh\n\
             -c\n\
             ./vsock-sample server --port 5005\n"
        );

        let mut env = String::new();
        env_file.read_to_string(&mut env).unwrap();
        assert_eq!(
            env,
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin\n"
        );
    }

    #[test]
    fn test_config_hello_world_image() {
        let mut docker = DockerUtil::new("hello-world".to_string());

        let (cmd_file, env_file) = docker.load().unwrap();
        let mut cmd_file = File::open(cmd_file.path()).unwrap();
        let mut env_file = File::open(env_file.path()).unwrap();

        let (mut cmd, mut env) = (String::new(), String::new());

        cmd_file.read_to_string(&mut cmd).unwrap();
        env_file.read_to_string(&mut env).unwrap();

        // Remove endline
        cmd.pop();
        env.pop();

        assert_eq!(cmd, "/hello",);

        assert_eq!(
            env,
            "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
        );
    }
}
