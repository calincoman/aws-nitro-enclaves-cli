// Copyright 2019-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#![allow(clippy::too_many_arguments)]

use std::fs::File;
use std::path::Path;
use std::process::Command;

mod cache;
mod docker;
mod image;
mod image_manager;
mod pull;
mod yaml_generator;

use aws_nitro_enclaves_image_format::defs::{EifBuildInfo, EifIdentityInfo, EIF_HDR_ARCH_ARM64};
use aws_nitro_enclaves_image_format::utils::identity::parse_custom_metadata;
use aws_nitro_enclaves_image_format::utils::{EifBuilder, SignEnclaveInfo};
use serde_json::json;
use sha2::Digest;
use std::collections::BTreeMap;
use yaml_generator::YamlGenerator;

pub const DEFAULT_TAG: &str = "1.0";

pub struct Docker2Eif<'a> {
    docker_image: String,
    // This field can be any struct that implements the 'ImageManager' trait
    image_manager: Box<dyn image_manager::ImageManager>,
    init_path: String,
    nsm_path: String,
    kernel_img_path: String,
    cmdline: String,
    linuxkit_path: String,
    artifacts_prefix: String,
    output: &'a mut File,
    sign_info: Option<SignEnclaveInfo>,
    img_name: Option<String>,
    img_version: Option<String>,
    metadata_path: Option<String>,
    build_info: EifBuildInfo,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Docker2EifError {
    DockerError,
    DockerfilePathError,
    ImagePullError,
    InitPathError,
    NsmPathError,
    KernelPathError,
    LinuxkitExecError,
    LinuxkitPathError,
    MetadataPathError,
    MetadataError(String),
    ArtifactsPrefixError,
    RamfsError,
    RemoveFileError,
    SignImageError(String),
    SignArgsError,
    UnsupportedArchError,
}

impl<'a> Docker2Eif<'a> {
    /// If the 'docker_dir' argument is Some (i.e. --docker-dir flag is used), the docker daemon will
    /// always be used to build the image locally and store it in the docker cache.
    /// 
    /// If the 'oci_image' argument is Some (i.e. the --image-uri flag is used) and the '--docker-dir'
    /// flag is not used, pull and cache the image without using the docker daemon.
    pub fn new(
        docker_image: String,
        docker_dir: Option<String>,
        oci_image: Option<String>,
        init_path: String,
        nsm_path: String,
        kernel_img_path: String,
        cmdline: String,
        linuxkit_path: String,
        output: &'a mut File,
        artifacts_prefix: String,
        certificate_path: &Option<String>,
        key_path: &Option<String>,
        img_name: Option<String>,
        img_version: Option<String>,
        metadata_path: Option<String>,
        build_info: EifBuildInfo,
    ) -> Result<Self, Docker2EifError> {
        
        let use_docker_daemon = match (&docker_dir, &oci_image) {
            // If a Dockerfile dir is specified, then use the daemon to build the image locally even
            // if the --image-uri flag is used.
            (Some(_), Some(_)) => true,
            // If a Dockerfile dir is not specified and the --image-uri flag is used, do not use docker.
            (None, Some(_)) => false,
            // For all other cases in which the --image-uri flag is not specified, use the daemon.
            _ => true
        };

        if !Path::new(&init_path).is_file() {
            return Err(Docker2EifError::InitPathError);
        } else if !Path::new(&nsm_path).is_file() {
            return Err(Docker2EifError::NsmPathError);
        } else if !Path::new(&kernel_img_path).is_file() {
            return Err(Docker2EifError::KernelPathError);
        } else if !Path::new(&linuxkit_path).is_file() {
            return Err(Docker2EifError::LinuxkitPathError);
        } else if !Path::new(&artifacts_prefix).is_dir() {
            return Err(Docker2EifError::ArtifactsPrefixError);
        }

        if let Some(ref path) = metadata_path {
            if !Path::new(path).is_file() {
                return Err(Docker2EifError::MetadataPathError);
            }
        }

        let sign_info = match (certificate_path, key_path) {
            (None, None) => None,
            (Some(cert_path), Some(key_path)) => Some(
                SignEnclaveInfo::new(cert_path, key_path)
                    .map_err(|err| Docker2EifError::SignImageError(format!("{:?}", err)))?,
            ),
            _ => return Err(Docker2EifError::SignArgsError),
        };

        Ok(Docker2Eif {
            docker_image: docker_image.clone(),
            image_manager: match use_docker_daemon {
                true => Box::new(crate::docker::DockerImageManager::new(&docker_image)),
                false => Box::new(crate::image_manager::OciImageManager::new(&oci_image.unwrap()))
            },
            init_path,
            nsm_path,
            kernel_img_path,
            cmdline,
            linuxkit_path,
            output,
            artifacts_prefix,
            sign_info,
            img_name,
            img_version,
            metadata_path,
            build_info,
        })
    }

    pub fn pull_image(&mut self) -> Result<(), Docker2EifError> {
        self.image_manager.pull_image().map_err(|e| {
            eprintln!("Docker error: {:?}", e);
            Docker2EifError::DockerError
        })?;

        Ok(())
    }

    pub fn build_docker_image(&self, dockerfile_dir: String) -> Result<(), Docker2EifError> {
        if !Path::new(&dockerfile_dir).is_dir() {
            return Err(Docker2EifError::DockerfilePathError);
        }
        self.image_manager.build_image(dockerfile_dir).map_err(|e| {
            eprintln!("Docker error: {:?}", e);
            Docker2EifError::DockerError
        })?;

        Ok(())
    }

    fn generate_identity_info(&mut self) -> Result<EifIdentityInfo, Docker2EifError> {
        let docker_info = self.image_manager.inspect_image().map_err(|e| {
            Docker2EifError::MetadataError(format!("Docker inspect error: {:?}", e))
        })?;

        let uri_split: Vec<&str> = self.docker_image.split(':').collect();
        if uri_split.is_empty() {
            return Err(Docker2EifError::MetadataError(
                "Wrong image name specified".to_string(),
            ));
        }

        // Image hash is used by default in case image version is not provided.
        // It's taken from JSON generated by `docker inspect` and a bit fragile.
        // May be later we should change it to fetching this data
        // from a specific struct and not JSON
        let img_hash = docker_info
            .get("Id")
            .and_then(|val| val.as_str())
            .and_then(|str| str.strip_prefix("sha256:"))
            .ok_or_else(|| {
                Docker2EifError::MetadataError(
                    "Image info must contain string Id field".to_string(),
                )
            })?;

        let img_name = self
            .img_name
            .clone()
            .unwrap_or_else(|| uri_split[0].to_string());
        let img_version = self
            .img_version
            .clone()
            .unwrap_or_else(|| img_hash.to_string());

        let mut custom_info = json!(null);
        if let Some(ref path) = self.metadata_path {
            custom_info = parse_custom_metadata(path).map_err(Docker2EifError::MetadataError)?
        }

        Ok(EifIdentityInfo {
            img_name,
            img_version,
            build_info: self.build_info.clone(),
            docker_info,
            custom_info,
        })
    }

    pub fn create(&mut self) -> Result<BTreeMap<String, String>, Docker2EifError> {
        let (cmd_file, env_file) = self.image_manager.load().map_err(|e| {
            eprintln!("Docker error: {:?}", e);
            Docker2EifError::DockerError
        })?;

        let yaml_generator = YamlGenerator::new(
            self.docker_image.clone(),
            self.init_path.clone(),
            self.nsm_path.clone(),
            cmd_file.path().to_str().unwrap().to_string(),
            env_file.path().to_str().unwrap().to_string(),
        );

        let ramfs_config_file = yaml_generator.get_bootstrap_ramfs().map_err(|e| {
            eprintln!("Ramfs error: {:?}", e);
            Docker2EifError::RamfsError
        })?;
        let ramfs_with_rootfs_config_file = yaml_generator.get_customer_ramfs().map_err(|e| {
            eprintln!("Ramfs error: {:?}", e);
            Docker2EifError::RamfsError
        })?;

        let bootstrap_ramfs = format!("{}/bootstrap-initrd.img", self.artifacts_prefix);
        let customer_ramfs = format!("{}/customer-initrd.img", self.artifacts_prefix);

        let output = Command::new(&self.linuxkit_path)
            .args(&[
                "build",
                "-name",
                &bootstrap_ramfs,
                "-format",
                "kernel+initrd",
                ramfs_config_file.path().to_str().unwrap(),
            ])
            .output()
            .map_err(|_| Docker2EifError::LinuxkitExecError)?;
        if !output.status.success() {
            eprintln!(
                "Linuxkit reported an error while creating the bootstrap ramfs: {:?}",
                String::from_utf8_lossy(&output.stderr)
            );
            return Err(Docker2EifError::LinuxkitExecError);
        }

        // Prefix the docker image filesystem, as expected by init
        let output = Command::new(&self.linuxkit_path)
            .args(&[
                "build",
                "-docker",
                "-name",
                &customer_ramfs,
                "-format",
                "kernel+initrd",
                "-prefix",
                "rootfs/",
                ramfs_with_rootfs_config_file.path().to_str().unwrap(),
            ])
            .output()
            .map_err(|_| Docker2EifError::LinuxkitExecError)?;
        if !output.status.success() {
            eprintln!(
                "Linuxkit reported an error while creating the customer ramfs: {:?}",
                String::from_utf8_lossy(&output.stderr)
            );
            return Err(Docker2EifError::LinuxkitExecError);
        }

        let arch = self.image_manager.architecture().map_err(|e| {
            eprintln!("Docker error: {:?}", e);
            Docker2EifError::DockerError
        })?;

        let flags = match arch.as_str() {
            docker::DOCKER_ARCH_ARM64 => EIF_HDR_ARCH_ARM64,
            docker::DOCKER_ARCH_AMD64 => 0,
            _ => return Err(Docker2EifError::UnsupportedArchError),
        };

        let eif_info = self.generate_identity_info()?;

        let mut build = EifBuilder::new(
            Path::new(&self.kernel_img_path),
            self.cmdline.clone(),
            self.sign_info.clone(),
            sha2::Sha384::new(),
            flags,
            eif_info,
        );

        // Linuxkit adds -initrd.img sufix to the file names.
        let bootstrap_ramfs = format!("{}-initrd.img", bootstrap_ramfs);
        let customer_ramfs = format!("{}-initrd.img", customer_ramfs);

        build.add_ramdisk(Path::new(&bootstrap_ramfs));
        build.add_ramdisk(Path::new(&customer_ramfs));

        Ok(build.write_to(self.output))
    }
}
