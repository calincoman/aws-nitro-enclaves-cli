## Nitro Enclaves Command Line Interface (Nitro CLI)

This repository contains a collection of tools and commands used for managing the lifecycle of enclaves. The Nitro CLI needs to be installed on the parent instance, and it can be used to start, manage, and terminate enclaves.  

### Prerequisites
  1. A working docker setup, follow https://docs.docker.com/install/overview/ for details of how to install docker on your host, including how to run it as non-root.
  2. Install gcc, make, git, llvm-dev, libclang-dev, clang.

### Driver information
  The Nitro Enclaves kernel driver in the 'drivers/virt/nitro_enclaves' directory is similar to the one merged into the Linux kernel mainline ( https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=726eb70e0d34dc4bc4dada71f52bba8ed638431e ) and that is available starting with the v5.10 Linux kernel.

  The Nitro Enclaves kernel driver is currently available in the following distros kernels:

  - Amazon Linux 2 v4.14 kernel starting with kernel-4.14.198-152.320.amzn2.x86_64
  - Amazon Linux 2 v5.4 kernel starting with kernel-5.4.68-34.125.amzn2.x86_64

  Out-of-tree driver build can be done using the Makefile in the 'drivers/virt/nitro_enclaves' directory.

### How to install (Git):
  1. Clone the repository.
  2. Set NITRO_CLI_INSTALL_DIR to the desired location, by default everything will be installed in build/install
  3. Run 'make nitro-cli && make vsock-proxy && make install'.
  4. Source the script ${NITRO_CLI_INSTALL_DIR}/etc/profile.d/nitro-cli-env.sh.
  5. [Optional] You could add ${NITRO_CLI_INSTALL_DIR}/etc/profile.d/nitro-cli-env.shenv.sh in you local shell configuration.
  6. You are now ready to go.

### How to install (Repository):
  1. Install the main Nitro CLI package from the AL2 repository: `sudo amazon-linux-extras install aws-nitro-enclaves-cli`.
  2. [Optional] In case you want to build EIF images, install additional Nitro Enclaves resources: `sudo yum install -y aws-nitro-enclaves-cli-devel`.
  3. Reserve resources (memory and CPUs) for future enclaves, by editing '/etc/nitro_enclaves/allocator.conf' (or use the default configuration - 512MB and 2 CPUs) and then starting the resource reservation service: `sudo systemctl start ne-allocator.service`.
  4. [Optional] If you want your resources configuration to persist across reboots, enable the service: `sudo systemctl enable config-enclave-resources.service`.
  5. You are now ready to go.

### How to use nitro-cli
  The user guide for the Nitro Enclaves CLI can be found at https://docs.aws.amazon.com/enclaves/latest/user/nitro-enclave-cli.html.

## License
  This library is licensed under the Apache 2.0 License.

## Source-code components
  The components of the CLI are organized as follows (all paths are relative to the CLI's root directory):

  - 'blobs': Binary blobs providing pre-compiled components needed for the building of enclave images:
      - 'blobs/bzImage': Kernel image
      - 'blobs/bzImage.config': Kernel config
      - 'blobs/cmdline': Kernel boot command line
      - 'blobs/init': Init process executable
      - 'blobs/linuxkit': LinuxKit-based user-space environment
      - 'blobs/nsm.ko': The driver which enables the Nitro Secure Module (NSM) component inside the enclave

  - 'build': An automatically-generated directory which stores the build output for various components (the CLI, the command executer etc.)

  - 'bootstrap': Various useful scripts for CLI environment configuration, namely:
      - 'allocatior.yaml': Configuration file for enclave memory and CPUs reservation
      - 'env.sh': A script which inserts the pre-built Nitro Enclaves kernel module, adds the CLI binary directory to $PATH and sets the blobs directory
      - 'nitro-cli-config': A script which can build, configure and install the Nitro Enclaves kernel module, as well as configure the memory and CPUs available for enclave launches (depending on the operation, root privileges may be required)
      - 'nitro-enclaves-allocator': Configuration script for enclave memory and CPUs reservation
      - 'nitro-enclaves-allocator.service': Configuration service for enclave memory and CPUs reservation

  - 'docs': Useful documentation

  - 'drivers': The source code of the kernel modules used by the CLI in order to control enclave behavior, containing:
      - 'drivers/virt/nitro_enclaves': The Nitro Enclaves driver used by the Nitro CLI

  - 'eif_defs': The definition of the enclave image format (EIF) file

  - 'eif_loader': The source code for the EIF loader, a module which ensures that an enclave has booted successfully

  - 'eif_utils': Utilities for the EIF files, focused mostly on building EIFs

  - 'enclave_build': A tool which builds EIF files starting from a Docker image and pre-existing binary blobs (such as those from 'blobs')

  - 'include': The header files exposed by the Nitro Enclaves kernel module used by the Nitro CLI

  - 'rust-cose': A Rust-based COSE implementation, needed by the EIF utilities module ('eif_utils')

  - 'samples': A collection of CLI-related sample applications. One sample is the command executer - an application that enables a parent instance to issue commands to an enclave (such as transferring a file, executing an application on the enclave etc.)

  - 'src': The Nitro CLI implementation, divided into 3 components:
      - The implementation of the background enclave process: 'src/enclave_proc'
      - The implementation of the CLI, which takes user commands and communicates with enclave processes: 'src/*.rs'
      - A common module used by both the CLI and the enclave process: 'src/common'

  - 'tests': Various unit and integration tests for the CLI

  - 'tools': Various useful configuration files used for CLI and EIF builds

  - 'vsock_proxy': The implementation of the Vsock - TCP proxy application, which is used to allow an enclave to communicate with an external service through the parent instance

  - 'ci_entrypoint.sh': The script which launches the CLI continuous integration tests

  - 'init.c': The implementation of the default init process used by an enclave's user-space

  - 'run_tests.sh': The continuous integration test suite for the CLI
