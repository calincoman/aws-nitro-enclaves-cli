// Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
#![deny(warnings)]

use log::{debug, warn};
use nix::sys::epoll::EpollFlags;
use nix::sys::socket::sockopt::PeerCredentials;
use nix::sys::socket::UnixCredentials;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::os::unix::net::UnixStream;
use std::string::ToString;
use std::sync::{Arc, Mutex};

use crate::common::{receive_from_stream, write_u64_le};
use crate::common::{
    EnclaveProcessCommandType, EnclaveProcessReply, ExitGracefully, NitroCliResult,
};

#[derive(PartialEq, Eq, Hash)]
enum CommandRequesterType {
    User(libc::uid_t),
    Group(libc::gid_t),
    Others,
}

struct CommandRequesterPolicy {
    policy: HashMap<CommandRequesterType, Vec<EnclaveProcessCommandType>>,
}

struct ConnectionData {
    epoll_flags: EpollFlags,
    input_stream: Option<UnixStream>,
}

/// An enclave process connection to a CLI instance, an enclave or itself.
#[derive(Clone)]
pub struct Connection {
    data: Arc<Mutex<ConnectionData>>,
}

impl Drop for ConnectionData {
    fn drop(&mut self) {
        if let Some(input_stream) = &self.input_stream {
            // Close the stream.
            input_stream
                .shutdown(std::net::Shutdown::Both)
                .ok_or_exit("Failed to shut down.");
        }
    }
}

impl CommandRequesterPolicy {
    fn new_with_defaults() -> Self {
        let cmds_read_write = vec![
            EnclaveProcessCommandType::Run,
            EnclaveProcessCommandType::Terminate,
            EnclaveProcessCommandType::TerminateComplete,
            EnclaveProcessCommandType::Describe,
            EnclaveProcessCommandType::GetEnclaveCID,
            EnclaveProcessCommandType::ConnectionListenerStop,
        ];
        let cmds_read_only = vec![
            EnclaveProcessCommandType::Describe,
            EnclaveProcessCommandType::GetEnclaveCID,
        ];
        let mut policy = HashMap::new();

        // The user which owns this enclave process may issue any command.
        policy.insert(
            CommandRequesterType::User(unsafe { libc::getuid() }),
            cmds_read_write.clone(),
        );

        // The root user may issue any command.
        policy.insert(
            CommandRequesterType::User(0 as libc::uid_t),
            cmds_read_write,
        );

        // All other users may only issue read-only commands.
        policy.insert(CommandRequesterType::Others, cmds_read_only);

        CommandRequesterPolicy { policy }
    }

    fn find_policy_rule(
        &self,
        cmd: EnclaveProcessCommandType,
        requester: &CommandRequesterType,
    ) -> bool {
        match self.policy.get(requester) {
            None => false,
            Some(allowed_cmds) => allowed_cmds.contains(&cmd),
        }
    }

    fn can_execute_command(&self, cmd: EnclaveProcessCommandType, creds: &UnixCredentials) -> bool {
        // Search for a policy rule on the provided user ID.
        if self.find_policy_rule(cmd, &CommandRequesterType::User(creds.uid())) {
            return true;
        }

        // Search for a policy rule on the provided group ID.
        if self.find_policy_rule(cmd, &CommandRequesterType::Group(creds.gid())) {
            return true;
        }

        // Search for a policy rule on all other users.
        if self.find_policy_rule(cmd, &CommandRequesterType::Others) {
            return true;
        }

        // If we haven't found any applicable policy rule we can't allow the command to be executed.
        false
    }
}

impl Connection {
    /// Create a new connection instance.
    pub fn new(epoll_flags: EpollFlags, input_stream: Option<UnixStream>) -> Self {
        let conn_data = ConnectionData {
            epoll_flags,
            input_stream,
        };

        Connection {
            data: Arc::new(Mutex::new(conn_data)),
        }
    }

    /// Read a command and its corresponding credentials.
    pub fn read_command(&self) -> NitroCliResult<EnclaveProcessCommandType> {
        let mut lock = self.data.lock().map_err(|e| e.to_string())?;
        if lock.input_stream.is_none() {
            return Err("Cannot read a command from this connection.".to_string());
        }

        // First, read the incoming command.
        let mut cmd =
            receive_from_stream::<EnclaveProcessCommandType>(lock.input_stream.as_mut().unwrap())
                .map_err(|e| e.to_string())?;

        // Next, read the credentials of the command requester.
        let conn_fd = lock.input_stream.as_ref().unwrap().as_raw_fd();
        let socket_creds = nix::sys::socket::getsockopt(conn_fd, PeerCredentials);

        // If the credentials cannot be read, the command will be skipped.
        let user_creds = match socket_creds {
            Ok(creds) => creds,
            Err(e) => {
                warn!("Failed to get user credentials: {}", e);
                return Ok(EnclaveProcessCommandType::NotPermitted);
            }
        };

        // Apply the default command access policy based on the user's credentials.
        let policy = CommandRequesterPolicy::new_with_defaults();
        if !policy.can_execute_command(cmd, &user_creds) {
            // Log the failed execution attempt.
            warn!(
                "The requester with credentials ({:?}) is not allowed to perform '{:?}'.",
                user_creds, cmd
            );

            // Force the command to be skipped by the main event loop.
            cmd = EnclaveProcessCommandType::NotPermitted;
        } else {
            // Log the successful execution attempt.
            debug!(
                "The requester with credentials ({:?}) is allowed to perform '{:?}'.",
                user_creds, cmd
            );
        }

        Ok(cmd)
    }

    /// Read an object from this connection.
    pub fn read<T>(&self) -> NitroCliResult<T>
    where
        T: DeserializeOwned,
    {
        let mut lock = self.data.lock().map_err(|e| e.to_string())?;
        if lock.input_stream.is_none() {
            return Err("Cannot read from this connection.".to_string());
        }

        receive_from_stream::<T>(lock.input_stream.as_mut().unwrap()).map_err(|e| e.to_string())
    }

    /// Write a u64 value on this connection.
    pub fn write_u64(&self, value: u64) -> NitroCliResult<()> {
        let mut lock = self.data.lock().map_err(|e| e.to_string())?;
        if lock.input_stream.is_none() {
            return Err("Cannot write a 64-bit value to this connection.".to_string());
        }

        write_u64_le(lock.input_stream.as_mut().unwrap(), value).map_err(|e| e.to_string())
    }

    /// Write a message to the standard output of the connection's other end.
    pub fn println(&self, msg: &str) -> NitroCliResult<()> {
        let mut msg_str = msg.to_string();

        // Append a new-line at the end of the string.
        msg_str.push('\n');

        let reply = EnclaveProcessReply::StdOutMessage(msg_str);
        self.write_reply(&reply)
    }

    /// Write a message to the standard error of the connection's other end.
    pub fn eprintln(&self, msg: &str) -> NitroCliResult<()> {
        let mut msg_str = msg.to_string();

        // Append a new-line at the end of the string.
        msg_str.push('\n');

        let reply = EnclaveProcessReply::StdErrMessage(msg_str);
        self.write_reply(&reply)
    }

    /// Write an operation's status to the connection's other end.
    pub fn write_status(&self, status: i32) -> NitroCliResult<()> {
        let reply = EnclaveProcessReply::Status(status);
        self.write_reply(&reply)
    }

    // Get the enclave event flags.
    pub fn get_enclave_event_flags(&self) -> Option<EpollFlags> {
        let lock = self
            .data
            .lock()
            .ok_or_exit("Failed to get connection lock.");
        match lock.input_stream {
            None => Some(lock.epoll_flags),
            _ => None,
        }
    }

    /// Write a string and its corresponding destination to a socket.
    fn write_reply(&self, reply: &EnclaveProcessReply) -> NitroCliResult<()> {
        let mut lock = self.data.lock().map_err(|e| e.to_string())?;
        if lock.input_stream.is_none() {
            return Err("Cannot write a message to this connection.".to_string());
        }

        let mut stream = lock.input_stream.as_mut().unwrap();
        let reply_bytes = serde_cbor::to_vec(reply).map_err(|e| e.to_string())?;

        write_u64_le(&mut stream, reply_bytes.len() as u64).map_err(|e| e.to_string())?;
        stream.write_all(&reply_bytes).map_err(|e| e.to_string())
    }
}

/// Print a STDOUT message to a connection. Do nothing if the connection is missing.
pub fn safe_conn_println(conn: Option<&Connection>, msg: &str) -> NitroCliResult<()> {
    if conn.is_none() {
        return Ok(());
    }

    conn.unwrap().println(msg)
}

/// Print a STDERR message to a connection. Do nothing if the connection is missing.
pub fn safe_conn_eprintln(conn: Option<&Connection>, msg: &str) -> NitroCliResult<()> {
    if conn.is_none() {
        return Ok(());
    }

    conn.unwrap().eprintln(msg)
}
