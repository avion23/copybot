//! Operator capability loading and authorization, separate from dashboard reads.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt;
use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;

pub enum OperatorAccess {
    ReadOnly,
    Authenticated(OperatorToken),
}

// Deliberately no Debug/Display implementation: capability material is never logged.
pub struct OperatorToken([u8; 32]);

#[derive(Debug)]
pub enum ConfigError {
    InvalidPath,
    File(std::io::Error),
    UnsafeFile,
    OversizedFile,
    DuplicateToken,
    InvalidToken,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidPath => "COPYBOT_OPERATOR_ENV_FILE must be an absolute path",
            Self::File(_) => "cannot read the operator environment file",
            Self::UnsafeFile => "operator environment file must be a regular file owned by this user with mode 0600",
            Self::OversizedFile => "operator environment file exceeds 64 KiB",
            Self::DuplicateToken => "operator environment file repeats COPYBOT_OPERATOR_TOKEN",
            Self::InvalidToken => "COPYBOT_OPERATOR_TOKEN must be an unquoted 64-character hexadecimal capability",
        };
        f.write_str(message)
    }
}
impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::File(error) => Some(error),
            Self::InvalidPath | Self::UnsafeFile | Self::OversizedFile
            | Self::DuplicateToken | Self::InvalidToken => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorAction {
    EditWallets,
    SetArmState,
    Flatten,
    ClearIncident,
}

impl OperatorAction {
    pub fn from_route(method: &str, path: &str) -> Option<Self> {
        match (method, path) {
            ("POST", "/api/wallets") => Some(Self::EditWallets),
            ("POST", "/api/arm") => Some(Self::SetArmState),
            ("POST", "/api/flatten") => Some(Self::Flatten),
            ("POST", "/api/incident/clear") => Some(Self::ClearIncident),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::EditWallets => "edit_wallets",
            Self::SetArmState => "set_arm_state",
            Self::Flatten => "flatten",
            Self::ClearIncident => "clear_incident",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationError {
    ReadOnly,
    MissingCredential,
    InvalidCredential,
    MalformedHeaders,
    CrossOrigin,
}

impl AuthorizationError {
    pub fn response(self) -> (&'static str, &'static str) {
        match self {
            Self::ReadOnly => ("403 Forbidden", "operator mutations are disabled"),
            Self::MissingCredential | Self::InvalidCredential => {
                ("401 Unauthorized", "valid operator capability required")
            }
            Self::MalformedHeaders => ("400 Bad Request", "ambiguous or malformed operator headers"),
            Self::CrossOrigin => ("403 Forbidden", "cross-origin operator mutation refused"),
        }
    }
}

pub struct AuthorizedAction(OperatorAction);
impl AuthorizedAction {
    pub fn action(&self) -> OperatorAction {
        self.0
    }
}

fn token_bytes(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    hex::decode_to_slice(text, &mut bytes).ok()?;
    Some(bytes)
}

fn single_header<'a>(request: &'a str, name: &str) -> Result<Option<&'a str>, AuthorizationError> {
    let mut found = None;
    for line in request.lines().skip(1).take_while(|line| !line.is_empty()) {
        if line.starts_with(' ') || line.starts_with('\t') {
            return Err(AuthorizationError::MalformedHeaders);
        }
        let (key, value) = line.split_once(':').ok_or(AuthorizationError::MalformedHeaders)?;
        if key.trim() != key {
            return Err(AuthorizationError::MalformedHeaders);
        }
        if key.eq_ignore_ascii_case(name) {
            if found.is_some() {
                return Err(AuthorizationError::MalformedHeaders);
            }
            found = Some(value.trim());
        }
    }
    Ok(found)
}

impl OperatorAccess {
    pub fn from_environment() -> Result<Self, ConfigError> {
        match std::env::var("COPYBOT_OPERATOR_ENV_FILE") {
            Err(std::env::VarError::NotPresent) => Ok(Self::ReadOnly),
            Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::InvalidPath),
            Ok(path) if path.is_empty() => Ok(Self::ReadOnly),
            Ok(path) => Self::from_env_file(Path::new(&path)),
        }
    }

    pub fn from_env_file(path: &Path) -> Result<Self, ConfigError> {
        if !path.is_absolute() {
            return Err(ConfigError::InvalidPath);
        }
        let file = OpenOptions::new().read(true).custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path).map_err(ConfigError::File)?;
        let metadata = file.metadata().map_err(ConfigError::File)?;
        // The service must own its credential file, not merely inherit a proxy identity.
        let owner = unsafe { libc::geteuid() };
        if !metadata.is_file() || metadata.permissions().mode() & 0o7777 != 0o600
            || metadata.uid() != owner {
            return Err(ConfigError::UnsafeFile);
        }
        let mut source = String::new();
        file.take(65_537).read_to_string(&mut source).map_err(ConfigError::File)?;
        if source.len() > 65_536 {
            return Err(ConfigError::OversizedFile);
        }
        let mut value = None;
        for line in source.lines() {
            let Some((key, candidate)) = line.trim().split_once('=') else { continue };
            if key.trim() != "COPYBOT_OPERATOR_TOKEN" {
                continue;
            }
            if value.is_some() {
                return Err(ConfigError::DuplicateToken);
            }
            value = Some(candidate.trim());
        }
        match value {
            None | Some("") => Ok(Self::ReadOnly),
            Some(text) => token_bytes(text).map(|key| Self::Authenticated(OperatorToken(key)))
                .ok_or(ConfigError::InvalidToken),
        }
    }

    pub fn enabled(&self) -> bool {
        matches!(self, Self::Authenticated(_))
    }

    pub fn authorize(
        &self,
        action: OperatorAction,
        request: &str,
    ) -> Result<AuthorizedAction, AuthorizationError> {
        let Self::Authenticated(expected) = self else {
            return Err(AuthorizationError::ReadOnly);
        };
        let header = single_header(request, "authorization")?
            .ok_or(AuthorizationError::MissingCredential)?;
        let (scheme, token) = header.split_once(' ').ok_or(AuthorizationError::InvalidCredential)?;
        if !scheme.eq_ignore_ascii_case("bearer") {
            return Err(AuthorizationError::InvalidCredential);
        }
        let supplied = token_bytes(token).ok_or(AuthorizationError::InvalidCredential)?;
        // Mac::verify_slice supplies the constant-time comparison; both keys are fixed-size.
        let mut expected_mac = Hmac::<Sha256>::new_from_slice(&expected.0)
            .expect("HMAC accepts a 32-byte key");
        expected_mac.update(b"copybot-operator-capability-v1");
        let tag = expected_mac.finalize().into_bytes();
        let mut supplied_mac = Hmac::<Sha256>::new_from_slice(&supplied)
            .expect("HMAC accepts a 32-byte key");
        supplied_mac.update(b"copybot-operator-capability-v1");
        supplied_mac.verify_slice(&tag).map_err(|_| AuthorizationError::InvalidCredential)?;
        let host = single_header(request, "host")?
            .filter(|host| !host.is_empty()).ok_or(AuthorizationError::MalformedHeaders)?;
        if let Some(origin) = single_header(request, "origin")? {
            if origin != format!("https://{host}") && origin != format!("http://{host}") {
                return Err(AuthorizationError::CrossOrigin);
            }
        }
        Ok(AuthorizedAction(action))
    }
}
