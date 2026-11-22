use std::path::{Path, PathBuf};
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    None,
    Token,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub auth_mode: AuthMode,
    pub token: String,
    pub state_dir: PathBuf,
    pub project: PathBuf,
    pub tenant_id: Option<String>,
    pub tenant_purpose: Option<String>,
    pub research_signing_key: Option<String>,
    pub research_mcp_enabled: bool,
    pub venue_base: Option<String>,
}

/// Public, non-secret defaults. Credentials deliberately remain env-only.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileSettings {
    host: Option<String>,
    port: Option<u16>,
    auth_mode: Option<String>,
    state_dir: Option<PathBuf>,
    tenant_id: Option<String>,
    tenant_purpose: Option<String>,
    research_mcp_enabled: Option<bool>,
    venue_base: Option<String>,
}

/// Explicit ENGINE_CONFIG must exist; the conventional project config is optional.
/// A repository-root project uses engine/config/config.json; a project pointing
/// directly at engine/ uses config/config.json.
fn load_file_settings(project: &Path) -> Result<FileSettings, String> {
    let explicit = std::env::var_os("ENGINE_CONFIG").map(PathBuf::from);
    let candidate = if let Some(path) = explicit.as_ref() {
        Some(if path.is_absolute() { path.clone() } else { project.join(path) })
    } else {
        [project.join("engine/config/config.json"), project.join("config/config.json")]
            .into_iter()
            .find(|path| path.is_file())
    };
    let Some(path) = candidate else { return Ok(FileSettings::default()) };
    let body = std::fs::read_to_string(&path)
        .map_err(|error| format!("Cannot read Engine config {}: {error}", path.display()))?;
    serde_json::from_str(&body)
        .map_err(|error| format!("Invalid Engine config {}: {error}", path.display()))
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let project = match std::env::var("ENGINE_PROJECT") {
            Ok(value) => PathBuf::from(value),
            Err(_) => std::env::current_dir().map_err(|error| error.to_string())?,
        };
        let file = load_file_settings(&project)?;
        // Environment wins over file; the file never contains credentials.
        let mut state_dir = std::env::var("ENGINE_STATE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| file.state_dir.unwrap_or_else(|| PathBuf::from(".local/state/engine")));
        if state_dir.is_relative() {
            state_dir = project.join(state_dir);
        }
        let auth = std::env::var("ENGINE_AUTH_MODE")
            .unwrap_or_else(|_| file.auth_mode.unwrap_or_else(|| "none".into()));
        let auth_mode = match auth.as_str() {
            "none" => AuthMode::None,
            "token" => AuthMode::Token,
            _ => return Err("ENGINE_AUTH_MODE must be none or token".into()),
        };
        let port = match std::env::var("ENGINE_PORT") {
            Ok(value) => value.parse::<u16>()
                .map_err(|_| "ENGINE_PORT is invalid".to_string())?,
            Err(_) => file.port.unwrap_or(8766),
        };
        let config = Self {
            host: std::env::var("ENGINE_HOST")
                .unwrap_or_else(|_| file.host.unwrap_or_else(|| "127.0.0.1".into())),
            port,
            auth_mode,
            token: std::env::var("ENGINE_TOKEN").unwrap_or_default(),
            state_dir,
            project,
            tenant_id: std::env::var("ENGINE_TENANT_ID").ok()
                .filter(|value| !value.is_empty())
                .or(file.tenant_id.filter(|value| !value.is_empty())),
            tenant_purpose: std::env::var("ENGINE_TENANT_PURPOSE").ok()
                .filter(|value| !value.is_empty())
                .or(file.tenant_purpose.filter(|value| !value.is_empty())),
            research_signing_key: std::env::var("ENGINE_RESEARCH_SIGNING_KEY")
                .ok().filter(|value| !value.is_empty()),
            research_mcp_enabled: std::env::var("ENGINE_RESEARCH_MCP_ENABLED")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
                .unwrap_or(file.research_mcp_enabled.unwrap_or(false)),
            venue_base: std::env::var("ENGINE_VENUE_BASE").ok()
                .filter(|value| !value.is_empty())
                .or(file.venue_base.filter(|value| !value.is_empty())),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        let url = std::env::var("SEEALPHA_DATABASE_URL")
            .map_err(|_| "SEEALPHA_DATABASE_URL must be configured for PostgreSQL".to_string())?;
        if url.is_empty() || (!url.starts_with("postgres://") && !url.starts_with("postgresql://")) {
            return Err("SEEALPHA_DATABASE_URL must be a PostgreSQL connection URI".into());
        }
        match self.auth_mode {
            AuthMode::Token => {
                if self.token.len() < 32
                    || self.token.chars().any(|char| char == '\n' || char == '\r')
                {
                    return Err(
                        "ENGINE_TOKEN must contain at least 32 characters in token mode".into(),
                    );
                }
            }
            AuthMode::None => {
                if !matches!(self.host.as_str(), "127.0.0.1" | "::1" | "localhost") {
                    return Err("Unauthenticated Engine must listen on loopback".into());
                }
            }
        }
        if let Some(key) = &self.research_signing_key {
            if key.len() < 32 || key.chars().any(|char| char == '\n' || char == '\r') {
                return Err("Research signing key must be at least 32 characters".into());
            }
            if key == &self.token {
                return Err("Research signing key must differ from the Engine token".into());
            }
            if self.tenant_id.is_none() {
                return Err("Research API requires a pinned Engine tenant".into());
            }
        }
        Ok(())
    }


    pub fn book_path(&self) -> PathBuf {
        self.state_dir.join("postgres-state")
    }
}

pub fn ensure_state_dir(path: &Path) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|error| error.to_string())
}

#[cfg(test)]
mod config_file_tests {
    use super::*;

    #[test]
    fn config_file_accepts_public_settings_and_rejects_secrets() {
        let settings: FileSettings = serde_json::from_str(
            r#"{"host":"127.0.0.1","port":8767,"auth_mode":"none",
                "state_dir":".local/state/engine","research_mcp_enabled":false}"#,
        ).unwrap();
        assert_eq!(settings.port, Some(8767));
        assert_eq!(settings.host.as_deref(), Some("127.0.0.1"));
        assert!(serde_json::from_str::<FileSettings>(r#"{"token":"unsafe"}"#).is_err());
        assert!(serde_json::from_str::<FileSettings>(r#"{"port":"invalid"}"#).is_err());
    }
}
