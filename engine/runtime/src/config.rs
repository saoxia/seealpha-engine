use std::path::{Path, PathBuf};

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
    pub python: PathBuf,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let project = match std::env::var("ENGINE_PROJECT") {
            Ok(value) => PathBuf::from(value),
            Err(_) => std::env::current_dir().map_err(|error| error.to_string())?,
        };
        let mut state_dir = std::env::var("ENGINE_STATE")
            .map(PathBuf::from)
            .unwrap_or_else(|_| project.join(".local/state/engine"));
        if state_dir.is_relative() {
            state_dir = project.join(state_dir);
        }
        let auth_mode = match std::env::var("ENGINE_AUTH_MODE")
            .unwrap_or_else(|_| "none".into())
            .as_str()
        {
            "none" => AuthMode::None,
            "token" => AuthMode::Token,
            _ => return Err("ENGINE_AUTH_MODE must be none or token".into()),
        };
        let token = std::env::var("ENGINE_TOKEN").unwrap_or_default();
        let host = std::env::var("ENGINE_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = std::env::var("ENGINE_PORT")
            .unwrap_or_else(|_| "8766".into())
            .parse::<u16>()
            .map_err(|_| "ENGINE_PORT is invalid".to_string())?;
        let research_signing_key = std::env::var("ENGINE_RESEARCH_SIGNING_KEY")
            .ok()
            .filter(|value| !value.is_empty());
        let config = Self {
            host,
            port,
            auth_mode,
            token,
            state_dir,
            project,
            tenant_id: std::env::var("ENGINE_TENANT_ID")
                .ok()
                .filter(|value| !value.is_empty()),
            tenant_purpose: std::env::var("ENGINE_TENANT_PURPOSE")
                .ok()
                .filter(|value| !value.is_empty()),
            research_signing_key,
            research_mcp_enabled: std::env::var("ENGINE_RESEARCH_MCP_ENABLED")
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
                .unwrap_or(false),
            venue_base: std::env::var("ENGINE_VENUE_BASE")
                .ok()
                .filter(|value| !value.is_empty()),
            python: std::env::var("ENGINE_PYTHON")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("python3")),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
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

    pub fn host_script(&self) -> PathBuf {
        self.project.join("python/strategy_host/host.py")
    }

    pub fn book_path(&self) -> PathBuf {
        self.state_dir.join("book.sqlite")
    }
}

pub fn ensure_state_dir(path: &Path) -> Result<(), String> {
    std::fs::create_dir_all(path).map_err(|error| error.to_string())
}
