//! Engine HTTP settings. No tenant or bearer authentication.
use std::path::{Path,PathBuf};

#[derive(Debug,Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub state_dir: PathBuf,
    pub project: PathBuf,
    pub research_mcp_enabled: bool,
}
impl Config {
    pub fn from_env() -> Result<Self,String> {
        let project = std::env::var("ENGINE_PROJECT").map(PathBuf::from)
            .unwrap_or(std::env::current_dir().map_err(|e|e.to_string())?);
        let mut state_dir = std::env::var("ENGINE_STATE").map(PathBuf::from)
            .unwrap_or_else(|_|project.join(".local/state/engine"));
        if state_dir.is_relative() { state_dir = project.join(state_dir); }
        let config = Self {
            host: std::env::var("ENGINE_HOST").unwrap_or_else(|_|"127.0.0.1".into()),
            port: std::env::var("ENGINE_PORT").unwrap_or_else(|_|"8766".into())
                .parse().map_err(|_|"ENGINE_PORT is invalid")?,
            state_dir,
            project,
            research_mcp_enabled: std::env::var("ENGINE_RESEARCH_MCP_ENABLED")
                .is_ok_and(|v|matches!(v.as_str(),"1"|"true"|"TRUE")),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self)->Result<(),String> {
        let url = std::env::var("SEEALPHA_DATABASE_URL")
            .map_err(|_|"SEEALPHA_DATABASE_URL must be configured for PostgreSQL")?;
        if url.is_empty() || !(url.starts_with("postgres://") || url.starts_with("postgresql://")) {
            return Err("SEEALPHA_DATABASE_URL must be a PostgreSQL connection URI".into());
        }
        // No application-layer authentication remains. Exposing this port requires
        // an explicit operator opt-in and an external access boundary.
        if !matches!(self.host.as_str(),"127.0.0.1"|"::1"|"localhost")
            && !std::env::var("ENGINE_ALLOW_UNAUTHENTICATED_REMOTE")
                .is_ok_and(|v|matches!(v.as_str(),"1"|"true"|"TRUE"))
        {
            return Err("Unauthenticated Engine must listen on loopback unless ENGINE_ALLOW_UNAUTHENTICATED_REMOTE=1".into());
        }
        Ok(())
    }

    pub fn book_path(&self)->PathBuf { self.state_dir.join("postgres-state") }
    pub fn ensure_state_dir(&self)->Result<(),std::io::Error>{
        std::fs::create_dir_all(Path::new(&self.state_dir))
    }
}
