//! Tenant-scoped, optional ClickHouse historical source settings.
use crate::{Runtime, RuntimeError};
use seealpha_market::clickhouse::ClickHouseConfig;
use serde_json::{Value, json};

impl Runtime {
    pub fn clickhouse_settings(&self, tenant: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        let saved = self.control.clickhouse_settings(tenant)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        let Some(saved) = saved else {
            return Ok(json!({"enabled":false,"configured":false,"password_configured":false}));
        };
        Ok(json!({
            "enabled":saved["enabled"],
            "configured":true,
            "url":saved["url"],
            "database":saved["database"],
            "user":saved["user"],
            "password_configured":saved["password"].as_str().is_some_and(|x| !x.is_empty())
        }))
    }

    /// Password is write-only. Omitted password retains the existing secret.
    /// Once an API setting exists, it supersedes environment variables for this tenant.
    pub fn update_clickhouse_settings(&self, tenant: &str, body: &Value) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        let enabled = body["enabled"].as_bool()
            .ok_or_else(|| RuntimeError::BadRequest("ClickHouse enabled 必须为布尔值。".into()))?;
        let existing = self.control.clickhouse_settings(tenant)
            .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        let previous = existing.as_ref();
        let value = |name: &str, default: &str| -> Result<String, RuntimeError> {
            match body.get(name) {
                Some(Value::String(s)) => Ok(s.clone()),
                Some(_) => Err(RuntimeError::BadRequest(format!("ClickHouse {name} 必须为字符串。"))),
                None => Ok(previous.and_then(|x| x[name].as_str()).unwrap_or(default).to_owned()),
            }
        };
        let config = ClickHouseConfig {
            url: value("url", "")?,
            database: value("database", "default")?,
            user: value("user", "default")?,
            password: value("password", "")?,
        };
        if enabled || !config.url.is_empty() {
            config.validate().map_err(|_| RuntimeError::BadRequest("ClickHouse 连接参数无效。".into()))?;
        }
        if config.password.len() > 4096 || config.url.len() > 2048 {
            return Err(RuntimeError::BadRequest("ClickHouse 配置长度超出上限。".into()));
        }
        self.control.save_clickhouse_settings(
            tenant,enabled,&config.url,&config.database,&config.user,&config.password,
        ).map_err(|e| RuntimeError::Conflict(e.to_string()))?;
        self.clickhouse_settings(tenant)
    }

    /// Used only when the ZIP downloader fails; no automatic connection on startup.
    pub(crate) fn optional_clickhouse_config(&self, tenant: &str) -> Option<ClickHouseConfig> {
        match self.control.clickhouse_settings(tenant) {
            Ok(Some(saved)) => {
                if saved["enabled"] != true { return None; }
                let config = ClickHouseConfig {
                    url: saved["url"].as_str()?.to_owned(),
                    database: saved["database"].as_str()?.to_owned(),
                    user: saved["user"].as_str()?.to_owned(),
                    password: saved["password"].as_str()?.to_owned(),
                };
                config.validate().ok()?;
                Some(config)
            }
            Ok(None) => ClickHouseConfig::from_env().ok()?,
            Err(_) => None,
        }
    }
}
