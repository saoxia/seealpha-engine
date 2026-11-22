//! Explicit opt-in native Binance source supervisor, one child per product.
//! Only read-only public market data; it NEVER marks a producer READY until
//! native REST handover, clock and Catalog validation are complete.
use crate::{Runtime, RuntimeError};
use seealpha_binance_market_data_source::{Product, parse_manifest};
use serde_json::{Value, json};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

fn bad(message: &str) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}

fn source_product(name: &str) -> Result<Product, RuntimeError> {
    match Product::parse(name) {
        Some(Product::Spot) => Ok(Product::Spot),
        Some(Product::FuturesUm) => Ok(Product::FuturesUm),
        _ => Err(bad("期权源尚未实现安全的原生订阅与 Catalog 交接。")),
    }
}
fn explicit_enable() -> bool {
    std::env::var("ENGINE_EXPERIMENTAL_SOURCE")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE"))
}
/// Only existing, real tenant/product directories may receive a native
/// Nautilus data stream. A symlink to another tenant is not acceptable even
/// when its canonical target happens to live under the administrator root.
pub(crate) fn native_tenant_product_root(
    base: &str,
    tenant: &str,
    product: &str,
) -> Result<PathBuf, RuntimeError> {
    if tenant.is_empty() || tenant.len() > 64 || !tenant.bytes().all(|x| x.is_ascii_alphanumeric())
    {
        return Err(bad("无效的源租户目录名称。"));
    }
    let root = PathBuf::from(base)
        .canonicalize()
        .map_err(|_| bad("原生 Nautilus 数据根目录尚未创建。"))?;
    if !root.is_dir() {
        return Err(bad("原生 Nautilus 数据根目录不是现有目录。"));
    }
    let tenant_path = root.join(tenant);
    let product_path = tenant_path.join(product);
    for path in [&tenant_path, &product_path] {
        let metadata =
            fs::symlink_metadata(path).map_err(|_| bad("必须提前创建租户与产品数据目录。"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(bad("禁止使用符号链接作为租户产品数据目录。"));
        }
    }
    product_path
        .canonicalize()
        .map_err(|_| bad("源产品目录无效。"))
}

/// Derives the *same* authorized native subscriptions from the committed
/// policy manifest. No independent strategy subscription state is introduced.
fn enabled_native_symbols(manifest: &[u8], product: Product) -> Result<String, RuntimeError> {
    let rows =
        parse_manifest(manifest, product).map_err(|_| bad("Nautilus 行情源策略清单无效。"))?;
    let symbols = rows
        .into_iter()
        .filter(|entry| entry.realtime_enabled)
        .map(|entry| entry.symbol)
        .collect::<Vec<_>>();
    if symbols.is_empty() || symbols.len() > 100 {
        return Err(bad("单个实验性 Nautilus 节点只允许 1 到 100 个交易对。"));
    }
    Ok(symbols.join(","))
}

fn manifest_bytes(policies: &[Value], product: Product) -> Result<Vec<u8>, RuntimeError> {
    let mut entries = Vec::new();
    for policy in policies {
        if policy["product"] != product.as_str() {
            continue;
        }
        let base = policy["base_level"].as_str().unwrap_or("1m");
        entries.push(json!({
            "product":product.as_str(),
            "symbol":policy["symbol"],
            "base_level":if base=="auto" {"1m"} else {base},
            "realtime_enabled":policy["realtime_enabled"].as_bool().unwrap_or(true),
        }));
    }
    if entries.iter().all(|item| item["realtime_enabled"] == false) || entries.is_empty() {
        return Err(bad("尚无启用的原生市场数据源配置。"));
    }
    let bytes = serde_json::to_vec(&json!({"version":1,"policies":entries}))
        .map_err(|_| bad("数据源清单无效。"))?;
    parse_manifest(&bytes, product).map_err(|_| bad("Binance 产品源清单校验未通过。"))?;
    Ok(bytes)
}
impl Runtime {
    /// Nautilus is the only supported Binance live data worker.
    /// No legacy websocket, journal or calculation process is launched.
    pub fn start_native_source(&self, tenant: &str, product: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        if !explicit_enable() {
            return Err(bad(
                "原生数据源尚未通过 READY 验收；需显式启用 ENGINE_EXPERIMENTAL_SOURCE。",
            ));
        }
        let source = source_product(product)?;
        let native_binary = std::env::var("ENGINE_NATIVE_NAUTILUS_SOURCE_BIN")
            .ok()
            .filter(|path| !path.is_empty())
            .ok_or_else(|| bad("必须先安装并配置 Nautilus 原生数据进程；旧采集器已移除。"))?;
        let catalog_base = std::env::var("ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT")
            .map_err(|_| bad("未配置 Nautilus Catalog 管理根目录。"))?;
        let feather_base = std::env::var("ENGINE_NATIVE_NAUTILUS_FEATHER_ROOT")
            .map_err(|_| bad("未配置 Nautilus Feather 管理根目录。"))?;
        let catalog = native_tenant_product_root(&catalog_base, tenant, product)?;
        let feather = native_tenant_product_root(&feather_base, tenant, product)?;
        let current = self.control.policies(tenant).map_err(crate::map_control)?;
        let manifest = manifest_bytes(&current, source)?;
        let symbols = enabled_native_symbols(&manifest, source)?;
        let key = format!("{tenant}:{product}");
        let mut processes = self
            .source_processes
            .lock()
            .map_err(|_| bad("数据源进程状态锁不可用。"))?;
        if let Some(child) = processes.get_mut(&key)
            && child
                .try_wait()
                .map_err(|_| bad("数据源进程状态未知。"))?
                .is_none()
        {
            return Err(bad("该产品 Nautilus 数据源已在运行。"));
        }
        processes.remove(&key);
        let child = Command::new(native_binary)
            .arg(product)
            .arg(symbols)
            .arg(feather)
            .arg(&catalog)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| bad("无法启动 Nautilus 原生数据源。"))?;
        let pid = child.id();
        processes.insert(key, child);
        Ok(json!({"tenant_id":tenant,"product":product,"pid":pid,
            "status":"experimental_degraded","data_ready":false,
            "allow_new_risk":false,"exchange_orders":0,"root":catalog,
            "backend":"nautilus_native_data",
            "message":"Nautilus 原生数据源尚未完成 REST/WS/ZIP 历史衔接与 READY 验收。"}))
    }

    pub fn stop_native_source(&self, tenant: &str, product: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        source_product(product)?;
        let key = format!("{tenant}:{product}");
        let mut processes = self
            .source_processes
            .lock()
            .map_err(|_| bad("数据源进程状态锁不可用。"))?;
        let mut child = processes
            .remove(&key)
            .ok_or_else(|| bad("没有本进程管理的对应原生数据源。"))?;
        // SIGKILL bypasses Nautilus LiveNode's Feather -> Parquet flush and
        // leaves recoverable partial files. Prefer SIGTERM and retain the child
        // on failed/timed-out shutdown; never report a catalog commit here.
        let already_exited = child
            .try_wait()
            .map_err(|_| bad("数据源进程状态未知。"))?
            .is_some();
        if !already_exited {
            let signal = Command::new("/bin/kill")
                .arg("-TERM")
                .arg(child.id().to_string())
                .status();
            if !signal.is_ok_and(|result| result.success()) {
                processes.insert(key, child);
                return Err(bad("无法向原生数据源发送 SIGTERM，保留进程管理状态。"));
            }
            let deadline = std::time::Instant::now()
                .checked_add(std::time::Duration::from_secs(30))
                .ok_or_else(|| bad("数据源关闭截止时间无效。"))?;
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => break,
                    Ok(None) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                    }
                    Ok(None) => {
                        processes.insert(key, child);
                        return Err(bad("数据源仍在完成退出/写入，不能报告成功关闭。"));
                    }
                    Err(_) => {
                        processes.insert(key, child);
                        return Err(bad("数据源停止状态待验证，保留进程管理状态。"));
                    }
                }
            }
        }
        Ok(json!({"tenant_id":tenant,"product":product,
            "status":"stopped","data_ready":false,"catalog_committed":false}))
    }

    pub fn native_source_status(&self, tenant: &str, product: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        source_product(product)?;
        let key = format!("{tenant}:{product}");
        let mut processes = self
            .source_processes
            .lock()
            .map_err(|_| bad("数据源进程状态锁不可用。"))?;
        let (running, pid) = if let Some(child) = processes.get_mut(&key) {
            let active = child
                .try_wait()
                .map_err(|_| bad("数据源进程状态未知。"))?
                .is_none();
            (active, Some(child.id()))
        } else {
            (false, None)
        };
        // The old status.json/SQLite journal is gone. Authoritative runtime
        // coverage must be established from native Catalog proof, not socket
        // connectivity. Only list authorized symbols here.
        let symbols = self
            .control
            .policies(tenant)
            .map_err(crate::map_control)?
            .into_iter()
            .filter(|policy| policy["product"] == product)
            .map(|policy| {
                json!({
                    "symbol":policy["symbol"],
                    "realtime_enabled":policy["realtime_enabled"],
                    "evidence":null
                })
            })
            .collect::<Vec<_>>();
        // Process/socket connectivity is never a substitute for exchange
        // confirmed readiness; nothing here allows new directional risk.
        Ok(json!({"tenant_id":tenant,"product":product,"pid":pid,
            "running":running,"data_ready":false,"allow_new_risk":false,
            "symbols":symbols,"status":if running {"degraded"} else {"stopped"} }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nautilus_native_symbols_reuse_only_active_validated_policies() {
        let policies = vec![
            json!({"product":"spot","symbol":"ETHUSDT","base_level":"1m","realtime_enabled":true}),
            json!({"product":"spot","symbol":"BTCUSDT","base_level":"1m","realtime_enabled":false}),
        ];
        let manifest = manifest_bytes(&policies, Product::Spot).unwrap();
        assert_eq!(
            enabled_native_symbols(&manifest, Product::Spot).unwrap(),
            "ETHUSDT"
        );
    }

    #[test]
    fn nautilus_root_is_tenant_and_market_scoped() {
        let dir = std::env::temp_dir().join(format!(
            "seealpha-native-root-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let dir_string = dir.to_string_lossy().into_owned();
        std::fs::create_dir_all(dir.join("tenant1").join("spot")).unwrap();
        let got = native_tenant_product_root(&dir_string, "tenant1", "spot").unwrap();
        assert_eq!(got, dir.join("tenant1").join("spot"));
        assert!(native_tenant_product_root(&dir_string, "../tenant1", "spot").is_err());
        assert!(native_tenant_product_root(&dir_string, "tenant2", "spot").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn nautilus_root_rejects_cross_tenant_symlinks() {
        use std::os::unix::fs::symlink;
        let dir = std::env::temp_dir().join(format!(
            "seealpha-native-symlink-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(dir.join("tenant1").join("spot")).unwrap();
        symlink(dir.join("tenant1"), dir.join("tenant2")).unwrap();
        assert!(native_tenant_product_root(&dir.to_string_lossy(), "tenant2", "spot").is_err());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn source_manifest_rejects_invalid_policy_before_starting_any_process() {
        let policies = vec![
            json!({"product":"spot","symbol":"ETHUSDT","base_level":"1m","realtime_enabled":true}),
        ];
        let raw = manifest_bytes(&policies, Product::Spot).unwrap();
        assert_eq!(parse_manifest(&raw, Product::Spot).unwrap().len(), 1);
        let invalid = vec![
            json!({"product":"options","symbol":"ALL","base_level":"1m","realtime_enabled":true}),
        ];
        assert!(manifest_bytes(&invalid, Product::Options).is_err());
    }
}
