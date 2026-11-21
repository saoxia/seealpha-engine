//! Explicit opt-in native Binance source supervisor, one child per product.
//! Only read-only public market data; it NEVER marks a producer READY until
//! native REST handover, clock and Catalog validation are complete.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_binance_market_data_source::{Product, parse_manifest};
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::Write;
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
pub(crate) fn source_root(
    project: &std::path::Path,
    product: &str,
) -> Result<PathBuf, RuntimeError> {
    let root = std::env::var("ENGINE_SOURCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| project.join("data").join("sources"));
    root.join(product)
        .canonicalize()
        .map_err(|_| bad("原生产品源根目录尚未创建，拒绝隐式创建新数据卷。"))
}
/// Only existing, real tenant/product directories may receive a native
/// Nautilus data stream. A symlink to another tenant is not acceptable even
/// when its canonical target happens to live under the administrator root.
pub(crate) fn native_tenant_product_root(
    base: &str,
    tenant: &str,
    product: &str,
) -> Result<PathBuf, RuntimeError> {
    if tenant.is_empty() || tenant.len() > 64
        || !tenant.bytes().all(|x| x.is_ascii_alphanumeric())
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
        let metadata = fs::symlink_metadata(path)
            .map_err(|_| bad("必须提前创建租户与产品数据目录。"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(bad("禁止使用符号链接作为租户产品数据目录。"));
        }
    }
    product_path.canonicalize().map_err(|_| bad("源产品目录无效。"))
}

/// Derives the *same* authorized native subscriptions from the committed
/// policy manifest. No independent strategy subscription state is introduced.
fn enabled_native_symbols(manifest: &[u8], product: Product) -> Result<String, RuntimeError> {
    let rows = parse_manifest(manifest, product)
        .map_err(|_| bad("Nautilus 行情源策略清单无效。"))?;
    let symbols = rows.into_iter()
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
fn atomic_manifest(path: &std::path::Path, bytes: &[u8]) -> Result<(), RuntimeError> {
    let tmp = path.with_extension("json.tmp");
    let result = (|| {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|_| bad("无法写入数据源清单。"))?;
        file.write_all(bytes)
            .map_err(|_| bad("无法写入数据源清单。"))?;
        file.sync_all().map_err(|_| bad("无法同步数据源清单。"))?;
        fs::rename(&tmp, path).map_err(|_| bad("无法原子替换数据源清单。"))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

impl Runtime {
    pub fn start_native_source(&self, tenant: &str, product: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        if !explicit_enable() {
            return Err(bad(
                "原生数据源尚未通过 READY 验收；需明确启用 ENGINE_EXPERIMENTAL_SOURCE。",
            ));
        }
        let source = source_product(product)?;
        let native_binary = std::env::var("ENGINE_NATIVE_NAUTILUS_SOURCE_BIN")
            .ok()
            .filter(|path| !path.is_empty());
        let root = if native_binary.is_some() {
            let catalog_base = std::env::var("ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT")
                .map_err(|_| bad("未配置 Nautilus Catalog 管理根目录。"))?;
            native_tenant_product_root(&catalog_base, tenant, product)?
        } else {
            source_root(&self.project, product)?
        };
        if !root.is_dir() {
            return Err(bad("数据源根目录不是现有目录。"));
        }
        let current = self.ledger.policies(tenant).map_err(map_ledger)?;
        let manifest = manifest_bytes(&current, source)?;
        let key = format!("{tenant}:{product}");
        let mut processes = self
            .source_processes
            .lock()
            .map_err(|_| bad("数据源进程状态锁不可用。"))?;
        let running = match processes.get_mut(&key) {
            Some(child) => child
                .try_wait()
                .map_err(|_| bad("数据源进程状态未知。"))?
                .is_none(),
            None => false,
        };
        if running {
            return Err(bad("该产品数据源已经运行，禁止重复启动。"));
        }
        processes.remove(&key);
        let safe_tenant: String = tenant
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect();
        if safe_tenant != tenant || tenant.is_empty() || tenant.len() > 64 {
            return Err(bad("租户编号无效。"));
        }
        let manifest_path = self
            .source_state_dir
            .join(format!("source-{safe_tenant}-{product}.json"));
        atomic_manifest(&manifest_path, &manifest)?;
        // A native Nautilus client is used only when the maintainer
        // explicitly installs/points at its verified binary; otherwise the
        // existing experimental collector remains unchanged.
        let mut command = if let Some(binary) = native_binary.as_deref() {
            let symbols = enabled_native_symbols(&manifest, source)?;
            let feather_base = std::env::var("ENGINE_NATIVE_NAUTILUS_FEATHER_ROOT")
                .map_err(|_| bad("未配置 Nautilus Feather 管理根目录。"))?;
            let feather = native_tenant_product_root(&feather_base, tenant, product)?;
            let mut command = Command::new(binary);
            command.arg(product).arg(symbols).arg(feather).arg(&root);
            command
        } else {
            let binary = std::env::var("ENGINE_SOURCE_BIN")
                .unwrap_or_else(|_| "binance-native-source".to_string());
            let mut command = Command::new(binary);
            command.arg(product).arg(&manifest_path).arg(&root);
            command
        };
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| bad("无法启动独立的 Rust 市场数据源进程。"))?;
        let pid = child.id();
        processes.insert(key, child);
        Ok(json!({"tenant_id":tenant,"product":product,"pid":pid,
            "status":"experimental_degraded","data_ready":false,
            "allow_new_risk":false,"exchange_orders":0,"root":root,"manifest":manifest_path,
            "backend":if native_binary.is_some() {"nautilus_native_data"} else {"legacy_experimental"},
            "message":"源仍处于 DEGRADED，未完成历史衔接与数据质量验收。"}))
    }

    /// A separate calculation process consumes the native source journal.
    /// Starting it never turns source status into READY or opens an order API.
    pub fn start_native_calculation(
        &self,
        tenant: &str,
        product: &str,
        symbol: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        if std::env::var("ENGINE_NATIVE_NAUTILUS_SOURCE_BIN")
            .is_ok_and(|path| !path.is_empty())
        {
            return Err(bad("Nautilus 原生 Catalog 源尚未验证旧 SQLite K 线计算器输入协议。"));
        }
        if !explicit_enable() {
            return Err(bad("原生行情计算尚未验收，必须显式启用实验开关。"));
        }
        source_product(product)?;
        seealpha_market::check_symbol(symbol).map_err(|_| bad("行情品种无效。"))?;
        let policies = self.ledger.policies(tenant).map_err(map_ledger)?;
        let policy = policies
            .iter()
            .find(|item| {
                item["product"] == product
                    && item["symbol"] == symbol
                    && item["realtime_enabled"] == true
            })
            .ok_or_else(|| bad("行情未得到持久化源授权。"))?;
        let base = match policy["base_level"].as_str().unwrap_or("1m") {
            "auto" => "1m",
            value => value,
        };
        let retained = policy["intervals"]
            .as_array()
            .map(|rows| rows.iter().filter_map(Value::as_str).collect::<Vec<_>>())
            .unwrap_or_else(|| vec!["1m"]);
        for interval in &retained {
            if *interval != "1M" && seealpha_market::interval_ms(interval).is_err() {
                return Err(bad("行情生成周期无效。"));
            }
        }
        let root = source_root(&self.project, product)?;
        let pair = root
            .join(symbol)
            .join(base)
            .canonicalize()
            .map_err(|_| bad("源行情子目录尚未创建。"))?;
        if !pair.starts_with(&root) || !pair.join("collection.sqlite").is_file() {
            return Err(bad("行情原始成交数据卷尚未确认可读取。"));
        }
        let key = format!("{tenant}:{product}:calc:{symbol}");
        let mut processes = self
            .source_processes
            .lock()
            .map_err(|_| bad("计算进程状态锁不可用。"))?;
        if let Some(existing) = processes.get_mut(&key)
            && existing
                .try_wait()
                .map_err(|_| bad("计算进程状态未知。"))?
                .is_none()
        {
            return Err(bad("计算进程已运行。"));
        }
        processes.remove(&key);
        let binary = std::env::var("ENGINE_CALC_BIN")
            .unwrap_or_else(|_| "binance-native-calculation".into());
        let child = Command::new(binary)
            .arg(product)
            .arg(symbol)
            .arg(base)
            .arg(&pair)
            .arg(retained.join(","))
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|_| bad("独立 Rust 计算进程启动失败。"))?;
        let pid = child.id();
        processes.insert(key, child);
        Ok(json!({"tenant_id":tenant,"product":product,"symbol":symbol,
            "pid":pid,"status":"experimental_degraded","data_ready":false,
            "message":"计算产物尚须经过完整历史修复与 Catalog 检查才能用于实盘。"}))
    }
    pub fn stop_native_calculation(
        &self,
        tenant: &str,
        product: &str,
        symbol: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        source_product(product)?;
        seealpha_market::check_symbol(symbol).map_err(|_| bad("品种无效。"))?;
        let key = format!("{tenant}:{product}:calc:{symbol}");
        let mut processes = self
            .source_processes
            .lock()
            .map_err(|_| bad("计算进程状态锁不可用。"))?;
        let mut child = processes
            .remove(&key)
            .ok_or_else(|| bad("该 Rust 计算进程未由本 Engine 管理。"))?;
        child.kill().map_err(|_| bad("无法终止计算进程。"))?;
        child.wait().map_err(|_| bad("计算进程停止状态未知。"))?;
        Ok(json!({"tenant_id":tenant,"product":product,"symbol":symbol,
            "status":"stopped","data_ready":false}))
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
        // Preserve legacy collector stop semantics. Only the explicitly
        // selected Nautilus writer needs a graceful Feather -> Parquet flush.
        if !std::env::var("ENGINE_NATIVE_NAUTILUS_SOURCE_BIN")
            .is_ok_and(|value| !value.is_empty())
        {
            child.kill().map_err(|_| bad("无法终止实验性数据源进程。"))?;
            child.wait().map_err(|_| bad("实验性数据源退出状态未知。"))?;
            return Ok(json!({"tenant_id":tenant,"product":product,
                "status":"stopped","data_ready":false}));
        }
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
        let product_root = source_root(&self.project, product).ok();
        let mut symbols = Vec::new();
        if let Some(root) = product_root {
            for policy in self.ledger.policies(tenant).map_err(map_ledger)? {
                if policy["product"] != product {
                    continue;
                }
                let symbol = policy["symbol"].as_str().unwrap_or("");
                let base = policy["base_level"].as_str().unwrap_or("1m");
                let path = root
                    .join(symbol)
                    .join(if base == "auto" { "1m" } else { base })
                    .join("status.json");
                let evidence = fs::read(path)
                    .ok()
                    .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok());
                symbols.push(json!({"symbol":symbol,"evidence":evidence}));
            }
        }
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
        assert_eq!(enabled_native_symbols(&manifest, Product::Spot).unwrap(), "ETHUSDT");
    }

    #[test]
    fn nautilus_root_is_tenant_and_market_scoped() {
        let dir = std::env::temp_dir().join(format!(
            "seealpha-native-root-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
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
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(dir.join("tenant1").join("spot")).unwrap();
        symlink(dir.join("tenant1"), dir.join("tenant2")).unwrap();
        assert!(native_tenant_product_root(
            &dir.to_string_lossy(), "tenant2", "spot"
        ).is_err());
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
