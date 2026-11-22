//! Native historical-data jobs and control-plane operations.
//! Historical jobs use only public Binance klines and commit atomically to
//! the Rust catalog after the complete requested interval is validated.
use crate::{Runtime, RuntimeError};
use seealpha_binance_market_data_source::archive_fetch::ArchiveSpec;
use crate::catalog::CatalogBar;
use seealpha_catalog::{DAY_MS, CatalogFunding};
use seealpha_market::futures_repair::{RepairNextStep, scan_day};
use seealpha_market::spot_repair;
use seealpha_market::funding::FundingSource;
use seealpha_market::options::{HISTORICAL_DATA_KIND as OPTION_HISTORY_KIND, OptionsSource, parse_option_symbol};
use seealpha_market::{BinanceSource, Product, check_symbol, interval_ms};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::fs;

fn map_state<E: std::fmt::Display>(error:E)->RuntimeError { RuntimeError::Conflict(error.to_string()) }
fn bad(message: &str) -> RuntimeError {
    RuntimeError::Unprocessable(message.into())
}
fn historical_backfill_kind_allowed(product: &str, kind: &str) -> bool {
    // Keep this policy close to the job boundary; see Binance OptionsSource::history.
    match product {
        "options" => kind == OPTION_HISTORY_KIND,
        "futures_um" => matches!(kind, "bars" | "funding"),
        "spot" => kind == "bars",
        _ => false,
    }
}

fn repair_archive_dataset(action: &str, product: &str) -> Option<&'static str> {
    match (action, product) {
        ("repair_spot_day", "spot") => Some("trades"),
        ("repair_futures_day", "futures_um") => Some("aggTrades"),
        _ => None,
    }
}

fn field<'a>(object: &'a Value, name: &str) -> Result<&'a str, RuntimeError> {
    object
        .get(name)
        .and_then(Value::as_str)
        .filter(|x| !x.trim().is_empty())
        .ok_or_else(|| bad("行情任务参数无效。"))
}
fn valid_symbol(product: &str, symbol: &str) -> bool {
    match product {
        "spot" | "futures_um" => check_symbol(symbol).is_ok(),
        "options" => {
            symbol == "ALL"
                || ((3..=30).contains(&symbol.len())
                    && symbol.chars().all(|c| {
                        c.is_ascii_uppercase() || c.is_ascii_digit() || c == '-' || c == '.'
                    }))
        }
        _ => false,
    }
}
fn valid_policy(policy: &Value) -> Result<(String, String), RuntimeError> {
    let product = policy
        .get("product")
        .and_then(Value::as_str)
        .unwrap_or("futures_um");
    let symbol = field(policy, "symbol")?;
    if !valid_symbol(product, symbol) {
        return Err(bad("行情产品或交易对无效。"));
    }
    let base = policy
        .get("base_level")
        .and_then(Value::as_str)
        .unwrap_or("1m");
    if base != "auto"
        && base != "tick"
        && base != "1s"
        && interval_ms(base).is_err()
        && !matches!(base, "2h" | "6h" | "8h" | "12h" | "3d" | "1w" | "1M")
    {
        return Err(bad("最小保存粒度无效。"));
    }
    if let Some(intervals) = policy.get("intervals") {
        let arr = intervals.as_array().ok_or_else(|| bad("生成周期无效。"))?;
        if arr.len() > 16
            || arr.iter().any(|v| {
                v.as_str().is_none_or(|t| {
                    interval_ms(t).is_err()
                        && !matches!(t, "2h" | "6h" | "8h" | "12h" | "3d" | "1w" | "1M")
                })
            })
        {
            return Err(bad("生成周期无效。"));
        }
    }
    for key in ["price_precision", "size_precision"] {
        if let Some(precision) = policy.get(key)
            && precision.as_u64().is_none_or(|value| value > 16)
        {
            return Err(bad("Nautilus Instrument 精度必须为 0..16。"));
        }
    }
    if let Some(days) = policy.get("history_days")
        && days.as_i64().is_none_or(|x| !(1..=36500).contains(&x))
    {
        return Err(bad("历史天数无效。"));
    }
    Ok((product.to_owned(), symbol.to_owned()))
}

fn hash_job(tenant_id: &str, payload: &Value) -> String {
    let mut digest = Sha256::new();
    digest.update(tenant_id.as_bytes());
    digest.update(payload.to_string().as_bytes());
    let b = digest.finalize();
    let mut out = String::new();
    for byte in b.iter().take(16) {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

/// Only existing, real tenant/product directories may receive a native
/// Nautilus data stream. A symlink to another tenant is not acceptable even
/// when its canonical target happens to live under the administrator root.
fn native_tenant_product_root(
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
            std::fs::symlink_metadata(path).map_err(|_| bad("必须提前创建租户与产品数据目录。"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(bad("禁止使用符号链接作为租户产品数据目录。"));
        }
    }
    product_path
        .canonicalize()
        .map_err(|_| bad("源产品目录无效。"))
}


impl Runtime {
    pub fn sync_data_policies(
        &self,
        tenant_id: &str,
        policies: &[Value],
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if policies.len() > 2500 {
            return Err(bad("行情配置数量超出范围。"));
        }
        let mut names = BTreeSet::new();
        for policy in policies {
            let key = valid_policy(policy)?;
            if !names.insert(key) {
                return Err(bad("行情配置包含重复交易对。"));
            }
        }
        // The control-plane journal is independent from collector journals.
        // A policy change cannot start a producer through a read subscription.
        self.control
            .replace_policies(tenant_id, policies)
            .map_err(map_state)?;
        Ok(json!({"synced":policies.len(),"last_error":null}))
    }

    pub fn data_policies(&self, tenant_id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        Ok(json!({"items":self.control.policies(tenant_id).map_err(map_state)?}))
    }

    pub fn data_services(&self, tenant_id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let jobs = self.control.data_jobs(tenant_id).map_err(map_state)?;
        let policies = self.control.policies(tenant_id).map_err(map_state)?;
        let active = jobs
            .iter()
            .filter(|j| matches!(j["status"].as_str(), Some("queued" | "running")))
            .count();
        Ok(
            json!({"policies":policies.len(),"jobs":jobs.len(),"pending_jobs":active,
            "producer_status":"unverified","active_collectors":0,
            "warning":"订阅是只读声明；未验证原生采集器 READY 前禁止报告已开始采集。"}),
        )
    }

    pub fn data_job_list(&self, tenant_id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        Ok(json!({"items":self.control.data_jobs(tenant_id).map_err(map_state)?}))
    }

    pub fn data_job(&self, tenant_id: &str, id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        self.control
            .data_job(tenant_id, id)
            .map_err(map_state)?
            .ok_or_else(|| RuntimeError::NotFound("数据任务不存在。".into()))
    }

    pub fn queue_data_job(&self, tenant_id: &str, payload: &Value) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let action = field(payload, "action")?;
        // Never claim support for deletion/trim without a verified snapshot or
        // delegate unsafe file system operations to a generic executor.
        if matches!(action, "repair_futures_day" | "repair_spot_day") {
            let product = field(payload, "product")?;
            let symbol = field(payload, "symbol")?;
            let day = field(payload, "day")?;
            let dataset = repair_archive_dataset(action, product)
                .ok_or_else(|| bad("按日修复任务和 Binance 产品类型不匹配。"))?;
            if check_symbol(symbol).is_err()
                || ArchiveSpec::new(product, dataset, symbol, day).is_err()
            {
                return Err(bad("Binance 按日修复仅接受合法合约与 UTC 日期。"));
            }
            let precision = |key| {
                payload.get(key).and_then(Value::as_u64)
                    .filter(|value| *value <= 16)
                    .ok_or_else(|| bad("必须提供经 Instrument 确认的价格和数量精度。"))
            };
            let normalized = json!({
                "action":action,
                "product":product,
                "symbol":symbol,
                "day":day,
                "price_precision":precision("price_precision")?,
                "size_precision":precision("size_precision")?,
            });
            let id = hash_job(tenant_id, &normalized);
            return self.control.queue_data_job(tenant_id, &id, action, &normalized)
                .map_err(map_state);
        }
        if !matches!(action, "backfill" | "archive_fetch" | "archive_import") {
            return Err(RuntimeError::Conflict(
                "该历史任务尚未完成 Rust 等价迁移。".into(),
            ));
        }
        if action == "archive_import" {
            let download_id = field(payload, "archive_job_id")?;
            if download_id.len() != 32 || !download_id.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(bad("必须提供已经完成的租户 ZIP 下载任务编号。"));
            }
            let download = self.data_job(tenant_id, download_id)?;
            if download["action"] != "archive_fetch"
                || download["status"] != "completed"
                || download["result"]["verified"] != true
            {
                return Err(RuntimeError::Conflict(
                    "必须先在同一租户完成可验证的官方 ZIP 获取任务。".into(),
                ));
            }
            let product = field(&download["payload"], "product")?;
            let dataset = field(&download["payload"], "dataset")?;
            if !matches!(
                (product, dataset),
                ("spot", "trades") | ("futures_um", "aggTrades")
            ) {
                return Err(bad(
                    "不能把不同粒度的历史成交导入实时兼容的 Nautilus Catalog。",
                ));
            }
            // The caller must supply precisions independently confirmed from
            // the Nautilus Instrument. ZIP display width is NOT the precision.
            let price_precision = payload["price_precision"]
                .as_u64()
                .filter(|v| *v <= 16)
                .ok_or_else(|| bad("缺少经 Nautilus Instrument 确认的价格精度。"))?;
            let size_precision = payload["size_precision"]
                .as_u64()
                .filter(|v| *v <= 16)
                .ok_or_else(|| bad("缺少经 Nautilus Instrument 确认的数量精度。"))?;
            let normalized = json!({
                "action": "archive_import",
                "archive_job_id": download_id,
                "price_precision": price_precision,
                "size_precision": size_precision
            });
            let id = hash_job(tenant_id, &normalized);
            return self
                .control
                .queue_data_job(tenant_id, &id, action, &normalized)
                .map_err(map_state);
        }
        if action == "archive_fetch" {
            let product = field(payload, "product")?;
            let dataset = field(payload, "dataset")?;
            let symbol = field(payload, "symbol")?;
            let day = field(payload, "day")?;
            ArchiveSpec::new(product, dataset, symbol, day)
                .map_err(|_| bad("Binance 历史 ZIP 归档参数无效。"))?;
            let normalized = payload.clone();
            let id = hash_job(tenant_id, &normalized);
            return self
                .control
                .queue_data_job(tenant_id, &id, action, &normalized)
                .map_err(map_state);
        }
        let product = field(payload, "product")?;
        let symbol = field(payload, "symbol")?;
        let kind = payload
            .get("data_kind")
            .and_then(Value::as_str)
            .unwrap_or("bars");
        // Only Binance Options Klines are acquired/persisted as historical market data.
        if !historical_backfill_kind_allowed(product, kind)
            || !valid_symbol(product, symbol)
            || (product == "options" && parse_option_symbol(symbol).is_err())
        {
            return Err(bad("历史行情类型不支持：期权只允许 bars K 线。"));
        }
        let start = payload
            .get("start_ms")
            .and_then(Value::as_i64)
            .ok_or_else(|| bad("开始时间无效。"))?;
        let end = payload
            .get("end_ms")
            .and_then(Value::as_i64)
            .ok_or_else(|| bad("结束时间无效。"))?;
        let interval = payload
            .get("interval")
            .and_then(Value::as_str)
            .unwrap_or("1m");
        if start < 0 || start >= end {
            return Err(bad("历史补齐时间范围无效。"));
        }
        if kind == "funding" {
            if end.saturating_sub(start) > 365 * 86_400_000 {
                return Err(bad("单次资金费读取超过一年。"));
            }
        } else {
            let step = interval_ms(interval).map_err(|_| bad("K 线周期无效。"))?;
            if product == "options" && !matches!(interval, "1m" | "5m" | "15m") {
                return Err(bad("原生期权历史只支持 1m/5m/15m。"));
            }
            if start % step != 0 || end % step != 0 || (end - start) / step > 5000 {
                return Err(bad("历史补齐范围须对齐 UTC K 线边界且不超过 5000 根。"));
            }
        }
        // Deterministic identity lets a client retry without duplicating work.
        let mut normalized = payload.clone();
        if let Some(object) = normalized.as_object_mut() {
            object.entry("interval".to_string()).or_insert(json!("1m"));
        }
        let id = hash_job(tenant_id, &normalized);
        self.control
            .queue_data_job(tenant_id, &id, action, &normalized)
            .map_err(map_state)
    }

    pub fn cancel_data_job(&self, tenant_id: &str, id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if self
            .control
            .transition_data_job(tenant_id, id, "queued", "cancelled", None)
            .map_err(map_state)?
        {
            return self.data_job(tenant_id, id);
        }
        let current = self.data_job(tenant_id, id)?;
        if current["status"] == "cancelled" {
            return Ok(current);
        }
        Err(RuntimeError::Conflict(
            "任务正在执行或已结束，不能直接取消。".into(),
        ))
    }

    /// A non-live, bounded backfill executor. It never invokes signed endpoints.
    /// Recovery leaves interrupted tasks visible rather than blindly restarting.
    pub async fn execute_data_job(&self, tenant_id: &str, id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let original = self.data_job(tenant_id, id)?;
        if !self
            .control
            .transition_data_job(tenant_id, id, "queued", "running", None)
            .map_err(map_state)?
        {
            return Err(RuntimeError::Conflict(
                "任务已领取、取消或需要人工核验。".into(),
            ));
        }
        let result = if original["action"] == "archive_fetch" {
            self.archive_fetch_job(tenant_id, &original["payload"])
                .await
        } else if original["action"] == "archive_import" {
            self.archive_import_job(tenant_id, &original["payload"])
                .await
        } else if matches!(original["action"].as_str(), Some("repair_futures_day" | "repair_spot_day")) {
            self.repair_market_day_job(tenant_id, &original["payload"]).await
        } else if original["action"] == "backfill" {
            self.backfill_job(tenant_id, &original["payload"]).await
        } else {
            Err(RuntimeError::Conflict(
                "未知数据任务类型，拒绝执行。".into(),
            ))
        };
        let (status, body) = match result {
            Ok(value) => ("completed", value),
            Err(error) => ("failed", json!({"detail":error.to_string()})),
        };
        self.control
            .transition_data_job(tenant_id, id, "running", status, Some(&body))
            .map_err(map_state)?;
        self.data_job(tenant_id, id)
    }

    /// Explicit offline Binance historical ZIP acquisition under a tenant-owned
    /// pre-existing archive volume. No REST/WebSocket client or trading actions.
    async fn archive_fetch_job(
        &self,
        tenant: &str,
        payload: &Value,
    ) -> Result<Value, RuntimeError> {
        let product = field(payload, "product")?;
        let dataset = field(payload, "dataset")?;
        let symbol = field(payload, "symbol")?;
        let day = field(payload, "day")?;
        ArchiveSpec::new(product, dataset, symbol, day)
            .map_err(|_| bad("Binance 历史归档请求无效。"))?;

        // The server controls the archive root; the request cannot specify a
        // filesystem destination. Each tenant's root must exist already.
        if tenant.is_empty()
            || tenant.len() > 64
            || !tenant.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(bad("租户归档路径标识无效。"));
        }
        let volume = std::env::var("ENGINE_ARCHIVE_ROOT")
            .map_err(|_| bad("必须由管理员配置 ENGINE_ARCHIVE_ROOT。"))?;
        let root = std::path::PathBuf::from(volume)
            .canonicalize()
            .map_err(|_| bad("历史归档根目录未就绪。"))?;
        if !root.is_dir() {
            return Err(bad("历史归档根目录不是现有目录。"));
        }
        let tenant_path = root.join(tenant);
        let metadata = std::fs::symlink_metadata(&tenant_path)
            .map_err(|_| bad("租户归档目录尚未创建，禁止自动新建数据卷。"))?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(bad("历史归档租户目录必须是真实目录，禁止符号链接。"));
        }
        let tenant_root = tenant_path
            .canonicalize()
            .map_err(|_| bad("历史归档租户目录不可访问。"))?;
        if !tenant_root.is_dir() || !tenant_root.starts_with(&root) {
            return Err(bad("历史归档租户隔离校验未通过。"));
        }
        let binary = std::env::var("ENGINE_ARCHIVE_FETCH_BIN")
            .unwrap_or_else(|_| "binance-trade-archive-fetch".into());
        let args = [
            product.to_owned(),
            dataset.to_owned(),
            symbol.to_owned(),
            day.to_owned(),
            tenant_root.to_string_lossy().into_owned(),
        ];
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(binary)
                .args(args)
                .stdin(std::process::Stdio::null())
                .output()
        })
        .await
        .map_err(|_| bad("归档下载任务未返回结果。"))?
        .map_err(|_| bad("归档下载命令无法启动。"))?;
        if !output.status.success() {
            return Err(RuntimeError::Conflict(format!(
                "Binance 官方 ZIP 获取/校验失败：{}",
                String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(300)
                    .collect::<String>(),
            )));
        }
        let mut report: Value = serde_json::from_slice(&output.stdout)
            .map_err(|_| bad("归档下载回执不是有效 JSON。"))?;
        let obj = report
            .as_object_mut()
            .ok_or_else(|| bad("归档下载回执结构无效。"))?;
        if obj.get("verified") != Some(&json!(true))
            || obj.get("catalog_committed") != Some(&json!(false))
            || obj
                .get("sha256")
                .and_then(Value::as_str)
                .is_none_or(|s| s.len() != 64)
        {
            return Err(bad("归档下载回执缺少完整性证据。"));
        }
        obj.insert("action".into(), json!("archive_fetch"));
        obj.insert("data_ready".into(), json!(false));
        obj.insert("allow_new_risk".into(), json!(false));
        obj.insert("exchange_orders".into(), json!(0));
        Ok(report)
    }

    /// Explicit offline import of an already downloaded, verified Binance ZIP
    /// into the upstream Rust Nautilus Catalog, never a custom Tick store.
    ///
    /// The previous completed download job is the provenance authority, not
    /// caller-supplied file paths or checksums. This never declares READY.
    async fn archive_import_job(
        &self,
        tenant: &str,
        payload: &Value,
    ) -> Result<Value, RuntimeError> {
        let download_id = field(payload, "archive_job_id")?;
        let download = self.data_job(tenant, download_id)?;
        if download["action"] != "archive_fetch"
            || download["status"] != "completed"
            || download["result"]["verified"] != true
            || download["result"]["catalog_committed"] != false
            || download["result"]["source"] != "https://data.binance.vision"
        {
            return Err(bad("没有已完成且可验证的同租户 Binance 下载证据。"));
        }
        let request = &download["payload"];
        let product = field(request, "product")?;
        let dataset = field(request, "dataset")?;
        let symbol = field(request, "symbol")?;
        let day = field(request, "day")?;
        if !matches!(
            (product, dataset),
            ("spot", "trades") | ("futures_um", "aggTrades")
        ) {
            return Err(bad(
                "当前 Nautilus Catalog 只接受与实时 TradeTick 同粒度的数据。",
            ));
        }
        let spec = ArchiveSpec::new(product, dataset, symbol, day)
            .map_err(|_| bad("归档来源参数无效。"))?;
        let sha = field(&download["result"], "sha256")?;
        if sha.len() != 64 || !sha.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(bad("已验证下载记录缺少有效的 SHA-256。"));
        }
        let archive_source = spec.remote_path();
        if download["result"]["archive"].as_str() != Some(archive_source.as_str()) {
            return Err(bad("归档来源不匹配，拒绝导入。"));
        }

        let volume = std::env::var("ENGINE_ARCHIVE_ROOT")
            .map_err(|_| bad("未配置管理员管理的历史归档根目录。"))?;
        let root = std::path::PathBuf::from(volume)
            .canonicalize()
            .map_err(|_| bad("历史归档根目录不可用。"))?;
        let tenant_root =
            native_tenant_product_root(&root.to_string_lossy(), tenant, product)?
                .parent()
                .ok_or_else(|| bad("租户归档路径无效。"))?
                .to_path_buf();
        let mut archive = tenant_root.join(product);
        for component in [dataset, symbol] {
            archive.push(component);
            let metadata = std::fs::symlink_metadata(&archive)
                .map_err(|_| bad("历史数据归档子目录不存在。"))?;
            if !metadata.is_dir() || metadata.file_type().is_symlink() {
                return Err(bad("历史数据归档目录不安全。"));
            }
        }
        archive.push(spec.archive_name());
        let meta = std::fs::symlink_metadata(&archive).map_err(|_| bad("历史 ZIP 尚未下载。"))?;
        if !meta.is_file() || meta.file_type().is_symlink() {
            return Err(bad("历史 ZIP 必须是普通文件。"));
        }
        let expected_path = archive.to_string_lossy().to_string();
        if download["result"]["path"].as_str() != Some(expected_path.as_str()) {
            return Err(bad("归档路径与原始下载证明不匹配。"));
        }

        let catalog_base = std::env::var("ENGINE_NATIVE_NAUTILUS_CATALOG_ROOT")
            .map_err(|_| bad("未配置原生 Nautilus Catalog 根目录。"))?;
        let catalog = crate::sources::native_tenant_product_root(&catalog_base, tenant, product)?;
        let executable = std::env::var("ENGINE_NATIVE_NAUTILUS_IMPORT_BIN")
            .map_err(|_| bad("未安装并配置已验证的 Nautilus 历史导入程序。"))?;
        if executable.is_empty() {
            return Err(bad("原生历史导入程序路径为空。"));
        }
        let args = [
            product.to_owned(),
            dataset.to_owned(),
            symbol.to_owned(),
            day.to_owned(),
            archive.to_string_lossy().into_owned(),
            catalog.to_string_lossy().into_owned(),
            sha.to_owned(),
            payload["price_precision"]
                .as_u64()
                .filter(|v| *v <= 16)
                .ok_or_else(|| bad("原生 Instrument 价格精度无效。"))?
                .to_string(),
            payload["size_precision"]
                .as_u64()
                .filter(|v| *v <= 16)
                .ok_or_else(|| bad("原生 Instrument 数量精度无效。"))?
                .to_string(),
        ];
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(executable)
                .args(args)
                .stdin(std::process::Stdio::null())
                .output()
        })
        .await
        .map_err(|_| bad("原生历史导入任务未返回结果。"))?
        .map_err(|_| bad("无法启动 Nautilus 历史导入程序。"))?;
        if !output.status.success() {
            return Err(RuntimeError::Conflict(format!(
                "Nautilus 历史 Tick 导入失败，可能已有局部 Parquet 需核验：{}",
                String::from_utf8_lossy(&output.stderr)
                    .chars()
                    .take(300)
                    .collect::<String>()
            )));
        }
        let mut report: Value = serde_json::from_slice(&output.stdout)
            .map_err(|_| bad("Nautilus 历史导入回执 JSON 无效。"))?;
        if report["catalog_committed"] != true
            || report["data_ready"] != false
            || report["coverage_complete"] != false
            || report["sha256"].as_str() != Some(sha)
            || report["price_precision"] != payload["price_precision"]
            || report["size_precision"] != payload["size_precision"]
            || report["instrument_id"].as_str().is_none()
            || report["records"].as_u64().is_none_or(|count| count == 0)
        {
            return Err(bad("Nautilus 历史导入回执未包含可信的完整性限制。"));
        }
        report["action"] = json!("archive_import");
        report["source_job_id"] = json!(download_id);
        report["allow_new_risk"] = json!(false);
        Ok(report)
    }

    async fn backfill_job(&self, tenant_id: &str, payload: &Value) -> Result<Value, RuntimeError> {
        let product = field(payload, "product")?;
        let symbol = field(payload, "symbol")?;
        let interval = payload
            .get("interval")
            .and_then(Value::as_str)
            .unwrap_or("1m");
        let begin = payload["start_ms"]
            .as_i64()
            .ok_or_else(|| bad("开始时间无效。"))?;
        let end = payload["end_ms"]
            .as_i64()
            .ok_or_else(|| bad("结束时间无效。"))?;
        let kind = payload
            .get("data_kind")
            .and_then(Value::as_str)
            .unwrap_or("bars");
        // Recheck stored/legacy queued jobs before any network request or write.
        if !historical_backfill_kind_allowed(product, kind) {
            return Err(bad("期权历史回填只能获取和落盘 K 线（bars）。"));
        }
        if kind == "funding" {
            let source = FundingSource::new().map_err(|e| RuntimeError::Conflict(e.to_string()))?;
            let events = source
                .history(symbol, begin, end)
                .await
                .map_err(|e| RuntimeError::Conflict(format!("结算资金费读取失败：{e}")))?;
            let rows = events
                .iter()
                .map(|record| {
                    json!({
                        "symbol":record.symbol,"funding_time_ms":record.funding_time_ms,
                        "funding_rate":record.funding_rate,"mark_price":record.mark_price,
                        "rate_type":record.rate_type
                    })
                })
                .collect::<Vec<_>>();
            self.require_active(tenant_id)?;
            let catalog = self.native_catalog()?.ok_or_else(|| RuntimeError::NotFound("未挂载 Pipeline Catalog。".into()))?;
            let observations = events.iter().map(|record| {
                Ok(CatalogFunding {
                    funding_time_ms: record.funding_time_ms,
                    rate_type: record.rate_type.clone(),
                    funding_rate: record.funding_rate.parse::<rust_decimal::Decimal>()
                        .map_err(|_| bad("资金费率无效。"))?,
                    mark_price: record.mark_price.parse::<rust_decimal::Decimal>()
                        .map_err(|_| bad("资金费标记价无效。"))?,
                })
            }).collect::<Result<Vec<_>, RuntimeError>>()?;
            catalog.append_funding(
                &seealpha_market::identity::instrument_id(
                    symbol, seealpha_market::identity::MarketProduct::FuturesUm,
                ).map_err(|e| bad(&e.to_string()))?,
                &observations,
            )
                .map_err(|e| RuntimeError::Conflict(e.to_string()))?;
            return Ok(
                json!({"action":"backfill","data_kind":"funding","symbol":symbol,
                "start_ms":begin,"end_ms":end,"observations":rows.len(),
                "completeness":"event_pages_only","exchange_orders":0}),
            );
        }
        let bars = if product == "options" {
            let source = OptionsSource::new().map_err(|e| RuntimeError::Conflict(e.to_string()))?;
            source.history(symbol, interval, begin, end).await
        } else {
            let market = Product::parse(product).map_err(|_| bad("行情产品无效。"))?;
            let source = BinanceSource::new().map_err(|e| RuntimeError::Conflict(e.to_string()))?;
            source
                .load_history(market, symbol, interval, begin, end)
                .await
        }
        .map_err(|e| RuntimeError::Conflict(format!("历史数据质量或远端读取失败：{e}")))?;
        // A historical response must never be published as a live ready feed.
        let records = bars
            .iter()
            .map(|bar| CatalogBar {
                open_time_ms: bar.open_time_ms,
                open: bar.open.to_string(),
                high: bar.high.to_string(),
                low: bar.low.to_string(),
                close: bar.close.to_string(),
                volume: bar.volume.to_string(),
            })
            .collect::<Vec<_>>();
        // The Python Pipeline determines precision from authoritative
        // Instrument definitions. Never hard-code 8 for native Parquet.
        let policies = self.control.policies(tenant_id).map_err(map_state)?;
        let instrument = policies
            .iter()
            .find(|item| item["product"] == product && item["symbol"] == symbol);
        let select_precision = |key: &str| -> Result<u32, RuntimeError> {
            let requested = payload
                .get(key)
                .and_then(Value::as_u64)
                .or_else(|| instrument.and_then(|p| p.get(key)).and_then(Value::as_u64));
            match requested {
                Some(value) if value <= 16 => Ok(value as u32),
                _ => Err(bad(
                    "原生 Parquet 归档必须使用 Instrument 或明确配置的价格与数量精度。",
                )),
            }
        };
        let price_precision = select_precision("price_precision")?;
        let size_precision = select_precision("size_precision")?;
        let published = self.commit_market_bars(
            tenant_id,
            product,
            symbol,
            interval,
            &records,
            price_precision,
            size_precision,
        )?;
        Ok(
            json!({"action":"backfill","product":product,"symbol":symbol,
            "interval":interval,"start_ms":begin,"end_ms":end,
            "validated_bars":records.len(),"source":"binance_public",
            "publication":published,"exchange_orders":0,"status":"completed"}),
        )
    }


}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_symbols_and_duplicate_policies_are_rejected() {
        assert!(!valid_symbol("spot", "ETH/USDT"));
        assert!(
            valid_policy(&json!({"product":"spot","symbol":"ETHUSDT","base_level":"1m"})).is_ok()
        );
        assert!(
            valid_policy(&json!({"product":"options","symbol":"ALL","base_level":"auto"})).is_ok()
        );
    }
    #[test]
    fn archive_job_inputs_are_validated_before_enqueue() {
        assert!(ArchiveSpec::new("spot", "trades", "BTCUSDT", "2026-11-20",).is_ok());
        assert!(ArchiveSpec::new("futures_um", "aggTrades", "ETHUSDT", "2026-11-20",).is_ok());
        assert!(ArchiveSpec::new("options", "trades", "BTCUSDT", "2026-11-20",).is_err());
        assert!(ArchiveSpec::new("spot", "trades", "../BTCUSDT", "2026-11-20",).is_err());
    }
    #[test]
    fn deterministic_job_id_is_tenant_scoped() {
        let data = json!({"action":"backfill","product":"spot","symbol":"ETHUSDT"});
        assert_eq!(hash_job("one", &data), hash_job("one", &data));
        assert_ne!(hash_job("one", &data), hash_job("two", &data));
    }
}

// The runtime Data Job adapter owns scheduling and process safety, not
// the USD-M data diagnosis or archive-trigger policy.

fn repair_error(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}

impl Runtime {
    /// Explicit user/worker-submitted historical repair job. Never start a
    /// data source implicitly in a read request, or write an active LiveNode
    /// Catalog while that node owns its writer reservation.
    pub(crate) async fn repair_market_day_job(
        &self,
        tenant: &str,
        payload: &Value,
    ) -> Result<Value, RuntimeError> {
        let symbol = payload["symbol"].as_str()
            .ok_or_else(|| repair_error("修复合约缺失。"))?;
        let day = payload["day"].as_str()
            .ok_or_else(|| repair_error("修复 UTC 日期缺失。"))?;
        let action = field(payload, "action")?;
        let product = field(payload, "product")?;
        let dataset = repair_archive_dataset(action, product)
            .ok_or_else(|| repair_error("修复任务与产品不匹配。"))?;
        let spec = ArchiveSpec::new(product, dataset, symbol, day)
            .map_err(|e| repair_error(format!("Binance 修复日期或合约无效：{e}")))?;
        let (start, end) = spec.utc_day_bounds_ms()
            .map_err(|e| repair_error(format!("Binance 修复 UTC 日期无效：{e}")))?;
        if start < 0 || start % DAY_MS != 0 || end - start != DAY_MS {
            return Err(repair_error("修复日期必须按 UTC 天对齐。"));
        }
        let catalog = self.native_catalog()?;
        let root = catalog.as_ref().map(|c| c.root());
        let scan = if product == "spot" {
            spot_repair::scan_day(symbol, start, end, root).await
        } else {
            scan_day(symbol, start, end, root).await
        }.map_err(|e| repair_error(e.to_string()))?;
        let next_step = scan.next_step();

        let mut response = json!({
            "action":action,"product":product,
            "symbol":symbol,"day":day,
            "levels_checked":scan.levels_checked,
            "missing_minute_count":scan.missing_minutes.len(),
            "missing_minutes":scan.missing_minutes,
            "conflicting_minute_count":scan.conflicting_minutes.len(),
            "conflicting_minutes":scan.conflicting_minutes,
            "unresolved_parent_windows":scan.unresolved_parent_windows,
            "data_ready":false,"allow_new_risk":false,
            "needs_recalculation":false,
            "exchange_orders":0
        });

        match next_step {
            RepairNextStep::RecalculateParent => {
                response["result"] = json!("parent_bar_recalculation_required");
                return Ok(response);
            }
            RepairNextStep::Nothing => {
                response["result"] = json!("no_missing_minutes");
                return Ok(response);
            }
            RepairNextStep::FetchOfficialArchive => {}
        }

        // Preserve a durable, tenant-scoped ZIP receipt through the *existing*
        // downloader; never trust user-supplied archive paths or SHA hashes.
        let fetch = self.run_market_repair_child(tenant, &json!({
            "action":"archive_fetch","product":product,
            "dataset":dataset,"symbol":symbol,"day":day
        })).await;
        let fetch = match fetch {
            Ok(fetch) => fetch,
            Err(error) => return Err(error),
        };
        response["archive_fetch_job_id"] = fetch["id"].clone();

        let price = payload["price_precision"].as_u64()
            .filter(|value| *value <= 16)
            .ok_or_else(|| repair_error("Instrument 价格精度缺失。"))?;
        let size = payload["size_precision"].as_u64()
            .filter(|value| *value <= 16)
            .ok_or_else(|| repair_error("Instrument 数量精度缺失。"))?;
        let import = self.queue_data_job(tenant, &json!({
            "action":"archive_import",
            "archive_job_id":fetch["id"],
            "price_precision":price,
            "size_precision":size
        }))?;
        response["archive_import_job_id"] = import["id"].clone();

        let import = self.execute_or_reuse_repair_child(tenant, import).await?;
        response["archive_import_job_id"] = import["id"].clone();
        response["result"] = json!("archive_imported_recalculation_pending");
        response["needs_recalculation"] = json!(true);
        Ok(response)
    }

    /// The nested executor is boxed so a repair job can reuse the existing
    /// archive jobs without duplicating download/import safety code.
    async fn execute_or_reuse_repair_child(
        &self,
        tenant: &str,
        job: Value,
    ) -> Result<Value, RuntimeError> {
        let job = if job["status"] == "queued" {
            let id = job["id"].as_str()
                .ok_or_else(|| repair_error("归档子任务 ID 无效。"))?;
            Box::pin(self.execute_data_job(tenant, id)).await?
        } else {
            job
        };
        if job["status"] != "completed" {
            return Err(repair_error(format!(
                "历史归档子任务尚未成功：{}；任务编号：{}",
                job["status"], job["id"]
            )));
        }
        Ok(job)
    }

    async fn run_market_repair_child(
        &self,
        tenant: &str,
        payload: &Value,
    ) -> Result<Value, RuntimeError> {
        let job = self.queue_data_job(tenant, payload)?;
        self.execute_or_reuse_repair_child(tenant, job).await
    }
}


#[cfg(test)]
mod option_historical_policy_tests {
    use super::historical_backfill_kind_allowed;

    #[test]
    fn options_historical_backfill_accepts_only_klines() {
        assert!(historical_backfill_kind_allowed("options", "bars"));
        for unsupported in ["trades", "aggTrades", "exercise", "funding", "depth", "mark", "greeks", "quotes"] {
            assert!(!historical_backfill_kind_allowed("options", unsupported), "{unsupported}");
        }
        assert!(historical_backfill_kind_allowed("futures_um", "funding"));
        assert!(historical_backfill_kind_allowed("spot", "bars"));
        assert!(!historical_backfill_kind_allowed("spot", "exercise"));
    }
}

#[cfg(test)]
mod repair_action_mapping_tests {
    use super::repair_archive_dataset;

    #[test]
    fn spot_uses_raw_trades_and_usdm_uses_aggregate_trades() {
        assert_eq!(repair_archive_dataset("repair_spot_day", "spot"), Some("trades"));
        assert_eq!(repair_archive_dataset("repair_futures_day", "futures_um"), Some("aggTrades"));
        assert_eq!(repair_archive_dataset("repair_spot_day", "futures_um"), None);
        assert_eq!(repair_archive_dataset("repair_futures_day", "spot"), None);
    }
}
