//! Native data control and read-side market subscriptions.
//! User/strategy subscriptions are declarations, never collector lifecycle calls.
//! Historical jobs use only public Binance klines and commit atomically to
//! the Rust catalog after the complete requested interval is validated.
use crate::{Runtime, RuntimeError, map_ledger};
use seealpha_binance_market_data_source::archive_fetch::ArchiveSpec;
use seealpha_ledger::CatalogBar;
use seealpha_market::exercise::{ExerciseSource, as_evidence};
use seealpha_market::funding::FundingSource;
use seealpha_market::options::{OptionsSource, parse_option_symbol};
use seealpha_market::{BinanceSource, Product, check_symbol, interval_ms};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

fn bad(message: &str) -> RuntimeError {
    RuntimeError::Unprocessable(message.into())
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
        let previous = self.ledger.policies(tenant_id).map_err(map_ledger)?;
        if previous.as_slice() != policies {
            let mut processes = self
                .source_processes
                .lock()
                .map_err(|_| RuntimeError::Conflict("数据源状态锁不可用。".into()))?;
            for product in ["spot", "futures_um"] {
                let key = format!("{tenant_id}:{product}");
                if let Some(child) = processes.get_mut(&key)
                    && child
                        .try_wait()
                        .map_err(|_| RuntimeError::Conflict("数据源状态未知。".into()))?
                        .is_none()
                {
                    return Err(RuntimeError::Conflict(
                        "原生数据源正在运行，请先停止后再修改持久化采集配置。".into(),
                    ));
                }
            }
        }
        // The control-plane journal is independent from collector journals.
        // A policy change cannot start a producer through a read subscription.
        self.ledger
            .replace_policies(tenant_id, policies)
            .map_err(map_ledger)?;
        Ok(json!({"synced":policies.len(),"last_error":null}))
    }

    pub fn data_policies(&self, tenant_id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        Ok(json!({"items":self.ledger.policies(tenant_id).map_err(map_ledger)?}))
    }

    pub fn data_services(&self, tenant_id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let jobs = self.ledger.data_jobs(tenant_id).map_err(map_ledger)?;
        let policies = self.ledger.policies(tenant_id).map_err(map_ledger)?;
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
        Ok(json!({"items":self.ledger.data_jobs(tenant_id).map_err(map_ledger)?}))
    }

    pub fn data_job(&self, tenant_id: &str, id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        self.ledger
            .data_job(tenant_id, id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::NotFound("数据任务不存在。".into()))
    }

    pub fn queue_data_job(&self, tenant_id: &str, payload: &Value) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let action = field(payload, "action")?;
        // Never claim support for deletion/trim without a verified snapshot or
        // delegate unsafe file system operations to a generic executor.
        if !matches!(action, "backfill" | "publish_catalog" | "archive_fetch") {
            return Err(RuntimeError::Conflict(
                "该历史任务尚未完成 Rust 等价迁移。".into(),
            ));
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
                .ledger
                .queue_data_job(tenant_id, &id, action, &normalized)
                .map_err(map_ledger);
        }
        if action == "publish_catalog" {
            let product = field(payload, "product")?;
            let symbol = field(payload, "symbol")?;
            let interval = field(payload, "interval")?;
            if !matches!(product, "spot" | "futures_um")
                || !valid_symbol(product, symbol)
                || interval_ms(interval).is_err()
            {
                return Err(bad("原生 Catalog 发布参数无效。"));
            }
            for precision in ["price_precision", "size_precision"] {
                if payload[precision].as_u64().is_none_or(|n| n > 16) {
                    return Err(bad("原生发布必须提供来自 Instrument 的价格/数量精度。"));
                }
            }
            let normalized = payload.clone();
            let id = hash_job(tenant_id, &normalized);
            return self
                .ledger
                .queue_data_job(tenant_id, &id, action, &normalized)
                .map_err(map_ledger);
        }
        let product = payload
            .get("product")
            .and_then(Value::as_str)
            .unwrap_or("futures_um");
        let symbol = field(payload, "symbol")?;
        let kind = payload
            .get("data_kind")
            .and_then(Value::as_str)
            .unwrap_or("bars");
        let exercise = kind == "exercise";
        if !matches!(kind, "bars" | "funding" | "exercise")
            || (exercise
                && (product != "options"
                    || !(symbol.ends_with("USDT") && check_symbol(symbol).is_ok())))
            || (!exercise && !valid_symbol(product, symbol))
            || (!exercise && product == "options" && parse_option_symbol(symbol).is_err())
            || (kind == "funding" && product != "futures_um")
        {
            return Err(bad("行情产品、合约或历史数据类型不支持。"));
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
        } else if exercise {
            if end.saturating_sub(start) > 90 * 86_400_000 {
                return Err(bad("单次公开行权事件读取超过 90 天。"));
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
        // Legacy callers omit product for the default USD-M market.
        if let Some(object) = normalized.as_object_mut() {
            object
                .entry("product".to_string())
                .or_insert(json!("futures_um"));
            object.entry("interval".to_string()).or_insert(json!("1m"));
        }
        let id = hash_job(tenant_id, &normalized);
        self.ledger
            .queue_data_job(tenant_id, &id, action, &normalized)
            .map_err(map_ledger)
    }

    pub fn cancel_data_job(&self, tenant_id: &str, id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if self
            .ledger
            .transition_data_job(tenant_id, id, "queued", "cancelled", None)
            .map_err(map_ledger)?
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
            .ledger
            .transition_data_job(tenant_id, id, "queued", "running", None)
            .map_err(map_ledger)?
        {
            return Err(RuntimeError::Conflict(
                "任务已领取、取消或需要人工核验。".into(),
            ));
        }
        let result = if original["action"] == "publish_catalog" {
            self.publish_catalog_job(tenant_id, &original["payload"])
                .await
        } else if original["action"] == "archive_fetch" {
            self.archive_fetch_job(tenant_id, &original["payload"]).await
        } else {
            self.backfill_job(tenant_id, &original["payload"]).await
        };
        let (status, body) = match result {
            Ok(value) => ("completed", value),
            Err(error) => ("failed", json!({"detail":error.to_string()})),
        };
        self.ledger
            .transition_data_job(tenant_id, id, "running", status, Some(&body))
            .map_err(map_ledger)?;
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
        let tenant_root = tenant_path.canonicalize()
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
        let obj = report.as_object_mut()
            .ok_or_else(|| bad("归档下载回执结构无效。"))?;
        if obj.get("verified") != Some(&json!(true))
            || obj.get("catalog_committed") != Some(&json!(false))
            || obj.get("sha256").and_then(Value::as_str).is_none_or(|s| s.len() != 64)
        {
            return Err(bad("归档下载回执缺少完整性证据。"));
        }
        obj.insert("action".into(), json!("archive_fetch"));
        obj.insert("data_ready".into(), json!(false));
        obj.insert("allow_new_risk".into(), json!(false));
        obj.insert("exchange_orders".into(), json!(0));
        Ok(report)
    }

    async fn publish_catalog_job(
        &self,
        tenant: &str,
        payload: &Value,
    ) -> Result<Value, RuntimeError> {
        let product = field(payload, "product")?;
        let symbol = field(payload, "symbol")?;
        let interval = field(payload, "interval")?;
        let base = self
            .ledger
            .policies(tenant)
            .map_err(map_ledger)?
            .into_iter()
            .find(|item| item["product"] == product && item["symbol"] == symbol)
            .and_then(|policy| {
                policy["base_level"]
                    .as_str()
                    .map(|base| if base == "auto" { "1m" } else { base }.to_owned())
            })
            .ok_or_else(|| bad("Catalog 发布必须引用已保存的产品源配置。"))?;
        let source_root = crate::sources::source_root(&self.project, product)?;
        let pair = source_root
            .join(symbol)
            .join(&base)
            .canonicalize()
            .map_err(|_| bad("原始行情数据卷不可用。"))?;
        if !pair.starts_with(&source_root) || !pair.join("collection.sqlite").is_file() {
            return Err(bad("没有已验证的原始成交 journal。"));
        }
        let catalog = self
            .native_catalog()?
            .ok_or_else(|| bad("尚未挂载 Python Nautilus v2 Catalog，禁止隐式创建数据目录。"))?;
        let bin = std::env::var("ENGINE_CATALOG_PUBLISH_BIN")
            .unwrap_or_else(|_| "binance-native-catalog-publish".into());
        let precision = field(payload, "price_precision")
            .ok()
            .map(str::to_owned)
            .or_else(|| payload["price_precision"].as_u64().map(|n| n.to_string()))
            .ok_or_else(|| bad("价格精度无效。"))?;
        let quantity = field(payload, "size_precision")
            .ok()
            .map(str::to_owned)
            .or_else(|| payload["size_precision"].as_u64().map(|n| n.to_string()))
            .ok_or_else(|| bad("数量精度无效。"))?;
        let args = vec![
            product.to_owned(),
            symbol.to_owned(),
            pair.to_string_lossy().to_string(),
            catalog.root().to_string_lossy().to_string(),
            interval.to_owned(),
            precision,
            quantity,
        ];
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(bin)
                .args(args)
                .stdin(std::process::Stdio::null())
                .output()
        })
        .await
        .map_err(|_| bad("Rust Catalog 发布子进程未能返回结果。"))?
        .map_err(|_| bad("Rust Catalog 发布子进程无法启动。"))?;
        if !output.status.success() {
            let details = String::from_utf8_lossy(&output.stderr);
            return Err(bad(&format!(
                "Rust Catalog 发布失败，未推进检查点：{}",
                details.chars().take(300).collect::<String>()
            )));
        }
        serde_json::from_slice::<Value>(&output.stdout)
            .map_err(|_| bad("Rust Catalog 发布记录损坏，拒绝推进任务状态。"))
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
        if kind == "exercise" {
            let source =
                ExerciseSource::new().map_err(|e| RuntimeError::Conflict(e.to_string()))?;
            let events = source
                .history(symbol, begin, end)
                .await
                .map_err(|e| RuntimeError::Conflict(format!("期权公开行权记录获取失败：{e}")))?;
            let rows = events.iter().map(as_evidence).collect::<Vec<_>>();
            self.ledger
                .import_public_exercises(tenant_id, &rows)
                .map_err(map_ledger)?;
            return Ok(json!({"action":"backfill","data_kind":"exercise",
                "product":"options","underlying":symbol,
                "start_ms":begin,"end_ms":end,"observations":rows.len(),
                "account_exercise_confirmed":false,"exchange_orders":0,
                "completeness":"single_public_page_only"}));
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
            self.ledger
                .import_funding_events(tenant_id, symbol, &rows)
                .map_err(map_ledger)?;
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
        let policies = self.ledger.policies(tenant_id).map_err(map_ledger)?;
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
                None if self.market_data_backend()? == "engine_sqlite_compat" => Ok(8),
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

    // Preserve the explicit legacy subscription envelope until v2 parity cutover.
    #[allow(clippy::too_many_arguments)]
    pub fn note_market_subscription(
        &self,
        tenant_id: &str,
        account_id: &str,
        product: &str,
        symbol: &str,
        role: &str,
        state: &str,
        progress: &Value,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if !valid_symbol(product, symbol)
            || role.len() > 48
            || role.is_empty()
            || !matches!(
                state,
                "running" | "stopped" | "error" | "unknown" | "disabled"
            )
        {
            return Err(bad(
                "行情订阅状态无效：未经数据质量证明的 READY 不得由外部上报。",
            ));
        }
        let result = self
            .ledger
            .record_subscription(
                tenant_id, account_id, product, symbol, role, state, progress,
            )
            .map_err(map_ledger)?;
        self.ledger
            .add_event(tenant_id, account_id, "subscription", &result.to_string())
            .map_err(map_ledger)?;
        Ok(result)
    }

    pub fn market_subscriptions(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        Ok(
            json!({"subscriptions":self.ledger.subscriptions(tenant_id,account_id).map_err(map_ledger)?}),
        )
    }

    pub fn subscribe_strategy_market(
        &self,
        tenant_id: &str,
        account_id: &str,
        issuance_id: &str,
        streams: &Value,
        lookback: i64,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if issuance_id.is_empty() || issuance_id.len() > 64 || !(1..=5000).contains(&lookback) {
            return Err(bad("策略行情订阅参数无效。"));
        }
        // HTTP is not the authorization source of truth. The Runtime API
        // must independently verify trusted account-issued strategy identity.
        let purpose = self.require_active(tenant_id)?.purpose;
        let issued = self
            .ledger
            .strategies(tenant_id, account_id)
            .map_err(map_ledger)?;
        if !issued
            .iter()
            .any(|item| item.issuance_id == issuance_id && item.grant_purpose == purpose)
        {
            return Err(RuntimeError::Conflict(
                "策略订阅未绑定有效租户账户发行记录。".into(),
            ));
        }
        let items = streams
            .as_array()
            .ok_or_else(|| bad("策略行情数据流无效。"))?;
        if items.is_empty() || items.len() > 8 {
            return Err(bad("策略行情数据流无效。"));
        }
        let policies = self.ledger.policies(tenant_id).map_err(map_ledger)?;
        let mut unique = BTreeSet::new();
        for stream in items {
            let source = stream
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or("BINANCE");
            let product = field(stream, "product")?;
            let symbol = field(stream, "symbol")?;
            let interval = field(stream, "interval")?;
            let kind = stream
                .get("kind")
                .and_then(Value::as_str)
                .unwrap_or("closed_bars");
            if source != "BINANCE"
                || kind != "closed_bars"
                || interval_ms(interval).is_err()
                || !valid_symbol(product, symbol)
                || product == "options"
            {
                return Err(bad("策略行情数据流不支持。"));
            }
            if !unique.insert((product, symbol, interval)) {
                return Err(bad("策略行情数据流包含重复项。"));
            }
            if !policies.iter().any(|policy| {
                policy["product"] == product
                    && policy["symbol"] == symbol
                    && policy["realtime_enabled"] == true
            }) {
                return Err(RuntimeError::Conflict(
                    "策略订阅未授权或采集策略未启用。".into(),
                ));
            }
        }
        // A read-side subscription never changes policies or starts a worker.
        self.ledger
            .set_strategy_reader(tenant_id, account_id, issuance_id, streams, lookback)
            .map_err(map_ledger)?;
        self.strategy_market_status(tenant_id)
    }

    pub fn unsubscribe_strategy_market(
        &self,
        tenant_id: &str,
        account_id: &str,
        issuance_id: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        self.ledger
            .remove_strategy_reader(tenant_id, account_id, issuance_id)
            .map_err(map_ledger)?;
        self.strategy_market_status(tenant_id)
    }

    pub fn strategy_market_status(&self, tenant_id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        let readers = self
            .ledger
            .strategy_readers(tenant_id)
            .map_err(map_ledger)?;
        let mut required = std::collections::BTreeMap::<String, i64>::new();
        for reader in &readers {
            let lookback = reader["lookback_bars"].as_i64().unwrap_or(1);
            if let Some(streams) = reader["streams"].as_array() {
                for s in streams {
                    let key = format!(
                        "{}:{}:{}",
                        s["product"].as_str().unwrap_or(""),
                        s["symbol"].as_str().unwrap_or(""),
                        s["interval"].as_str().unwrap_or("")
                    );
                    let entry = required.entry(key).or_insert(0);
                    *entry = (*entry).max(lookback);
                }
            }
        }
        Ok(
            json!({"items":readers,"requirements":required,"started_collectors":0,
            "data_ready":false,"allow_new_risk":false,
            "readiness_reason":"Feed/Catalog 完整性和恢复证据尚未由可信协调器确认。"}),
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
        assert!(ArchiveSpec::new(
            "spot", "trades", "BTCUSDT", "2026-11-20",
        ).is_ok());
        assert!(ArchiveSpec::new(
            "futures_um", "aggTrades", "ETHUSDT", "2026-11-20",
        ).is_ok());
        assert!(ArchiveSpec::new(
            "options", "trades", "BTCUSDT", "2026-11-20",
        ).is_err());
        assert!(ArchiveSpec::new(
            "spot", "trades", "../BTCUSDT", "2026-11-20",
        ).is_err());
    }
    #[test]
    fn deterministic_job_id_is_tenant_scoped() {
        let data = json!({"action":"backfill","product":"spot","symbol":"ETHUSDT"});
        assert_eq!(hash_job("one", &data), hash_job("one", &data));
        assert_ne!(hash_job("one", &data), hash_job("two", &data));
    }
}
