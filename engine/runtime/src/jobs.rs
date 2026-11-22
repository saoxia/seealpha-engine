//! Native durable asynchronous backtest tasks. A task is registered before
//! a strategy host is started; cancelled or interrupted tasks are never
//! executed blindly. The task queue never has exchange credentials.
use crate::{Runtime, RuntimeError, map_control};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn fail(message: &str) -> RuntimeError {
    RuntimeError::Conflict(message.into())
}
fn hash(tenant: &str, text: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"rust-backtest-task-v1:");
    digest.update(tenant.as_bytes());
    digest.update(b":");
    digest.update(text.as_bytes());
    let bytes = digest.finalize();
    let mut out = String::with_capacity(32);
    for byte in bytes.iter().take(16) {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn validate_backtest_window(parameters: &Value) -> Result<(), RuntimeError> {
    let start = parameters["start_ms"]
        .as_i64()
        .ok_or_else(|| fail("回测时间范围无效。"))?;
    let end = parameters["end_ms"]
        .as_i64()
        .ok_or_else(|| fail("回测时间范围无效。"))?;
    if start < 0 || end <= start {
        return Err(fail("回测时间范围无效。"));
    }
    Ok(())
}

impl Runtime {
    pub fn schedule_backtest_job(
        &self,
        tenant: &str,
        parameters: &Value,
        idempotency_key: Option<&str>,
    ) -> Result<Value, RuntimeError> {
        let current = self.require_active(tenant)?;
        if current.purpose != "development" {
            return Err(fail("正式租户不能运行历史回测。"));
        }
        let account = parameters["account_id"]
            .as_str()
            .filter(|value| !value.is_empty() && value.len() <= 40)
            .ok_or_else(|| fail("回测账户编号无效。"))?;
        let _ = account;
        // Validate the interval before queuing persistent work. Previously an
        // inverted/negative interval was accepted and failed only in the
        // asynchronous worker, leaving a misleading queued job behind.
        validate_backtest_window(parameters)?;
        let quote = parameters["quote_currency"].as_str().unwrap_or("");
        if !(3..=8).contains(&quote.len()) || !quote.bytes().all(|v| v.is_ascii_uppercase()) {
            return Err(fail("回测结算币种无效。"));
        }
        let balance = parameters["starting_balance"]
            .as_str()
            .ok_or_else(|| fail("虚拟账户资金无效。"))?
            .parse::<Decimal>()
            .map_err(|_| fail("虚拟账户资金无效。"))?;
        if balance <= Decimal::ZERO {
            return Err(fail("虚拟账户资金无效。"));
        }
        if let Some(key) = idempotency_key
            && (key.is_empty()
                || key.len() > 128
                || !key
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-')))
        {
            return Err(fail("幂等键无效。"));
        }
        let key = idempotency_key
            .map(str::to_owned)
            .unwrap_or_else(|| parameters.to_string());
        let id = hash(tenant, &key);
        let mut params = parameters.clone();
        if let Some(obj) = params.as_object_mut() {
            obj.entry("product".to_string())
                .or_insert(json!("futures_um"));
            obj.entry("interval".to_string()).or_insert(json!("1m"));
        }
        let result = self
            .control
            .queue_backtest_job(tenant, &id, "backtest", &params)
            .map_err(map_control)?;
        Ok(result)
    }

    pub fn execute_backtest_job(&self, tenant: &str, id: &str) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        let stored = self
            .control
            .backtest_job(tenant, id)
            .map_err(map_control)?
            .ok_or_else(|| RuntimeError::NotFound("回测任务不存在。".into()))?;
        if stored["action"] != "backtest" {
            return Err(fail("任务类型不匹配。"));
        }
        if !self
            .control
            .transition_backtest_job(tenant, id, "queued", "running", None)
            .map_err(map_control)?
        {
            return Err(fail("任务已运行、取消或需要人工核验。"));
        }
        let run = self.run_backtest_task_body(tenant, &stored["payload"]);
        let (status, result) = match run {
            Ok(report) => ("completed", report),
            Err(error) => ("failed", json!({"detail":error.to_string()})),
        };
        self.control
            .transition_backtest_job(tenant, id, "running", status, Some(&result))
            .map_err(map_control)?;
        self.control
            .backtest_job(tenant, id)
            .map_err(map_control)?
            .ok_or_else(|| fail("回测任务状态不存在。"))
    }

    fn run_backtest_task_body(&self, tenant: &str, params: &Value) -> Result<Value, RuntimeError> {
        let account = params["account_id"]
            .as_str()
            .ok_or_else(|| fail("回测账户编号无效。"))?;
        let product = params["product"].as_str().unwrap_or("futures_um");
        let interval = params["interval"].as_str().unwrap_or("1m");
        let start = params["start_ms"]
            .as_i64()
            .ok_or_else(|| fail("开始时间无效。"))?;
        let end = params["end_ms"]
            .as_i64()
            .ok_or_else(|| fail("结束时间无效。"))?;
        let starting = params["starting_balance"]
            .as_str()
            .ok_or_else(|| fail("资金无效。"))?
            .parse::<Decimal>()
            .map_err(|_| fail("资金无效。"))?;
        let quote = params["quote_currency"].as_str().unwrap_or("USDT");
        let symbol = if let Some(symbol) = params["symbol"].as_str() {
            symbol.to_string()
        } else {
            let policies = self.control.policies(tenant).map_err(map_control)?;
            let mut candidates = policies
                .iter()
                .filter(|p| p["product"] == product)
                .filter_map(|p| p["symbol"].as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if candidates.len() != 1 {
                return Err(fail("回测未声明品种或无法从唯一已授权产品源推导交易对。"));
            }
            candidates.pop_first().unwrap_or("").to_owned()
        };
        self.run_issued_catalog_backtest(
            tenant, account, product, &symbol, interval, start, end, starting, quote,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invalid_backtest_windows_before_persisting() {
        for payload in [
            json!({}),
            json!({"start_ms": -1, "end_ms": 1000}),
            json!({"start_ms": 2000, "end_ms": 1000}),
            json!({"start_ms": 1000, "end_ms": 1000}),
            json!({"start_ms": 0, "end_ms": "1000"}),
        ] {
            assert!(validate_backtest_window(&payload).is_err(), "{payload}");
        }
        assert!(validate_backtest_window(&json!({"start_ms": 0, "end_ms": 1000})).is_ok());
    }

    #[test]
    fn task_idempotency_is_tenant_scoped_and_stable() {
        assert_eq!(hash("t", "id"), hash("t", "id"));
        assert_ne!(hash("t", "id"), hash("other", "id"));
    }
}
