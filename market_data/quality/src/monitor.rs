//! Read-only market-data service status snapshots; no process creation or repair.
use std::fs;
use std::path::Path;
use serde_json::{Value, json};

const MAX_STATUS_BYTES: u64 = 128 * 1024;
const MAX_MONITOR_BYTES: u64 = 4 * 1024 * 1024;

/// The caller (native source supervisor) supplies whether the owner process is running.
/// Reading an observation must never start a source, claim a lock, or mutate data.
pub fn observe_status(
    path: &Path,
    now_ms: i64,
    enabled: bool,
    process_running: Option<bool>,
    options_expired: bool,
) -> Value {
    if !enabled {
        return json!({"state":"STOPPED","healthy":false,"backlog":null,"reason":"disabled"});
    }
    let mut item = read_limited_json(path, MAX_STATUS_BYTES)
        .filter(|v| v.is_object() && v.get("valid_until_ms").and_then(Value::as_i64).is_some())
        .unwrap_or_else(|| json!({"state":"DEGRADED","healthy":false,"backlog":null,"reason":"status_unavailable"}));
    if item.get("reason").and_then(Value::as_str) != Some("status_unavailable") {
        let ready = item.get("state").and_then(Value::as_str) == Some("READY");
        let reason = item.get("reason").cloned().filter(|v| !v.is_null() && v != "")
            .or_else(|| item.get("last_error").cloned())
            .unwrap_or(Value::Null);
        item["healthy"] = json!(ready);
        item["reason"] = reason;
        if now_ms > item["valid_until_ms"].as_i64().unwrap_or(i64::MIN) && !options_expired {
            item["state"] = json!("DEGRADED");
            item["healthy"] = json!(false);
            item["reason"] = json!("status_expired");
        }
    }
    if options_expired && item.get("reason").and_then(Value::as_str) == Some("expired") {
        item["state"] = json!("STOPPED");
        item["healthy"] = json!(true);
    } else if process_running == Some(false) {
        item["state"] = json!("DEGRADED");
        item["healthy"] = json!(false);
        item["reason"] = json!("process_not_running");
    }
    item
}

/// Read a shared status snapshot with bounded memory and fail-closed expiry semantics.
pub fn read_monitor(path: &Path, now_ms: i64) -> Value {
    let Some(mut value) = read_limited_json(path, MAX_MONITOR_BYTES) else {
        return json!({"state":"DEGRADED","reason":"monitor_unavailable","items":[]});
    };
    let Some(expiry) = value.get("valid_until_ms").and_then(Value::as_i64) else {
        return json!({"state":"DEGRADED","reason":"monitor_unavailable","items":[]});
    };
    if !value.get("items").is_some_and(Value::is_array) {
        return json!({"state":"DEGRADED","reason":"monitor_unavailable","items":[]});
    }
    if now_ms > expiry {
        value["state"] = json!("DEGRADED");
        value["reason"] = json!("monitor_expired");
        if let Some(items) = value["items"].as_array_mut() {
            for item in items {
                if let Some(services) = item.get_mut("services").and_then(Value::as_object_mut) {
                    for status in services.values_mut() {
                        if status.is_object() {
                            status["healthy"] = json!(false);
                            status["reason"] = json!("monitor_expired");
                        }
                    }
                }
            }
        }
    }
    value
}

fn read_limited_json(path: &Path, limit: u64) -> Option<Value> {
    let metadata = fs::metadata(path).ok()?;
    if metadata.len() > limit || !metadata.is_file() { return None; }
    let content = fs::read(path).ok()?;
    if content.len() as u64 > limit { return None; }
    serde_json::from_slice(&content).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_status_fails_closed() {
        let value = observe_status(Path::new("/nonexistent/seealpha-status.json"), 10, true, None, false);
        assert_eq!(value["state"], "DEGRADED");
        assert_eq!(value["reason"], "status_unavailable");
        let disabled = observe_status(Path::new("/nonexistent/seealpha-status.json"), 10, false, None, false);
        assert_eq!(disabled["state"], "STOPPED");
    }
    #[test]
    fn missing_monitor_fails_closed() {
        let value = read_monitor(Path::new("/nonexistent/seealpha-monitor.json"), 10);
        assert_eq!(value["reason"], "monitor_unavailable");
    }
}
