//! Bounded one-shot Python host transport.
//!
//! The Python process is not a security sandbox. Runtime/deployment must use
//! a restricted UID, read-only data, no network/credentials, and OS CPU/memory
//! limits for untrusted strategies. These limits protect the protocol itself.

use crate::{HostResponse, StrategyError};
use serde_json::Value;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const MAX_INPUT_BYTES: usize = 1024 * 1024;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

pub(super) fn invoke(
    python: &Path,
    host: &Path,
    request: &Value,
) -> Result<HostResponse, StrategyError> {
    invoke_with_timeout(python, host, request, DEFAULT_TIMEOUT)
}

fn invoke_with_timeout(
    python: &Path,
    host: &Path,
    request: &Value,
    timeout: Duration,
) -> Result<HostResponse, StrategyError> {
    let payload = serde_json::to_vec(request).map_err(|_| StrategyError::Host)?;
    if payload.len() > MAX_INPUT_BYTES {
        return Err(StrategyError::Package("策略输入超过上限。".into()));
    }
    let mut child = Command::new(python)
        .arg("-I")
        .arg(host)
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("PYTHONNOUSERSITE", "1")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|_| StrategyError::Host)?;

    let Some(mut stdin) = child.stdin.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(StrategyError::Host);
    };
    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(StrategyError::Host);
    };

    let (write_tx, write_rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let ok = stdin.write_all(&payload).is_ok();
        drop(stdin);
        let _ = write_tx.send(ok);
    });

    let exceeded = Arc::new(AtomicBool::new(false));
    let reader_exceeded = exceeded.clone();
    let (read_tx, read_rx) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let read = stdout
            .take((MAX_OUTPUT_BYTES + 1) as u64)
            .read_to_end(&mut output);
        if output.len() > MAX_OUTPUT_BYTES {
            reader_exceeded.store(true, Ordering::Relaxed);
        }
        let _ = read_tx.send(read.map(|_| output));
    });

    let start = Instant::now();
    let status = loop {
        if exceeded.load(Ordering::Relaxed) {
            let _ = child.kill();
            let _ = child.wait();
            return Err(StrategyError::Failed("策略宿主输出超出上限。".into()));
        }
        match child.try_wait() {
            Ok(Some(exit)) => break exit,
            Ok(None) => {}
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(StrategyError::Host);
            }
        }
        if start.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(StrategyError::Timeout);
        }
        std::thread::sleep(Duration::from_millis(5));
    };

    // Grandchildren may keep inherited pipes open. Never wait indefinitely,
    // even when the immediate interpreter has already exited.
    let remaining = timeout.saturating_sub(start.elapsed());
    let wrote = write_rx
        .recv_timeout(remaining)
        .map_err(|_| StrategyError::Timeout)?;
    let remaining = timeout.saturating_sub(start.elapsed());
    let output = read_rx
        .recv_timeout(remaining)
        .map_err(|_| StrategyError::Timeout)?
        .map_err(|_| StrategyError::Host)?;
    if !wrote {
        return Err(StrategyError::Host);
    }
    if output.len() > MAX_OUTPUT_BYTES {
        return Err(StrategyError::Failed("策略宿主输出超出上限。".into()));
    }
    if !status.success() {
        return Err(StrategyError::Failed("策略宿主异常退出。".into()));
    }
    serde_json::from_slice(&output).map_err(|_| StrategyError::Failed("策略宿主输出无效。".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refuses_unbounded_host_input_without_a_process() {
        let massive = serde_json::json!({ "x": "x".repeat(MAX_INPUT_BYTES + 1) });
        let result = invoke_with_timeout(
            Path::new("python3"),
            Path::new("absent.py"),
            &massive,
            Duration::from_millis(10),
        );
        assert!(matches!(result, Err(StrategyError::Package(_))));
    }
}
