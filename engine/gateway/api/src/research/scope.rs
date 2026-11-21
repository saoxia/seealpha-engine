//! Pure authorization checks for research artifacts.
//!
//! HTTP/MCP adapters must authenticate the signed Grant on every request.
//! Runtime owns persistence and the source of trusted task/tenant context.
//! Nothing here issues an authorization or starts a job.

use super::{Grant, ResearchError, require_op};
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_SNAPSHOT_BYTES: usize = 128 * 1024 * 1024;
const MAX_SNAPSHOT_ROWS: u64 = 3_000_000;

#[derive(Debug, Clone, Copy)]
pub struct ResearchContext<'a> {
    /// From the trusted Engine tenant binding, never a request body.
    pub tenant_id: &'a str,
    /// From the trusted task registry, never a request body.
    pub task_id: &'a str,
    pub tenant_purpose: &'a str,
}

/// Check operation *and* the resource owner's pinned tenant/task together.
/// A valid grant does not itself establish that a caller owns a resource.
pub fn authorize_operation(
    grant: &Grant,
    context: ResearchContext<'_>,
    operation: &str,
    dataset: Option<&str>,
    strategy: Option<&str>,
) -> Result<(), ResearchError> {
    if context.tenant_purpose != "development"
        || context.tenant_id != grant.tenant_id
        || context.task_id != grant.task_id
        || !super::identifier(context.tenant_id)
        || !super::identifier(context.task_id)
    {
        return Err(ResearchError::Scope);
    }
    require_op(grant, operation, dataset, strategy)
}

/// Authorize an existing Catalog dataset and a bounded [start, end) UTC
/// window before AI10 creates the durable snapshot task. HTTP date parsing
/// belongs to AI9; call this only with validated epoch nanoseconds.
pub fn authorize_snapshot_request(
    grant: &Grant,
    context: ResearchContext<'_>,
    dataset: &str,
    start_ns: i64,
    end_ns: i64,
) -> Result<(), ResearchError> {
    authorize_operation(grant, context, "snapshots:create", Some(dataset), None)?;
    const THIRTY_ONE_DAYS_NS: i64 = 31 * 24 * 60 * 60 * 1_000_000_000;
    if !super::identifier(dataset)
        || !end_ns
            .checked_sub(start_ns)
            .is_some_and(|length| length > 0 && length <= THIRTY_ONE_DAYS_NS)
    {
        return Err(ResearchError::Scope);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotAccess {
    pub snapshot_id: String,
    pub dataset_id: String,
    pub status: String,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub row_count: Option<u64>,
}

/// Authorize a persisted snapshot *before* returning its metadata.
/// Unknown states and malformed journal rows fail closed, not as `ready`.
pub fn authorize_snapshot(
    grant: &Grant,
    context: ResearchContext<'_>,
    snapshot: &Value,
) -> Result<SnapshotAccess, ResearchError> {
    let row = snapshot.as_object().ok_or(ResearchError::Scope)?;
    let field = |key: &str| {
        row.get(key)
            .and_then(Value::as_str)
            .ok_or(ResearchError::Scope)
    };
    let id = field("snapshot_id")?;
    let tenant = field("tenant_id")?;
    let task = field("task_id")?;
    let dataset = field("dataset_id")?;
    let status = field("status")?;
    if !lower_hex(id, 32)
        || !super::identifier(dataset)
        || field("format")? != "parquet"
        || tenant != context.tenant_id
        || task != context.task_id
        || !matches!(
            status,
            "pending" | "running" | "ready" | "failed" | "interrupted"
        )
    {
        return Err(ResearchError::Scope);
    }
    authorize_operation(grant, context, "snapshots:read", Some(dataset), None)?;
    let number = |key: &str| -> Result<Option<u64>, ResearchError> {
        match row.get(key) {
            Some(Value::Null) | None => Ok(None),
            Some(value) => value.as_u64().map(Some).ok_or(ResearchError::Scope),
        }
    };
    let size_bytes = number("size_bytes")?;
    let row_count = number("row_count")?;
    let sha256 = match row.get("sha256") {
        Some(Value::String(hash)) if lower_hex(hash, 64) => Some(hash.clone()),
        Some(Value::Null) | None => None,
        _ => return Err(ResearchError::Scope),
    };
    if status == "ready" {
        if size_bytes.is_none_or(|n| n == 0 || n > MAX_SNAPSHOT_BYTES as u64)
            || row_count.is_none_or(|n| n > MAX_SNAPSHOT_ROWS)
            || sha256.is_none()
        {
            return Err(ResearchError::Scope);
        }
    } else if size_bytes.is_some() || row_count.is_some() || sha256.is_some() {
        // An unfinished or failed export must never claim completed evidence.
        return Err(ResearchError::Scope);
    }
    Ok(SnapshotAccess {
        snapshot_id: id.to_owned(),
        dataset_id: dataset.to_owned(),
        status: status.to_owned(),
        size_bytes,
        sha256,
        row_count,
    })
}

/// Verify the actual bytes and the journal's recorded size and checksum.
/// The calling transport must still open the owned file without following symlinks.
pub fn verify_snapshot_bytes(evidence: &SnapshotAccess, bytes: &[u8]) -> Result<(), ResearchError> {
    if evidence.status != "ready"
        || bytes.is_empty()
        || bytes.len() > MAX_SNAPSHOT_BYTES
        || evidence.size_bytes != Some(bytes.len() as u64)
        || !bytes.starts_with(b"PAR1")
        || !bytes.ends_with(b"PAR1")
    {
        return Err(ResearchError::Scope);
    }
    let digest = format!("{:x}", Sha256::digest(bytes));
    if evidence.sha256.as_deref() != Some(digest.as_str()) {
        return Err(ResearchError::Scope);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct CandidateEnvelope<'a> {
    pub candidate_id: &'a str,
    pub package_hash: &'a str,
    pub archive: &'a [u8],
    pub dataset_ref: &'a str,
    pub strategy_ref: &'a str,
    pub unrestricted_python: bool,
}

/// Validate the candidate's ownership and immutable envelope, *not* its Python.
/// Call the restricted AST parser (or isolated Python candidate inspector) before
/// dispatch; never execute unverified archive contents from this function alone.
pub fn authorize_candidate_envelope(
    grant: &Grant,
    context: ResearchContext<'_>,
    candidate: CandidateEnvelope<'_>,
) -> Result<(), ResearchError> {
    let operation = if candidate.unrestricted_python {
        "python_candidates:submit"
    } else {
        "candidates:submit"
    };
    authorize_operation(
        grant,
        context,
        operation,
        Some(candidate.dataset_ref),
        Some(candidate.strategy_ref),
    )?;
    let max_bytes = if candidate.unrestricted_python {
        256 * 1024
    } else {
        64 * 1024
    };
    if !lower_hex(candidate.candidate_id, 32)
        || !lower_hex(candidate.package_hash, 64)
        || candidate.archive.is_empty()
        || candidate.archive.len() > max_bytes
        || !valid_dataset(candidate.dataset_ref)
        || !valid_account_ref(candidate.strategy_ref)
    {
        return Err(ResearchError::Scope);
    }
    let digest = format!("{:x}", Sha256::digest(candidate.archive));
    if digest != candidate.package_hash {
        return Err(ResearchError::Scope);
    }
    Ok(())
}

fn lower_hex(input: &str, length: usize) -> bool {
    input.len() == length
        && input
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

fn valid_account_ref(input: &str) -> bool {
    let Some(account) = input.strip_prefix("account:") else {
        return false;
    };
    !account.is_empty() && account.len() <= 40 && super::identifier(account)
}

fn valid_dataset(input: &str) -> bool {
    let mut segments = input.split(':');
    let (Some(product), Some(symbol), Some(interval)) =
        (segments.next(), segments.next(), segments.next())
    else {
        return false;
    };
    segments.next().is_none()
        && matches!(product, "spot" | "futures_um")
        && matches!(interval, "1m" | "5m")
        && (3..=20).contains(&symbol.len())
        && symbol
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::research::Grant;
    use serde_json::json;

    fn grant() -> Grant {
        Grant {
            tenant_id: "tenant-a".into(),
            task_id: "task-1".into(),
            ops: vec![
                "snapshots:read".into(),
                "snapshots:create".into(),
                "candidates:submit".into(),
            ],
            datasets: vec!["spot:ETHUSDT:1m".into()],
            strategies: vec!["account:account1".into()],
            not_before: 100,
            expires_at: 200,
        }
    }

    fn context<'a>(tenant: &'a str, task: &'a str, purpose: &'a str) -> ResearchContext<'a> {
        ResearchContext {
            tenant_id: tenant,
            task_id: task,
            tenant_purpose: purpose,
        }
    }

    #[test]
    fn reject_cross_tenant_cross_task_production_and_ungranted_scope() {
        let g = grant();
        let dev = context("tenant-a", "task-1", "development");
        assert!(
            authorize_operation(&g, dev, "snapshots:read", Some("spot:ETHUSDT:1m"), None).is_ok()
        );
        for c in [
            context("tenant-b", "task-1", "development"),
            context("tenant-a", "task-2", "development"),
            context("tenant-a", "task-1", "production"),
        ] {
            assert_eq!(
                authorize_operation(&g, c, "snapshots:read", None, None),
                Err(ResearchError::Scope)
            );
        }
        assert_eq!(
            authorize_operation(&g, dev, "candidates:submit", None, Some("account:other")),
            Err(ResearchError::Scope)
        );
    }

    #[test]
    fn snapshot_creation_window_is_bounded_and_scope_checked() {
        let g = grant();
        let c = context("tenant-a", "task-1", "development");
        const MAX: i64 = 31 * 24 * 60 * 60 * 1_000_000_000;
        authorize_snapshot_request(&g, c, "spot:ETHUSDT:1m", 0, MAX).unwrap();
        assert_eq!(
            authorize_snapshot_request(&g, c, "spot:ETHUSDT:1m", 0, MAX + 1),
            Err(ResearchError::Scope)
        );
        assert_eq!(
            authorize_snapshot_request(&g, c, "spot:ETHUSDT:1m", MAX, 0),
            Err(ResearchError::Scope)
        );
        assert_eq!(
            authorize_snapshot_request(&g, c, "spot:BTCUSDT:1m", 0, 1),
            Err(ResearchError::Scope)
        );
        assert_eq!(
            authorize_snapshot_request(&g, c, "spot:ETHUSDT:1m", i64::MIN, i64::MAX),
            Err(ResearchError::Scope)
        );
    }

    #[test]
    fn snapshot_is_scoped_and_not_ready_until_evidence_exists() {
        let g = grant();
        let c = context("tenant-a", "task-1", "development");
        let mut m = json!({
            "snapshot_id": "0123456789abcdef0123456789abcdef",
            "tenant_id": "tenant-a", "task_id": "task-1",
            "dataset_id": "spot:ETHUSDT:1m",
            "format": "parquet", "status": "pending",
            "size_bytes": null, "row_count": null, "sha256": null
        });
        let pending = authorize_snapshot(&g, c, &m).unwrap();
        assert_eq!(pending.status, "pending");
        assert_eq!(
            verify_snapshot_bytes(&pending, b"PAR1xPAR1"),
            Err(ResearchError::Scope)
        );
        m["task_id"] = json!("other");
        assert_eq!(authorize_snapshot(&g, c, &m), Err(ResearchError::Scope));
        m["task_id"] = json!("task-1");
        m["status"] = json!("unknown");
        assert_eq!(authorize_snapshot(&g, c, &m), Err(ResearchError::Scope));
        m["status"] = json!("ready");
        assert_eq!(authorize_snapshot(&g, c, &m), Err(ResearchError::Scope));
        let bytes = b"PAR1testPAR1";
        m["size_bytes"] = json!(bytes.len());
        m["row_count"] = json!(2);
        m["sha256"] = json!(format!("{:x}", Sha256::digest(bytes)));
        let ready = authorize_snapshot(&g, c, &m).unwrap();
        verify_snapshot_bytes(&ready, bytes).unwrap();
        assert_eq!(
            verify_snapshot_bytes(&ready, b"PAR1bad!PAR1"),
            Err(ResearchError::Scope)
        );
    }

    #[test]
    fn candidate_envelope_checks_hash_task_account_and_dataset() {
        let g = grant();
        let c = context("tenant-a", "task-1", "development");
        let archive = b"offline-archive";
        let hash = format!("{:x}", Sha256::digest(archive));
        let mut candidate = CandidateEnvelope {
            candidate_id: "0123456789abcdef0123456789abcdef",
            package_hash: &hash,
            archive,
            dataset_ref: "spot:ETHUSDT:1m",
            strategy_ref: "account:account1",
            unrestricted_python: false,
        };
        authorize_candidate_envelope(&g, c, candidate).unwrap();
        candidate.strategy_ref = "account:account2";
        assert_eq!(
            authorize_candidate_envelope(&g, c, candidate),
            Err(ResearchError::Scope)
        );
        candidate.strategy_ref = "account:account1";
        candidate.unrestricted_python = true;
        assert_eq!(
            authorize_candidate_envelope(&g, c, candidate),
            Err(ResearchError::Scope)
        );
        candidate.unrestricted_python = false;
        candidate.dataset_ref = "spot:BTCUSDT:1m";
        assert_eq!(
            authorize_candidate_envelope(&g, c, candidate),
            Err(ResearchError::Scope)
        );
    }
}
