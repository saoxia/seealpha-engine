//! Built-in Rust strategy inspection, issuance, and authenticated execution.
//! The caller supplies a trusted tenant/account scope; no package grants trading permission.
use crate::executor::{ExecutionInput, IssuedNativeEmaExecutor, StrategyExecutor};
use crate::native_ema::run_native_ema;
use crate::native_manifest::{authorize_native_manifest, inspect_native_manifest};
use crate::store::{StoreError, StrategyLoad, StrategyStore, unpack_native_zip};
use crate::{AccountScope, IssuedStrategy, PackageMetadata, StrategyError, StrategyRun};
use serde_json::Value;

#[derive(Clone, Copy)]
pub struct NativeSubmission<'a> {
    pub tenant_id: &'a str,
    pub account_id: &'a str,
    pub issuance_id: &'a str,
    pub version: &'a str,
    pub code_key: &'a str,
    pub entry_point: &'a str,
    pub package_hash: &'a str,
    pub grant_purpose: &'a str,
    pub parameters: &'a Value,
    pub archive: &'a [u8],
}

pub struct NativeAccepted {
    pub code_key: String,
    pub version: String,
    pub parameters: Value,
}

#[derive(Debug, Clone)]
pub struct IssuedNativeInput {
    pub member_id: String,
    pub archive: Vec<u8>,
    pub parameters: Value,
}

struct PreparedNative {
    metadata: PackageMetadata,
    parameters: Value,
    manifest: Vec<u8>,
    is_zip: bool,
}

fn store_error(error: StoreError) -> StrategyError {
    StrategyError::Package(error.to_string())
}

/// Shared preflight for inspection and issuance. Only compiled-in Rust
/// strategies are supported; uploaded bytes are treated as inert manifests.
fn prepare_native(archive: &[u8], parameters: &Value) -> Result<PreparedNative, StrategyError> {
    let is_zip = archive.starts_with(b"PK\x03\x04");
    let manifest = if is_zip {
        unpack_native_zip(archive).map_err(store_error)?.0
    } else {
        archive.to_vec()
    };
    let metadata = inspect_native_manifest(&manifest)?;
    let prepared = run_native_ema(parameters, "ETHUSDT", &[])?.prepared;
    Ok(PreparedNative { metadata, parameters: prepared, manifest, is_zip })
}

pub fn inspect_native_package(
    archive: &[u8],
    parameters: &Value,
) -> Result<(PackageMetadata, Value), StrategyError> {
    let prepared = prepare_native(archive, parameters)?;
    Ok((prepared.metadata, prepared.parameters))
}

impl StrategyStore {
    /// Validate immutable issuance identity, prepare parameters and persist it.
    /// The caller must supply a trusted tenant scope and purpose.
    pub fn issue_native(
        &self,
        submission: &NativeSubmission<'_>,
        tenant_purpose: &str,
    ) -> Result<NativeAccepted, StrategyError> {
        let grant_purpose = if submission.grant_purpose.is_empty() {
            tenant_purpose
        } else {
            submission.grant_purpose
        };
        if grant_purpose != tenant_purpose {
            return Err(StrategyError::Package(if tenant_purpose == "production" {
                "正式租户只能加载正式授权。".into()
            } else {
                "下发用途与租户类型不一致。".into()
            }));
        }
        if submission.account_id.is_empty()
            || submission.issuance_id.is_empty()
            || submission.account_id.len() > 40
            || submission.issuance_id.len() > 64
            || submission.version.len() > 40
        {
            return Err(StrategyError::Package("策略下发缺少账户、编号或版本。".into()));
        }

        let prepared = prepare_native(submission.archive, submission.parameters)?;
        let metadata = &prepared.metadata;
        let code_key = if submission.code_key.is_empty() { metadata.name.as_str() } else { submission.code_key };
        let version = if submission.version.is_empty() { metadata.version.as_str() } else { submission.version };
        let entry_point = if submission.entry_point.is_empty() { metadata.entry_point.as_str() } else { submission.entry_point };
        let package_hash = if submission.package_hash.is_empty() { metadata.digest.as_str() } else { submission.package_hash };
        if version.len() > 40
            || metadata.digest != package_hash
            || metadata.name != code_key
            || metadata.version != version
            || metadata.entry_point != entry_point
        {
            return Err(StrategyError::Package("策略包与下发记录不一致。".into()));
        }
        authorize_native_manifest(
            &prepared.manifest,
            IssuedStrategy {
                tenant_id: submission.tenant_id,
                account_id: submission.account_id,
                issuance_id: submission.issuance_id,
                grant_purpose,
                code_key,
                version,
                entry_point,
                package_hash,
            },
            AccountScope {
                tenant_id: submission.tenant_id,
                account_id: submission.account_id,
                purpose: tenant_purpose,
            },
        )?;
        let load = StrategyLoad {
            tenant_id: submission.tenant_id.to_string(),
            account_id: submission.account_id.to_string(),
            issuance_id: submission.issuance_id.to_string(),
            code_key: code_key.to_string(),
            version: version.to_string(),
            grant_purpose: grant_purpose.to_string(),
            parameters_json: prepared.parameters.to_string(),
            entry_point: entry_point.to_string(),
            package_hash: package_hash.to_string(),
            archive: prepared.manifest,
        };
        if prepared.is_zip {
            self.insert_zip(&load, submission.archive).map_err(store_error)?;
        } else {
            self.insert(&load).map_err(store_error)?;
        }
        Ok(NativeAccepted {
            code_key: code_key.to_string(),
            version: version.to_string(),
            parameters: prepared.parameters,
        })
    }

    /// Load only immutable, account-issued native strategies.
    pub fn issued_native_inputs(
        &self,
        tenant_id: &str,
        account_id: &str,
    ) -> Result<Vec<IssuedNativeInput>, StrategyError> {
        let loads = self.strategies(tenant_id, account_id).map_err(store_error)?;
        if loads.is_empty() {
            return Err(StrategyError::Package("该租户账户没有已下发的策略。".into()));
        }
        loads.into_iter().map(|load| {
            let parameters = serde_json::from_str(&load.parameters_json)
                .map_err(|_| StrategyError::Package(
                    "已下发策略参数记录损坏，禁止用默认参数执行。".into()
                ))?;
            Ok(IssuedNativeInput {
                member_id: load.issuance_id,
                archive: load.archive,
                parameters,
            })
        }).collect()
    }
}

/// Bind a requested preview to its trusted persisted issuance before executing.
/// Runtime supplies the configured tenant/account scope and market input.
pub fn execute_issued_native(
    load: &StrategyLoad,
    scope: AccountScope<'_>,
    manifest: &[u8],
    input: ExecutionInput<'_>,
) -> Result<StrategyRun, StrategyError> {
    let stored_parameters: Value = serde_json::from_str(&load.parameters_json)
        .map_err(|_| StrategyError::Package("已下发策略参数记录损坏。".into()))?;
    if load.archive.as_slice() != manifest || &stored_parameters != input.parameters {
        return Err(StrategyError::Package(
            "预览策略包或参数与可信下发记录不一致。".into()
        ));
    }
    IssuedNativeEmaExecutor {
        manifest,
        issued: IssuedStrategy {
            tenant_id: &load.tenant_id,
            account_id: &load.account_id,
            issuance_id: &load.issuance_id,
            grant_purpose: &load.grant_purpose,
            code_key: &load.code_key,
            version: &load.version,
            entry_point: &load.entry_point,
            package_hash: &load.package_hash,
        },
        scope,
    }.execute(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    const MANIFEST: &[u8] = br#"{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}"#;

    #[test]
    fn raw_and_zip_native_preflight_agree() {
        let params = json!({"fast_period": 2, "slow_period": 3});
        let (raw, prepared) = inspect_native_package(MANIFEST, &params).unwrap();
        assert_eq!(raw.digest, crate::sha256_hex(MANIFEST));
        assert_eq!(prepared["fast_period"], 2);

        let buffer = std::io::Cursor::new(Vec::new());
        let mut writer = zip::ZipWriter::new(buffer);
        writer.start_file("manifest.json", zip::write::SimpleFileOptions::default()).unwrap();
        writer.write_all(MANIFEST).unwrap();
        let zip = writer.finish().unwrap().into_inner();
        let (zipped, zip_params) = inspect_native_package(&zip, &params).unwrap();
        assert_eq!(raw, zipped);
        assert_eq!(prepared, zip_params);
        assert!(inspect_native_package(b"{}", &params).is_err());
    }

    #[test]
    fn preview_fails_closed_on_changed_package_parameters_or_scope() {
        let digest = crate::sha256_hex(MANIFEST);
        let load = StrategyLoad {
            tenant_id: "tenant".into(), account_id: "account".into(),
            issuance_id: "issue".into(), code_key: "ema_crossover".into(),
            version: "1".into(), grant_purpose: "development".into(),
            parameters_json: "{}".into(),
            entry_point: "native:ema_crossover".into(), package_hash: digest,
            archive: MANIFEST.to_vec(),
        };
        let scope = AccountScope {
            tenant_id: "tenant", account_id: "account", purpose: "development",
        };
        let params = json!({});
        let input = ExecutionInput {
            parameters: &params, symbol: "ETHUSDT", interval: "1m", bars: &[],
        };
        assert!(execute_issued_native(&load, scope, MANIFEST, input).is_ok());
        assert!(execute_issued_native(&load, scope, b"{}", input).is_err());
        let changed = json!({"allocation": "2"});
        assert!(execute_issued_native(&load, scope, MANIFEST, ExecutionInput {
            parameters: &changed, ..input
        }).is_err());
        assert!(execute_issued_native(&load, AccountScope {
            account_id: "other", ..scope
        }, MANIFEST, input).is_err());
    }
}
