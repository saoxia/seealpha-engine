//! Issued-package preflight inspection in a bounded credential-free Python
//! strategy host, with Rust-owned archive and metadata validation.
use crate::{Runtime, RuntimeError, map_strategy, write_archive};
use seealpha_strategy::{HostCommand, inspect_archive, run_host};
use serde_json::{Value, json};

impl Runtime {
    pub fn inspect_strategy_package(
        &self,
        tenant_id: &str,
        bytes: &[u8],
        parameters: &Value,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant_id)?;
        if !parameters.is_object() {
            return Err(RuntimeError::Unprocessable("策略包参数必须为对象。".into()));
        }
        if bytes.starts_with(&[0x50,0x4b,0x03,0x04]) {
            let (manifest, _) = seealpha_strategy::store::unpack_native_zip(bytes)
                .map_err(crate::map_store)?;
            let metadata = seealpha_strategy::native_manifest::inspect_native_manifest(&manifest)
                .map_err(map_strategy)?;
            let prepared = seealpha_strategy::native_ema::run_native_ema(parameters, "ETHUSDT", &[])
                .map_err(map_strategy)?;
            return Ok(json!({
                "metadata": {"name": metadata.name,"version": metadata.version,
                             "entry_point": metadata.entry_point,"package_hash": metadata.digest},
                "parameters": prepared.prepared,
            }));
        }
        let metadata = inspect_archive(bytes).map_err(map_strategy)?;
        let file = write_archive(bytes)?;
        self.track_start();
        let result = run_host(&HostCommand {
            python: &self.python,
            host: &self.host,
            package_dir: None,
            archive_path: Some(&file),
            parameters,
            symbol: "",
            interval: "1m",
            bars: &[],
            op: "prepare",
        });
        self.track_end();
        let _ = std::fs::remove_file(&file);
        let prepared = result.map_err(map_strategy)?;
        Ok(json!({
            "metadata":{
                "name":metadata.name,
                "version":metadata.version,
                "entry_point":metadata.entry_point,
                "package_hash":metadata.digest,
            },
            "parameters":prepared.prepared,
        }))
    }
}
