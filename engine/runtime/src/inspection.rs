//! Native Rust strategy-package preflight. Never execute uploaded code.
use crate::{Runtime, RuntimeError, map_strategy};
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
        let manifest = if bytes.starts_with(&[0x50, 0x4b, 0x03, 0x04]) {
            seealpha_strategy::store::unpack_native_zip(bytes)
                .map_err(crate::map_store)?.0
        } else {
            bytes.to_vec()
        };
        let metadata = seealpha_strategy::native_manifest::inspect_native_manifest(&manifest)
            .map_err(map_strategy)?;
        let prepared = seealpha_strategy::native_ema::run_native_ema(parameters, "ETHUSDT", &[])
            .map_err(map_strategy)?;
        Ok(json!({
            "metadata": {
                "name": metadata.name,
                "version": metadata.version,
                "entry_point": metadata.entry_point,
                "package_hash": metadata.digest,
            },
            "parameters": prepared.prepared,
        }))
    }
}
