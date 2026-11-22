//! Native-only fill synchronization boundary.
//! Nautilus order acknowledgments and unverified FillReports must never be
//! written as exchange-authenticated trade evidence.
use crate::{Runtime, RuntimeError};
use serde_json::Value;

impl Runtime {
    pub fn sync_verified_order_fills(
        &self,
        tenant: &str,
        _account: &str,
        _environment: &str,
        _request_id: &str,
    ) -> Result<Value, RuntimeError> {
        self.require_active(tenant)?;
        Err(RuntimeError::Conflict(
            "Nautilus 原生成交报告与交易所真实 Trade ID 核验尚未接入，禁止将未验证报告记账。".into()
        ))
    }
}
