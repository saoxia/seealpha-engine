//! Reconcile real exchange trade history into the ledger. A signed GET can
//! update local evidence; it can never retry a trade POST or infer a fill from
//! an accepted NEW order.
use crate::{Runtime, RuntimeError, map_ledger};
use rust_decimal::Decimal;
use seealpha_venue::lifecycle::VerifiedOrderState;
use seealpha_venue::{Credentials, Product, VenueClient};
use serde_json::{Value, json};

fn error(msg: &str) -> RuntimeError {
    RuntimeError::Conflict(msg.into())
}
fn market_product(stored: &Value, symbol: &str) -> Result<Product, RuntimeError> {
    let product = stored["product"].as_str();
    match product {
        Some("spot") => Ok(Product::Spot),
        Some("futures_um") => Ok(Product::FuturesUm),
        Some("options") => Ok(Product::Option),
        None if symbol.ends_with("-PERP") => Ok(Product::FuturesUm),
        None if seealpha_market::options::parse_option_symbol(symbol).is_ok() => {
            Ok(Product::Option)
        }
        None if seealpha_market::check_symbol(symbol).is_ok() => Ok(Product::Spot),
        _ => Err(error("订单产品标识无法核实，拒绝猜测交易所品种。")),
    }
}

impl Runtime {
    #[allow(clippy::too_many_arguments)]
    pub fn sync_verified_order_fills(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        request_id: &str,
        api_key: &str,
        secret: &str,
        venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        let t = self.require_active(tenant)?;
        if t.purpose != "production" || !matches!(environment, "production" | "sandbox") {
            return Err(error("非正式租户或账户环境无效。"));
        }
        if api_key.is_empty() || secret.is_empty() || api_key == secret {
            return Err(error("交易所凭据无效。"));
        }
        let stored = self
            .ledger
            .get_order_request(tenant, account, request_id)
            .map_err(map_ledger)?
            .ok_or_else(|| RuntimeError::NotFound("没有该订单请求。".into()))?;
        let mut record = serde_json::from_str::<Value>(&stored)
            .map_err(|_| error("订单账本损坏，拒绝推断执行状态。"))?;
        if record["environment"] != environment {
            return Err(error("订单和账户环境不匹配。"));
        }
        let symbol = record["symbol"]
            .as_str()
            .ok_or_else(|| error("订单合约无效。"))?
            .to_owned();
        let client_id = record["response"]["client_order_id"]
            .as_str()
            .ok_or_else(|| error("订单客户端编号缺失。"))?
            .to_owned();
        let product = market_product(&record, &symbol)?;
        if record["order_list"] == "oco" {
            if product != Product::Spot {
                return Err(error("OCO 订单列表与交易产品不匹配。"));
            }
            return self.sync_verified_oco_fills(
                tenant,
                account,
                environment,
                request_id,
                &symbol,
                &client_id,
                record,
                api_key,
                secret,
                venue_base,
            );
        }
        let algo = record["algo_order"] == true;
        let client = VenueClient::new(venue_base).map_err(|_| error("交易网关配置无效。"))?;
        let credentials = Credentials {
            api_key: api_key.to_owned(),
            secret: secret.to_owned(),
        };
        let (order, parent_algo) = if algo {
            if product != Product::FuturesUm {
                return Err(error("只有 USD-M 条件单可以通过 Algo Service 恢复。"));
            }
            let parent = client
                .query_um_conditional(&credentials, &symbol, &client_id)
                .map_err(|_| error("条件单交易所查询失败，保持 unknown，不可重新提交。"))?;
            let actual = parent
                .get("actualOrderId")
                .and_then(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .or_else(|| v.as_u64().map(|n| n.to_string()))
                })
                .filter(|v| !v.is_empty() && v != "0")
                .ok_or_else(|| error("条件单尚未确认触发真实订单，不得虚构成交。"))?;
            let child = client
                .query_um_order_by_id(&credentials, &symbol, &actual)
                .map_err(|_| error("条件单真实子订单查询失败，保持待核实。"))?;
            (child, Some(parent))
        } else {
            let Some(normal) = client
                .query_order(&credentials, product, &symbol, &client_id)
                .map_err(|_| error("交易所订单查询失败，保持原状态且禁止重试委托。"))?
            else {
                return Err(error("交易所尚未确认此订单，保持 unknown 状态并人工核验。"));
            };
            (normal, None)
        };
        let next = VerifiedOrderState::from_exchange(&order)
            .map_err(|_| error("交易所订单状态或累计成交量不可信，拒绝覆盖账本。"))?;
        let previous = if algo {
            &record["triggered_order"]
        } else {
            &record["response"]["order"]
        };
        if !previous.is_null() {
            let prev = VerifiedOrderState::from_exchange(previous)
                .map_err(|_| error("已有订单证据损坏，拒绝覆盖。"))?;
            prev.advance(&next)
                .map_err(|_| error("交易所订单状态回退、终态变化或累计成交量冲突，需人工核验。"))?;
        }
        let order_id = order["order_id"]
            .as_str()
            .ok_or_else(|| error("交易所订单 ID 缺失。"))?;
        let qty = order["filled"]
            .as_str()
            .ok_or_else(|| error("交易所累计成交量缺失。"))?
            .parse::<Decimal>()
            .map_err(|_| error("交易所累计成交量无效。"))?;
        if qty < Decimal::ZERO {
            return Err(error("交易所累计成交量为负。"));
        }
        let trades = client
            .query_order_trades(&credentials, product, &symbol, order_id)
            .map_err(|_| error("真实成交明细查询未完成，拒绝虚构成交。"))?;
        let mut observed = Decimal::ZERO;
        for item in &trades {
            let amount = item["quantity"]
                .as_str()
                .ok_or_else(|| error("成交数量缺失。"))?
                .parse::<Decimal>()
                .map_err(|_| error("成交数量无效。"))?;
            observed = observed
                .checked_add(amount)
                .ok_or_else(|| error("成交数量溢出。"))?;
        }
        if observed > qty {
            return Err(error("成交明细超过交易所订单累计成交量。"));
        }
        self.ledger
            .import_verified_fills(tenant, account, environment, order_id, &trades)
            .map_err(map_ledger)?;
        let effective_client_id = if algo {
            order["client_order_id"]
                .as_str()
                .ok_or_else(|| error("条件单触发子订单缺少客户编号。"))?
        } else {
            &client_id
        };
        self.ledger
            .upsert_order_row(
                tenant,
                account,
                environment,
                effective_client_id,
                &order.to_string(),
            )
            .map_err(map_ledger)?;
        let terminal = matches!(
            order["status"].as_str(),
            Some("FILLED" | "CANCELED" | "EXPIRED" | "EXPIRED_IN_MATCH" | "REJECTED")
        );
        let fills_complete = terminal && observed == qty;
        record["response"]["status"] = json!("completed");
        if algo {
            record["algo_evidence"] = json!(parent_algo);
            record["triggered_order"] = order.clone();
        } else {
            record["response"]["order"] = order.clone();
        }
        record["records"] = json!(trades);
        record["ledger_synced"] = json!(fills_complete);
        record["verified_fill_count"] = json!(trades.len());
        record["verified_filled_quantity"] = json!(observed.to_string());
        record["order_terminal"] = json!(terminal);
        record["fills_complete"] = json!(fills_complete);
        self.ledger
            .save_order_request(tenant, account, request_id, &record.to_string())
            .map_err(map_ledger)?;
        Ok(json!({"request_id":request_id,"client_order_id":client_id,
            "order":order,"parent_algo":parent_algo,"verified_fills":trades,
            "observed_filled_quantity":observed.to_string(),
            "reported_filled_quantity":qty.to_string(),
            "fills_complete":fills_complete,"order_terminal":terminal,
            "ledger_synced":fills_complete,
            "repost_order":false}))
    }
    /// Verify both child orders of a Spot OCO list using exchange-authored
    /// trade history. The two legs can end differently; never infer a fill
    /// from the order list status or the other leg's cancellation.
    #[allow(clippy::too_many_arguments)]
    fn sync_verified_oco_fills(
        &self,
        tenant: &str,
        account: &str,
        environment: &str,
        request_id: &str,
        symbol: &str,
        list_id: &str,
        mut record: Value,
        api_key: &str,
        secret: &str,
        venue_base: &str,
    ) -> Result<Value, RuntimeError> {
        let client =
            VenueClient::new(venue_base).map_err(|_| error("现货订单列表网关配置无效。"))?;
        let credentials = Credentials {
            api_key: api_key.to_owned(),
            secret: secret.to_owned(),
        };
        let list = client
            .query_spot_oco(&credentials, symbol, list_id)
            .map_err(|_| error("OCO 列表证据不完整，保持待核实状态。"))?;
        let entries = list["orders"]
            .as_array()
            .ok_or_else(|| error("OCO 列表缺少子订单。"))?;
        let prior = record["order_children"].as_array();
        let mut children = Vec::new();
        let mut all_terminal = true;
        let mut all_complete = true;
        for entry in entries {
            let client_id = entry["clientOrderId"]
                .as_str()
                .ok_or_else(|| error("OCO 子订单客户端编号缺失。"))?;
            let expected_id = entry["orderId"]
                .as_u64()
                .map(|n| n.to_string())
                .or_else(|| entry["orderId"].as_str().map(str::to_owned))
                .ok_or_else(|| error("OCO 子订单交易所编号缺失。"))?;
            let order = client
                .query_order(&credentials, Product::Spot, symbol, client_id)
                .map_err(|_| error("OCO 子订单查询未确认。"))?
                .ok_or_else(|| error("OCO 子订单未在交易所找到。"))?;
            if order["order_id"].as_str() != Some(expected_id.as_str()) {
                return Err(error("OCO 子订单 ID 与列表证据不一致。"));
            }
            let state = VerifiedOrderState::from_exchange(&order)
                .map_err(|_| error("OCO 子订单状态不可核实。"))?;
            if let Some(old) = prior.and_then(|items| {
                items
                    .iter()
                    .find(|row| row["order"]["client_order_id"].as_str() == Some(client_id))
            }) {
                let previous = VerifiedOrderState::from_exchange(&old["order"])
                    .map_err(|_| error("旧 OCO 子订单证据损坏。"))?;
                previous
                    .advance(&state)
                    .map_err(|_| error("OCO 子订单状态发生不合法回退。"))?;
            }
            let trades = client
                .query_order_trades(&credentials, Product::Spot, symbol, &expected_id)
                .map_err(|_| error("OCO 真实成交历史未完整核实。"))?;
            let mut total = Decimal::ZERO;
            for row in &trades {
                let amount = row["quantity"]
                    .as_str()
                    .ok_or_else(|| error("OCO 成交数量缺失。"))?
                    .parse::<Decimal>()
                    .map_err(|_| error("OCO 成交数量无效。"))?;
                total = total
                    .checked_add(amount)
                    .ok_or_else(|| error("OCO 数量溢出。"))?;
            }
            if total > state.filled_quantity {
                return Err(error("OCO 成交证据超过交易所累计成交量。"));
            }
            all_terminal &= state.phase.terminal();
            all_complete &= total == state.filled_quantity;
            children.push(json!({"order":order,"fills":trades,
                "observed_quantity":total.to_string()}));
        }
        // All remote queries and quantity checks precede any durable writes.
        for child in &children {
            let order = &child["order"];
            let order_id = order["order_id"]
                .as_str()
                .ok_or_else(|| error("OCO 子订单 ID 无效。"))?;
            let child_client = order["client_order_id"]
                .as_str()
                .ok_or_else(|| error("OCO 子订单客户编号无效。"))?;
            let trades = child["fills"]
                .as_array()
                .ok_or_else(|| error("成交证据无效。"))?;
            self.ledger
                .import_verified_fills(tenant, account, environment, order_id, trades)
                .map_err(map_ledger)?;
            self.ledger
                .upsert_order_row(
                    tenant,
                    account,
                    environment,
                    child_client,
                    &order.to_string(),
                )
                .map_err(map_ledger)?;
        }
        let complete = all_terminal && all_complete && list["list_status"] == "ALL_DONE";
        record["response"]["status"] = json!("completed");
        record["response"]["order"] = list.clone();
        record["order_children"] = json!(children);
        record["fills_complete"] = json!(complete);
        record["ledger_synced"] = json!(complete);
        record["order_terminal"] = json!(all_terminal);
        self.ledger
            .save_order_request(tenant, account, request_id, &record.to_string())
            .map_err(map_ledger)?;
        Ok(json!({"request_id":request_id,"client_order_id":list_id,
            "order_list":list,"order_children":children,
            "fills_complete":complete,"ledger_synced":complete,
            "repost_order":false}))
    }
}
