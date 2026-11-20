//! Narrow, typed protocol boundary between Rust and the retained Python host.
//! All target amounts are parsed with exact Decimal; never pass through f64.

use crate::{HostCommand, HostResponse, StrategyBar, StrategyError, StrategyRun};
use crate::decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

const SECRET_FIELDS: &[&str] = &[
    "api_key", "secret", "passphrase", "password",
    "token", "credential", "credentials",
];
const MAX_PUBLICATIONS: usize = 20_000;

pub(super) fn check_command(command: &HostCommand<'_>) -> Result<(), StrategyError> {
    if !matches!(command.op, "run" | "prepare")
        || command.package_dir.is_some() == command.archive_path.is_some()
        || command.bars.len() > 5_000
        || command.symbol.len() > 20
        || command.interval.is_empty()
        || command.interval.len() > 16
        || !command.interval.bytes().all(|c| c.is_ascii_alphanumeric())
        || !command.parameters.is_object()
    {
        return Err(invalid("策略宿主输入无效。"));
    }
    check_parameter_values(command.parameters)?;
    if command.op == "prepare" {
        if !command.bars.is_empty() {
            return Err(invalid("策略预登记不能包含行情。"));
        }
    } else if !valid_symbol(command.symbol) {
        return Err(invalid("策略品种无效。"));
    }
    let mut previous = None;
    for bar in command.bars {
        check_bar(bar)?;
        if previous.is_some_and(|time| bar.open_time_ms <= time) {
            return Err(invalid("K 线时间重复或倒退。"));
        }
        previous = Some(bar.open_time_ms);
    }
    Ok(())
}

pub(super) fn check_response(
    command: &HostCommand<'_>,
    response: HostResponse,
) -> Result<StrategyRun, StrategyError> {
    if !response.ok {
        return Err(StrategyError::Failed(if response.error.is_empty() {
            "策略执行失败。".into()
        } else {
            response.error
        }));
    }
    let prepared = response.prepared;
    check_parameter_values(&prepared)?;
    let object = prepared.as_object().ok_or_else(|| invalid("策略参数无效。"))?;
    let symbol = object.get("symbol").and_then(Value::as_str)
        .ok_or_else(|| invalid("策略参数需要 symbol。"))?;
    if !valid_symbol(symbol) {
        return Err(invalid("策略参数需要 symbol。"));
    }
    for field in ["allocation", "risk_limit"] {
        let value = object.get(field).and_then(Value::as_str)
            .ok_or_else(|| invalid("策略参数缺少十进制额度。"))?;
        let amount = Decimal::from_str(value)
            .map_err(|_| invalid("策略金额必须是精确 Decimal。"))?;
        if amount <= Decimal::ZERO || amount.scale() > 8 {
            return Err(invalid("策略金额必须是大于零的精确 Decimal。"));
        }
    }
    if response.publications.len() > MAX_PUBLICATIONS {
        return Err(invalid("策略目标发布数量超过上限。"));
    }
    if command.op == "prepare" {
        if !response.publications.is_empty() {
            return Err(invalid("预登记不能发布目标仓位。"));
        }
    } else {
        if symbol != command.symbol {
            return Err(invalid("策略参数与授权行情品种不一致。"));
        }
        for publication in &response.publications {
            if publication.symbol != command.symbol
                || publication.bar_index >= command.bars.len()
                || !valid_symbol(&publication.symbol)
            {
                return Err(invalid("策略目标发布了未授权的品种或时间索引。"));
            }
            Decimal::from_str(&publication.quantity)
                .map_err(|_| invalid("策略目标数量必须是有限 Decimal。"))?;
        }
    }
    Ok(StrategyRun { prepared, publications: response.publications })
}

fn check_parameter_values(parameters: &Value) -> Result<(), StrategyError> {
    let map = parameters.as_object().ok_or_else(|| invalid("策略参数无效。"))?;
    if map.len() > 128 {
        return Err(invalid("策略参数超过数量限制。"));
    }
    for (key, value) in map {
        if key.is_empty()
            || key.len() > 64
            || SECRET_FIELDS.contains(&key.to_ascii_lowercase().as_str())
            || !(value.as_str().is_some_and(|x| x.len() <= 4096)
                || value.as_i64().is_some()
                || value.as_u64().is_some())
        {
            return Err(invalid("策略参数含密钥或非法值。"));
        }
    }
    Ok(())
}

fn check_bar(bar: &StrategyBar) -> Result<(), StrategyError> {
    if bar.open_time_ms < 0 {
        return Err(invalid("K 线时间无效。"));
    }
    let parse = |value: &str| Decimal::from_str(value).map_err(|_| invalid("K 线十进制数值无效。"));
    let open = parse(&bar.open)?;
    let high = parse(&bar.high)?;
    let low = parse(&bar.low)?;
    let close = parse(&bar.close)?;
    let volume = parse(&bar.volume)?;
    if low <= Decimal::ZERO
        || open <= Decimal::ZERO
        || close <= Decimal::ZERO
        || low > open || low > close
        || high < open || high < close
        || volume < Decimal::ZERO
    {
        return Err(invalid("K 线价格或成交量无效。"));
    }
    Ok(())
}

fn valid_symbol(value: &str) -> bool {
    (1..=20).contains(&value.len())
        && value.bytes().all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

fn invalid(message: &str) -> StrategyError {
    StrategyError::Package(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Publication;
    use serde_json::json;
    use std::path::Path;

    fn command<'a>(params: &'a Value, bars: &'a [StrategyBar]) -> HostCommand<'a> {
        HostCommand {
            python: Path::new("python3"),
            host: Path::new("host.py"),
            package_dir: Some(Path::new(".")),
            archive_path: None,
            parameters: params,
            symbol: "ETHUSDT",
            interval: "1m",
            bars,
            op: "run",
        }
    }

    fn bar() -> StrategyBar {
        StrategyBar {
            open_time_ms: 1,
            open: "2000".into(), high: "2010".into(),
            low: "1990".into(), close: "2001".into(), volume: "10".into(),
        }
    }

    #[test]
    fn host_output_cannot_publish_orders_or_cross_symbol_targets() {
        let params = json!({"symbol":"ETHUSDT","allocation":"1.00000001","risk_limit":"2"});
        let bars = vec![bar()];
        let c = command(&params, &bars);
        check_command(&c).unwrap();
        let good = HostResponse {
            ok: true, error: String::new(), prepared: params.clone(),
            publications: vec![Publication {
                symbol: "ETHUSDT".into(),
                quantity: "0.00000001".into(), bar_index: 0,
            }],
        };
        check_response(&c, good.clone()).unwrap();
        let mut wrong = good.clone();
        wrong.publications[0].symbol = "BTCUSDT".into();
        assert!(check_response(&c, wrong).is_err());
        let mut wrong = good.clone();
        wrong.publications[0].bar_index = 1;
        assert!(check_response(&c, wrong).is_err());
        let mut wrong = good.clone();
        wrong.publications[0].quantity = "NaN".into();
        assert!(check_response(&c, wrong).is_err());
        let mut wrong = good.clone();
        wrong.prepared["api_key"] = json!("secret");
        assert!(check_response(&c, wrong).is_err());
        let mut wrong = good;
        wrong.prepared["risk_limit"] = json!(1.5);
        assert!(check_response(&c, wrong).is_err());
    }

    #[test]
    fn prepared_bars_and_ops_have_strict_boundaries() {
        let params = json!({});
        let bars = vec![bar(), bar()];
        let c = command(&params, &bars);
        assert!(check_command(&c).is_err());
        let bars = vec![bar()];
        let mut c = command(&params, &bars);
        c.op = "submit_order";
        assert!(check_command(&c).is_err());
        c.op = "prepare";
        assert!(check_command(&c).is_err());
        c.op = "run";
        let mut invalid = bar();
        invalid.low = "2011".into();
        assert!(check_command(&command(&params, &[invalid])).is_err());
    }

    #[test]
    fn rejects_unknown_host_response_fields() {
        assert!(serde_json::from_value::<HostResponse>(json!({
            "ok": true,
            "prepared": {"symbol":"ETHUSDT","allocation":"1","risk_limit":"1"},
            "publications": [],
            "orders": [{"symbol":"ETHUSDT"}],
        })).is_err());
    }
}
