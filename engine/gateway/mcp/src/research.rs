//! Task-scoped Research MCP Streamable HTTP adapter.
//! The API gateway mounts this handler at the existing `/research/mcp` route.
//! It deliberately shares the Research grant and job operations, without a second server.
use super::{
    AppError, AppState, ResearchBacktestRequest, authorize, available_datasets, cancel_job,
    read_job, submit_job,
};
use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use crate::research::Grant;
use serde_json::{Value, json};

pub(super) const MCP_VERSION: &str = "2025-11-25";

fn rpc_result(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn rpc_error(id: &Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn tool_error(error: AppError) -> Value {
    let status = error.into_response().status().as_u16();
    json!({
        "isError": true,
        "content": [{
            "type": "text",
            "text": format!("Research operation rejected (HTTP {status})")
        }]
    })
}

fn tool_success(data: Value) -> Value {
    json!({
        "content": [{"type": "text", "text": data.to_string()}],
        "structuredContent": data
    })
}

fn tool_catalog() -> Vec<(&'static str, &'static str, Value)> {
    vec![
        (
            "list_datasets",
            "datasets:read",
            json!({
                "type":"object","properties":{},"additionalProperties":false
            }),
        ),
        (
            "submit_backtest",
            "backtests:submit",
            json!({
                "type":"object", "properties": {
                    "purpose":{"const":"development"}, "execution":{"const":"simulation"},
                    "strategy_ref":{"type":"string"}, "dataset_refs":{"type":"array"},
                    "start":{"type":"string"}, "end":{"type":"string"},
                    "starting_balance":{"type":"string"}, "quote_currency":{"type":"string"},
                    "parameters":{"type":"object"}, "idempotency_key":{"type":"string"}
                },
                "required":["purpose","execution","strategy_ref","dataset_refs","start",
                    "end","starting_balance","quote_currency","idempotency_key"],
                "additionalProperties":false
            }),
        ),
        (
            "get_backtest",
            "backtests:read",
            json!({
                "type":"object",
                "properties":{"backtest_id":{"type":"string"}},
                "required":["backtest_id"],"additionalProperties":false
            }),
        ),
        (
            "cancel_backtest",
            "backtests:cancel",
            json!({
                "type":"object",
                "properties":{"backtest_id":{"type":"string"}},
                "required":["backtest_id"],"additionalProperties":false
            }),
        ),
    ]
}

fn tool_call(state: &AppState, grant: &Grant, name: &str, args: &Value) -> Result<Value, AppError> {
    let object = args
        .as_object()
        .ok_or_else(|| AppError::unprocessable("Invalid research tool arguments"))?;
    match name {
        "list_datasets" if object.is_empty() => available_datasets(state, grant),
        "submit_backtest" => {
            let request: ResearchBacktestRequest = serde_json::from_value(args.clone())
                .map_err(|_| AppError::unprocessable("Invalid research tool arguments"))?;
            submit_job(state, grant, &request)
        }
        "get_backtest" | "cancel_backtest" if object.len() == 1 => {
            let id = object
                .get("backtest_id")
                .and_then(Value::as_str)
                .ok_or_else(|| AppError::unprocessable("Invalid research backtest identity"))?;
            if name == "get_backtest" {
                read_job(state, grant, id)
            } else {
                cancel_job(state, grant, id)
            }
        }
        _ => Err(AppError::unprocessable("Invalid research tool arguments")),
    }
}

pub(super) async fn handle(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AppError> {
    if headers.contains_key("origin") {
        return Err(AppError::forbidden(
            "Browser origins not allowed on Research MCP",
        ));
    }
    let grant = authorize(&state, &headers, false)?;
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();
    if !content_type.eq_ignore_ascii_case("application/json") {
        return Ok((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(json!({"detail":"MCP requires application/json"})),
        )
            .into_response());
    }
    let accept = headers
        .get(header::ACCEPT)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("*/*");
    if !accept.contains("application/json") && !accept.contains("*/*") {
        return Ok((
            StatusCode::NOT_ACCEPTABLE,
            Json(json!({"detail":"MCP JSON response not accepted"})),
        )
            .into_response());
    }
    if headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|version| version != MCP_VERSION)
    {
        return Err(AppError::bad_request("Unsupported MCP protocol version"));
    }
    if body.len() > 512 * 1024 {
        return Ok((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(json!({"detail":"MCP request too large"})),
        )
            .into_response());
    }
    let message: Value = match serde_json::from_slice(&body) {
        Ok(msg) => msg,
        Err(_) => return Ok(Json(rpc_error(&Value::Null, -32700, "Parse error")).into_response()),
    };
    if !message.is_object() || message["jsonrpc"] != "2.0" {
        return Ok(Json(rpc_error(&Value::Null, -32600, "Invalid Request")).into_response());
    }
    let Some(method) = message["method"].as_str() else {
        return Ok(Json(rpc_error(&Value::Null, -32600, "Invalid Request")).into_response());
    };
    if message.get("id").is_none() {
        if matches!(
            method,
            "notifications/initialized" | "notifications/cancelled"
        ) {
            return Ok(StatusCode::ACCEPTED.into_response());
        }
        return Ok(Json(rpc_error(&Value::Null, -32600, "Invalid Request")).into_response());
    }
    let id = &message["id"];
    if !(id.is_string() || id.as_i64().is_some()) {
        return Ok(Json(rpc_error(&Value::Null, -32600, "Invalid Request")).into_response());
    }
    let default_params = json!({});
    let params = message.get("params").unwrap_or(&default_params);
    if !params.is_object() {
        return Ok(Json(rpc_error(id, -32602, "Invalid params")).into_response());
    }
    let result = match method {
        "initialize" => {
            if !params["protocolVersion"].is_string() {
                rpc_error(id, -32602, "Invalid initialize params")
            } else {
                rpc_result(
                    id,
                    json!({
                        "protocolVersion":MCP_VERSION,
                        "capabilities":{"tools":{"listChanged":false}},
                        "serverInfo":{"name":"seealpha-engine-research","version":"1.0.0"}
                    }),
                )
            }
        }
        "ping" => rpc_result(id, json!({})),
        "tools/list" => {
            if !params["cursor"].is_null() {
                rpc_error(id, -32602, "Unsupported tools cursor")
            } else {
                let tools: Vec<Value> = tool_catalog()
                    .into_iter()
                    .filter(|(_, operation, _)| grant.ops.iter().any(|op| op == operation))
                    .map(|(name, _, schema)| {
                        json!({
                            "name":name,"description":"Task-scoped development research",
                            "inputSchema":schema
                        })
                    })
                    .collect();
                rpc_result(id, json!({"tools":tools}))
            }
        }
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let available = tool_catalog().iter().any(|(tool, operation, _)| {
                *tool == name && grant.ops.iter().any(|op| op == operation)
            });
            if !available {
                rpc_error(id, -32602, "Unknown or unavailable tool")
            } else {
                let args = params.get("arguments").unwrap_or(&default_params);
                if !args.is_object() {
                    rpc_error(id, -32602, "Invalid tool arguments")
                } else {
                    let payload = match tool_call(&state, &grant, name, args) {
                        Ok(value) => tool_success(value),
                        Err(error) => tool_error(error),
                    };
                    rpc_result(id, payload)
                }
            }
        }
        _ => rpc_error(id, -32601, "Method not found"),
    };
    Ok(Json(result).into_response())
}

