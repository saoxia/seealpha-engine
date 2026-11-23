//! Strategy issuance and read-only target preview.
use super::*;

pub(super) async fn inspect_strategy(
    State(state): State<AppState>,
    Path(_tenant_id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, AppError> {
    let encoded = body["archive"]
        .as_str()
        .ok_or_else(|| AppError::bad_request("策略包无效。"))?;
    if encoded.len() > 400_000 {
        return Err(AppError::bad_request("策略包超出范围。"));
    }
    let archive = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .map_err(|_| AppError::bad_request("策略包无效。"))?;
    let params = body.get("parameters").cloned().unwrap_or_else(|| json!({}));
    Ok(Json(
        state
            .runtime
            .inspect_strategy_package(&archive, &params)
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn strategy_load(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Json(body): Json<StrategyLoadBody>,
) -> Result<Json<Value>, AppError> {
    let archive = base64::engine::general_purpose::STANDARD
        .decode(body.archive.trim())
        .map_err(|_| AppError::bad_request("策略包无效。"))?;
    Ok(Json(
        state
            .runtime
            .accept_strategy(&StrategySubmission {
                tenant_id: &tenant_id,
                account_id: &body.account_id,
                issuance_id: &body.issuance_id,
                version: &body.version,
                code_key: &body.code_key,
                entry_point: &body.entry_point,
                package_hash: &body.package_hash,
                grant_purpose: &body.grant_purpose,
                parameters: &body.parameters,
                archive: &archive,
            })
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn list_strategies(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
    Query(query): Query<AccountQuery>,
) -> Result<Json<Value>, AppError> {
    Ok(Json(
        state
            .runtime
            .strategy_loads(&tenant_id, query.account_id.as_deref())
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn strategy_preview(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<PreviewBody>,
) -> Result<Json<Value>, AppError> {
    let strategies = state
        .runtime
        .stored_strategies(&tenant_id, &account_id)
        .map_err(AppError::from)?;
    let snapshot = bars_snapshot(&body)?;
    Ok(Json(
        state
            .runtime
            .preview_strategies(PreviewRequest {
                tenant_id,
                account_id,
                snapshot,
                strategies,
                request_id: body.request_id,
            })
            .map_err(AppError::from)?,
    ))
}

