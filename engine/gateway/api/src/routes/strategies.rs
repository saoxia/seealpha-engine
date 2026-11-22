//! Strategy issuance, preview and paper sessions.
use super::*;

pub(super) async fn inspect_strategy(
    State(state): State<AppState>,
    Path(tenant_id): Path<String>,
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
            .inspect_strategy_package(&tenant_id, &archive, &params)
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
            .run_paper_preview(PaperRequest {
                tenant_id,
                account_id,
                snapshot,
                strategies,
                request_id: body.request_id,
            })
            .map_err(AppError::from)?,
    ))
}

pub(super) async fn paper_session(
    State(state): State<AppState>,
    Path((tenant_id, account_id)): Path<(String, String)>,
    Json(body): Json<PaperBody>,
) -> Result<Json<Value>, AppError> {
    check_symbol(&body.symbol).map_err(|error| AppError::bad_request(error.to_string()))?;
    let tenant = state
        .runtime
        .require_active(&tenant_id)
        .map_err(AppError::from)?;
    if body.native_sample && !body.use_sample {
        return Err(AppError::bad_request("native_sample 只能与 use_sample 一起使用。"));
    }
    if body.use_sample && !body.native_sample {
        return Err(AppError::bad_request("Python 示例策略已退役；请显式设置 native_sample=true。"));
    }
    let strategies = if body.use_sample {
        if tenant.purpose == "production" {
            return Err(AppError::forbidden("正式租户不能使用示例策略。"));
        }
        vec![StrategyInput {
            member_id: "ema_crossover".into(),
            package: PackageBody::NativeEma,
            parameters: if body.parameters.is_null() {
                json!({})
            } else {
                body.parameters
            },
        }]
    } else {
        state
            .runtime
            .stored_strategies(&tenant_id, &account_id)
            .map_err(AppError::from)?
    };
    let product =
        Product::parse(&body.product).map_err(|error| AppError::bad_request(error.to_string()))?;
    let snapshot = state
        .market
        .load(&LoadQuery {
            product,
            symbol: body.symbol,
            interval: body.interval,
            limit: body.limit,
        })
        .await
        .map_err(|error| AppError::bad_request(error.to_string()))?;
    Ok(Json(
        state
            .runtime
            .run_paper(PaperRequest {
                tenant_id,
                account_id,
                snapshot,
                strategies,
                request_id: body.request_id,
            })
            .map_err(AppError::from)?,
    ))
}
