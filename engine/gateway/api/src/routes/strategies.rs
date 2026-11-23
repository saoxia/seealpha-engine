//! Strategy operations delegate to existing StrategyStore and native executor.
use super::*;
use seealpha_strategy::native_issuance::{inspect_native_package,NativeSubmission};
pub(super) async fn inspect_strategy(State(_state):State<AppState>,Json(body):Json<Value>)->Result<Json<Value>,AppError>{
    let encoded=body["archive"].as_str().ok_or_else(||AppError::bad_request("策略包无效。"))?;
    if encoded.len()>400_000{return Err(AppError::bad_request("策略包超出范围。"));}
    let archive=base64::engine::general_purpose::STANDARD.decode(encoded).map_err(|_|AppError::bad_request("策略包无效。"))?;
    let params=body.get("parameters").cloned().unwrap_or_else(||json!({}));
    if !params.is_object(){return Err(AppError::unprocessable("策略包参数必须为对象。"));}
    let (metadata,prepared)=inspect_native_package(&archive,&params).map_err(|e|AppError::conflict(e.to_string()))?;
    Ok(Json(json!({"metadata":{"name":metadata.name,"version":metadata.version,"entry_point":metadata.entry_point,"package_hash":metadata.digest},"parameters":prepared})))
}
pub(super) async fn strategy_load(State(state):State<AppState>,Json(body):Json<StrategyLoadBody>)->Result<Json<Value>,AppError>{
    let archive=base64::engine::general_purpose::STANDARD.decode(body.archive.trim()).map_err(|_|AppError::bad_request("策略包无效。"))?;
    let accepted=state.strategy_store.issue_native(&NativeSubmission{
        tenant_id:LEGACY_STORAGE_SCOPE,account_id:&body.account_id,issuance_id:&body.issuance_id,
        version:&body.version,code_key:&body.code_key,entry_point:&body.entry_point,
        package_hash:&body.package_hash,parameters:&body.parameters,archive:&archive,
    }).map_err(|e|AppError::conflict(e.to_string()))?;
    Ok(Json(json!({"account_id":body.account_id,"issuance_id":body.issuance_id,
        "code_key":accepted.code_key,"version":accepted.version,"parameters":accepted.parameters})))
}
pub(super) async fn list_strategies(State(state):State<AppState>,Query(query):Query<AccountQuery>)->Result<Json<Value>,AppError>{
    let loads=if let Some(account)=query.account_id{
        state.strategy_store.list(LEGACY_STORAGE_SCOPE,&account).map_err(|e|AppError::conflict(e.to_string()))?
    }else{Vec::new()};
    let items=loads.into_iter().map(|load|json!({"account_id":load.account_id,"issuance_id":load.issuance_id,
        "code_key":load.code_key,"version":load.version,"entry_point":load.entry_point,"package_hash":load.package_hash})).collect::<Vec<_>>();
    Ok(Json(json!({"items":items})))
}
pub(super) async fn strategy_preview(State(state):State<AppState>,Path(account_id):Path<String>,Json(body):Json<PreviewBody>)->Result<Json<Value>,AppError>{
    let snapshot=bars_snapshot(&body)?;
    Ok(Json(crate::strategy_preview::preview_strategies(&state,&account_id,snapshot,body.request_id)?))
}
