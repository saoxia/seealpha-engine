//! Explicit opt-in native Binance source supervisor, one child per product.
//! Only read-only public market data; it NEVER marks a producer READY until
//! native REST handover, clock and Catalog validation are complete.
use crate::{Runtime,RuntimeError,map_ledger};
use seealpha_binance_market_data_source::{Product,parse_manifest};
use serde_json::{json,Value};
use std::fs::{self,OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command,Stdio};

fn bad(message:&str)->RuntimeError {RuntimeError::Conflict(message.into())}

fn source_product(name:&str)->Result<Product,RuntimeError>{
    match Product::parse(name) {
        Some(Product::Spot)=>Ok(Product::Spot),
        Some(Product::FuturesUm)=>Ok(Product::FuturesUm),
        _=>Err(bad("期权源尚未实现安全的原生订阅与 Catalog 交接。")),
    }
}
fn explicit_enable()->bool {
    std::env::var("ENGINE_EXPERIMENTAL_SOURCE")
        .is_ok_and(|value|matches!(value.as_str(),"1"|"true"|"TRUE"))
}
fn source_root(project:&std::path::Path,product:&str)->Result<PathBuf,RuntimeError>{
    let root=std::env::var("ENGINE_SOURCE_ROOT").map(PathBuf::from)
        .unwrap_or_else(|_|project.join("data").join("sources"));
    root.join(product).canonicalize().map_err(|_|bad("原生产品源根目录尚未创建，拒绝隐式创建新数据卷。"))
}
fn manifest_bytes(policies:&[Value],product:Product)->Result<Vec<u8>,RuntimeError>{
    let mut entries=Vec::new();
    for policy in policies {
        if policy["product"]!=product.as_str() {continue;}
        let base=policy["base_level"].as_str().unwrap_or("1m");
        entries.push(json!({
            "product":product.as_str(),
            "symbol":policy["symbol"],
            "base_level":if base=="auto" {"1m"} else {base},
            "realtime_enabled":policy["realtime_enabled"].as_bool().unwrap_or(true),
        }));
    }
    if entries.iter().all(|item|item["realtime_enabled"]==false) || entries.is_empty() {
        return Err(bad("尚无启用的原生市场数据源配置。"));
    }
    let bytes=serde_json::to_vec(&json!({"version":1,"policies":entries}))
        .map_err(|_|bad("数据源清单无效。"))?;
    parse_manifest(&bytes,product).map_err(|_|bad("Binance 产品源清单校验未通过。"))?;
    Ok(bytes)
}
fn atomic_manifest(path:&std::path::Path,bytes:&[u8])->Result<(),RuntimeError>{
    let tmp=path.with_extension("json.tmp");
    let result=(||{
        let mut file=OpenOptions::new().create(true).write(true).truncate(true)
            .open(&tmp).map_err(|_|bad("无法写入数据源清单。"))?;
        file.write_all(bytes).map_err(|_|bad("无法写入数据源清单。"))?;
        file.sync_all().map_err(|_|bad("无法同步数据源清单。"))?;
        fs::rename(&tmp,path).map_err(|_|bad("无法原子替换数据源清单。"))?;
        Ok(())
    })();
    if result.is_err() {let _=fs::remove_file(&tmp);}
    result
}

impl Runtime {
    pub fn start_native_source(&self,tenant:&str,product:&str)->Result<Value,RuntimeError>{
        self.require_active(tenant)?;
        if !explicit_enable() {
            return Err(bad("原生数据源尚未通过 READY 验收；需明确启用 ENGINE_EXPERIMENTAL_SOURCE。"));
        }
        let source=source_product(product)?;
        let root=source_root(&self.project,product)?;
        if !root.is_dir(){return Err(bad("数据源根目录不是现有目录。"));}
        let current=self.ledger.policies(tenant).map_err(map_ledger)?;
        let manifest=manifest_bytes(&current,source)?;
        let key=format!("{tenant}:{product}");
        let mut processes=self.source_processes.lock()
            .map_err(|_|bad("数据源进程状态锁不可用。"))?;
        let running=match processes.get_mut(&key) {
            Some(child)=>child.try_wait().map_err(|_|bad("数据源进程状态未知。"))?.is_none(),
            None=>false,
        };
        if running {return Err(bad("该产品数据源已经运行，禁止重复启动。"));}
        processes.remove(&key);
        let safe_tenant:String=tenant.chars().filter(|c|c.is_ascii_alphanumeric()).collect();
        if safe_tenant!=tenant || tenant.is_empty() || tenant.len()>64 {
            return Err(bad("租户编号无效。"));
        }
        let manifest_path=self.source_state_dir.join(format!("source-{safe_tenant}-{product}.json"));
        atomic_manifest(&manifest_path,&manifest)?;
        let binary=std::env::var("ENGINE_SOURCE_BIN")
            .unwrap_or_else(|_|"binance-native-source".to_string());
        let child=Command::new(binary)
            .arg(product).arg(&manifest_path).arg(&root)
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null())
            .spawn().map_err(|_|bad("无法启动独立的 Rust 市场数据源进程。"))?;
        let pid=child.id();
        processes.insert(key,child);
        Ok(json!({"tenant_id":tenant,"product":product,"pid":pid,
            "status":"experimental_degraded","data_ready":false,
            "exchange_orders":0,"root":root,"manifest":manifest_path,
            "message":"源仍处于 DEGRADED，未完成历史衔接与数据质量验收。"}))
    }

    pub fn stop_native_source(&self,tenant:&str,product:&str)->Result<Value,RuntimeError>{
        self.require_active(tenant)?;
        source_product(product)?;
        let key=format!("{tenant}:{product}");
        let mut processes=self.source_processes.lock()
            .map_err(|_|bad("数据源进程状态锁不可用。"))?;
        let mut child=processes.remove(&key)
            .ok_or_else(||bad("没有本进程管理的对应原生数据源。"))?;
        child.kill().map_err(|_|bad("无法终止数据源进程，需检查其写入锁。"))?;
        child.wait().map_err(|_|bad("数据源停止结果待核验。"))?;
        Ok(json!({"tenant_id":tenant,"product":product,
            "status":"stopped","data_ready":false}))
    }

    pub fn native_source_status(&self,tenant:&str,product:&str)->Result<Value,RuntimeError>{
        self.require_active(tenant)?;
        source_product(product)?;
        let key=format!("{tenant}:{product}");
        let mut processes=self.source_processes.lock()
            .map_err(|_|bad("数据源进程状态锁不可用。"))?;
        let (running,pid)=if let Some(child)=processes.get_mut(&key) {
            let active=child.try_wait().map_err(|_|bad("数据源进程状态未知。"))?.is_none();
            (active,Some(child.id()))
        } else {(false,None)};
        let product_root=source_root(&self.project,product).ok();
        let mut symbols=Vec::new();
        if let Some(root)=product_root {
            for policy in self.ledger.policies(tenant).map_err(map_ledger)? {
                if policy["product"]!=product {continue;}
                let symbol=policy["symbol"].as_str().unwrap_or("");
                let base=policy["base_level"].as_str().unwrap_or("1m");
                let path=root.join(symbol).join(if base=="auto" {"1m"} else {base}).join("status.json");
                let evidence=fs::read(path).ok()
                    .and_then(|raw|serde_json::from_slice::<Value>(&raw).ok());
                symbols.push(json!({"symbol":symbol,"evidence":evidence}));
            }
        }
        // Process/socket connectivity is never a substitute for exchange
        // confirmed readiness; nothing here allows new directional risk.
        Ok(json!({"tenant_id":tenant,"product":product,"pid":pid,
            "running":running,"data_ready":false,"allow_new_risk":false,
            "symbols":symbols,"status":if running {"degraded"} else {"stopped"} }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_manifest_rejects_invalid_policy_before_starting_any_process(){
        let policies=vec![json!({"product":"spot","symbol":"ETHUSDT","base_level":"1m","realtime_enabled":true})];
        let raw=manifest_bytes(&policies,Product::Spot).unwrap();
        assert_eq!(parse_manifest(&raw,Product::Spot).unwrap().len(),1);
        let invalid=vec![json!({"product":"options","symbol":"ALL","base_level":"1m","realtime_enabled":true})];
        assert!(manifest_bytes(&invalid,Product::Options).is_err());
    }
}
