# Strategy ZIP packages and filesystem storage

The Engine receives self-describing strategy ZIP packages over the existing API. Each ZIP includes a root `manifest.json` containing `format`, `runtime`, `name`, `version`, and `entry_point`; optional `description` is supported for the current native format.

The API transport remains Base64 in JSON as `archive`. `account_id` and `issuance_id` are required trusted issuance scope. The metadata fields `version`, `code_key`, `entry_point`, and `package_hash` are derived from the verified Manifest when omitted and checked if present. Tenant purpose must be authenticated; the ZIP itself does not confer authorization.

The strategy store uses only the local filesystem, never a strategy SQLite database. New ZIPs are extracted into immutable directories:

```text
<ENGINE_STATE>/strategies/<tenant_id>/<account_id>/<issuance_id>/
  manifest.json
  package.zip
  issuance.json
  ...other extracted files
```

The incoming ZIP is size-bounded (1 MiB), with at most 128 entries and 4 MiB total uncompressed. Extraction rejects unsafe paths, symlinks, encryption and duplicate entries. The store checks hashes and refuses conflicting re-uploads under the same issuance ID. The Runtime only authorizes the existing compiled-in native EMA execution path, **not arbitrary uploaded Python or Rust code**.

## Legacy code removal

The former Ledger `strategy_loads` and `native_strategy_loads` schema declarations, structs and read/write APIs have been removed. No automatic migration reads old strategy rows. Previously issued packages must be re-issued via the ZIP API; trading-account Ledger tables are otherwise untouched. Already existing SQLite files may still contain the old tables, but the new code will not use them or delete them automatically.

## Verification required

Run `cargo fmt --all --check`, `cargo check --workspace --locked`, and `cargo test --workspace --locked`, plus API and restart tests. Do not merge before successful verification.
