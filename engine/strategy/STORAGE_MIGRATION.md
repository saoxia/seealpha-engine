# Self-describing strategy packages (transition)

Strategy packages are distributed through the existing authenticated strategy API as Base64-encoded ZIP bytes in the `archive` JSON field. A ZIP must include a root-level `manifest.json`:

```json
{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}
```

The name, version, runtime and entry point belong to the ZIP. API `code_key`, `version`, `entry_point`, `package_hash` and `grant_purpose` may be omitted; when supplied, each is checked against the trusted tenant and verified Manifest. `tenant_id` remains derived from the authenticated API route; `account_id` and `issuance_id` remain mandatory trusted issuance scope. A package Manifest **does not confer authorization**.

This first phase accepts ZIPs only when their manifest identifies the already supported compiled-in `rust-builtin` EMA strategy; it does not enable arbitrary Rust/Python execution. The package hash currently used by the native executor is SHA-256 of the exact manifest bytes; the ZIP itself is also independently hashed to detect changes.

Uploaded ZIPs are bounded (1 MiB ZIP, 4 MiB total uncompressed, 128 entries), checked for duplicate paths, traversal, symlinks and encrypted content, and extracted into a staging directory before publication. Published storage:

```text
<ENGINE_STATE>/
  strategies/
    <tenant_id>/
      <account_id>/
        <issuance_id>/
          manifest.json
          ... (other extracted files)
          package.zip
          issuance.json     # trusted scope, parameters and package hashes
  book.sqlite
```

The issuance directory is immutable; a repeated identical package is idempotent, while a different payload under the same issuance ID is rejected. The existing runtime still verifies authorized scope and exact Manifest bytes before executing its built-in native strategy.

## Legacy and rollback

Startup copies legacy `strategy_loads` records from `book.sqlite` into folders, preserving all original bytes. A legacy ZIP is saved as `legacy-archive.bin` rather than pretending it is a Native Manifest. The source SQLite rows remain unchanged. After a migration has been verified in a staging environment, remove legacy strategy tables and the temporary SQLite import dependency in a separate audited change.

This is a migration-phase implementation. It does not yet implement a dedicated centralized strategy catalog, Python runtime admission, hot swaps, or full lifecycle activation records. Before production use, run Cargo fmt/check/tests, concurrency/restart/fault-injection tests, backup/recovery tests and endpoint integration tests.
