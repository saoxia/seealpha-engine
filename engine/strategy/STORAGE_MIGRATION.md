# Strategy ZIP packages and filesystem storage

The Engine receives self-describing strategy ZIP packages over the existing API. Each ZIP includes a root `manifest.json` containing `format`, `runtime`, `name`, `version`, and `entry_point`; optional `description` is supported for the current native format.

The API transport remains Base64 in JSON as `archive`. `account_id` and `issuance_id` are required trusted issuance scope. The metadata fields `version`, `code_key`, `entry_point`, and `package_hash` are derived from the verified Manifest when omitted and checked if present. The caller's account identity must be trusted; the ZIP itself does not confer authorization.

Strategy packages are stored as immutable filesystem directories; **metadata is indexed in PostgreSQL**, not in the trading Ledger. PostgreSQL holds account/issuance, name, version, parameters, entry point, manifest hash and ZIP hash, but **no ZIP or executable BLOB**. The list API queries PostgreSQL only; Runtime loads and revalidates files for execution. The index is rebuilt from validated issuance directories at startup so an interrupted publish or missing index can be recovered.

New ZIPs are extracted into immutable directories:

```text
<ENGINE_STATE>/strategies/<account_id>/<issuance_id>/
  manifest.json
  package.zip
  issuance.json
  ...other extracted files
```

The incoming ZIP is size-bounded (1 MiB), with at most 128 entries and 4 MiB total uncompressed. Extraction rejects unsafe paths, symlinks, encryption and duplicate entries. The store checks hashes and refuses conflicting re-uploads under the same issuance ID. The Runtime only authorizes the existing compiled-in native EMA execution path, **not arbitrary uploaded Python or Rust code**.

## Verification required

Run `cargo fmt --all --check`, `cargo check --workspace --locked`, and `cargo test --workspace --locked`, plus API and restart tests. Do not merge before successful verification.

