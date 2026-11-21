# Native Rust strategy issuance: first admission increment

This feature only validates immutable manifests for an Engine-owned **built-in** strategy. It does **not** accept uploaded Rust binaries, dynamically load libraries, grant live order permissions, or enable production-native strategy execution.

The first allowed manifest is a UTF-8 JSON file, maximum 4096 bytes, without unknown fields:

```json
{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}
```

The manifest `runtime` is **not** an executable path. `native:ema_crossover` identifies compiled Rust code already present in the Engine. The validator hashes the **exact submitted byte sequence** with SHA-256 and checks the hash and metadata against a trusted `IssuedStrategy` record plus authenticated `AccountScope`. Whitespace changes produce a different digest, by design. This is *artifact authentication*, not a claim that JSON alone proves signer identity. Only the trusted issuance/ledger path can grant an account access to an artifact.

The existing `authorize_issued` check remains the authority for: tenant ID, account ID, issuance ID, granted purpose, code key, version, entry point and package hash. Missing or mismatched fields fail closed. The manifest currently allows exactly one built-in Rust strategy, so no user-submitted machine code can be executed by this surface.

## Ledger staging (not an admission endpoint)

A dedicated `native_strategy_loads` table now stores native manifests independently of legacy Python ZIP strategy loads. The `Ledger::insert_native_strategy` operation is write-once per tenant/account/issuance ID, requires an active tenant with matching grant purpose, and rejects replacement even when a caller submits identical bytes. `native_strategies` always queries by tenant **and** account.

This is an internal persistence primitive, **not** a trusted issuer. The Ledger storage function does not independently authenticate a caller or validate manifest SHA-256. Before calling it, a future authenticated admission flow MUST invoke `inspect_native_manifest` and `authorize_native_manifest`, ensure parameters are canonical and validated, and bind issuer identity to the trusted tenant/account. Neither HTTP API nor production native dispatch currently calls this storage function.

## Work still required before production-native strategy execution

1. Connect the separate native Ledger artifact type to a new authenticated HTTP admission flow; authenticate the issuing principal, verify the manifest hash and persist validated immutable manifest plus parameters.
2. On session creation, reload the trusted issuance record and revalidate manifest bytes, digest, account, tenant, grant purpose and parameter identity. Do not accept caller-provided permission fields.
3. Run the compiled strategy only after authorization, then feed its target holdings through the shared risk/netting/ledger path. Keep sample-only `NativeEma` blocked in production.
4. Add end-to-end tests for wrong-account, wrong-tenant, wrong-purpose, mismatched bytes, unknown native strategy, tampered parameters, replay/idempotency, and Python-free execution.

No API endpoint or production permission is enabled by this incremental change.
