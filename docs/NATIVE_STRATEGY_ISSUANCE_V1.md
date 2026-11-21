# Native Rust strategy issuance: first admission increment

This feature only validates immutable manifests for an Engine-owned **built-in** strategy. It does **not** accept uploaded Rust binaries, dynamically load libraries, grant live order permissions, or enable production-native strategy execution.

The first allowed manifest is a UTF-8 JSON file, maximum 4096 bytes, without unknown fields:

```json
{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}
```

The manifest `runtime` is **not** an executable path. `native:ema_crossover` identifies compiled Rust code already present in the Engine. The validator hashes the **exact submitted byte sequence** with SHA-256 and checks the hash and metadata against a trusted `IssuedStrategy` record plus authenticated `AccountScope`. Whitespace changes produce a different digest, by design. This is *artifact authentication*, not a claim that JSON alone proves signer identity. Only the trusted issuance/ledger path can grant an account access to an artifact.

The existing `authorize_issued` check remains the authority for: tenant ID, account ID, issuance ID, granted purpose, code key, version, entry point and package hash. Missing or mismatched fields fail closed. The manifest currently allows exactly one built-in Rust strategy, so no user-submitted machine code can be executed by this surface.

## Work still required before production-native strategy execution

1. Define a Rust-native artifact type in the Ledger and HTTP issuance protocol, distinct from ZIP/Python packages; authenticate the issuing principal and persist the immutable manifest plus parameters.
2. On session creation, reload the trusted issuance record and revalidate manifest bytes, digest, account, tenant, grant purpose and parameter identity. Do not accept caller-provided permission fields.
3. Run the compiled strategy only after authorization, then feed its target holdings through the shared risk/netting/ledger path. Keep sample-only `NativeEma` blocked in production.
4. Add end-to-end tests for wrong-account, wrong-tenant, wrong-purpose, mismatched bytes, unknown native strategy, tampered parameters, replay/idempotency, and Python-free execution.

No API endpoint or production permission is enabled by this incremental change.
