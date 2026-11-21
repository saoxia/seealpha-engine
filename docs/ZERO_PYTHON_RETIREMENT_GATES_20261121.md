# Rust-only Python retirement cutover (2026-11-21)

> **Status: PR #130 merged to `main` on 2026-11-21; migration parity remains untested and unverified.**
> This is a breaking change, not verified Rust parity with the former Python Engine.

## Changes introduced by PR #130

- Strategy issuance now validates exact native JSON manifest bytes instead of Python ZIP packages.
- The native manifest permits only a known precompiled Rust EMA implementation; it checks issuance metadata, SHA-256, tenant, account and purpose.
- Issued paper sessions and historical catalog replays dispatch to the Rust executor without starting a Python subprocess.
- Development-only `native_sample` remains available for explicit Rust paper simulation.
- Removed the known Python strategy host, Nautilus compatibility shims, Python sample strategy, Python contract sources, the related Python test files, optional Python image, `pyproject.toml` and `uv.lock`.
- The default Rust Docker image and Rust CI jobs remain. Legacy Python CI stages have been removed.

## Supported native manifest

```json
{"format":1,"runtime":"rust-builtin","name":"ema_crossover","version":"1","entry_point":"native:ema_crossover"}
```

Store exactly those manifest bytes (including any trailing newline) in the trusted issuance and calculate the SHA-256 on the exact bytes. Metadata and account scope must match. This selects compiled Rust code; **it is not arbitrary user-uploaded Rust code execution**.

## Breaking changes and remaining risks

- Existing Python `.zip` issuance and Python directory packages are not executable and are not automatically migrated.
- Existing Platform integration may need to change its package upload/deployment contract. The removed Python contract definitions are still available from Git history; their removal does not migrate any other repository.
- The Rust workspace still contains historical Rust adapter definitions or test fixtures referencing Python. They are not evidence of an operational Python interpreter in the default image.
- Native EMA does not implement arbitrary NautilusTrader Python strategies.
- No tests, compilation, lint, build, container or Platform end-to-end validation was performed, at user request.
- The follow-up cleanup removes the leftover `tests/__init__.py` marker and adds CI enforcement against tracked `.py`, `.pyi` and `.pyw` files. This is a future regression gate, not evidence of completed execution or a comprehensive audited tree at the time of writing.
- Live-order and historical/derivatives parity limitations in `docs/RUST_CUTOVER_AUDIT_20261121.md` remain.

## Deployment guidance

Do not deploy this merged change as a transparent compatible upgrade. Existing issued Python packages require a separate migration strategy, and production order/data functionality remains subject to existing safety gates. If old packages are needed, revert this draft change or retrieve the Python host from Git history; do not silently reinterpret Python bytecode as native strategy manifests.
