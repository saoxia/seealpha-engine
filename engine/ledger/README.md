# Ledger / Funds

The four **business subdomains** of ledger and funds are:
`accounts`, `movements`, `allocations`, and `reconciliation`.

The first two house current verified account/funds code; the latter two are
reserved boundaries for future explicitly tested behavior. This is not a
second OMS/portfolio engine or an alternative to NautilusTrader.

Existing other source files at the crate root are unchanged and outside this
specific funds subdomain reorganization. The public `Ledger` API and SQLite
schema remain untouched.

See [functional design](../../docs/architecture/LEDGER_FUNDS_LEVEL2_20261122.md).
