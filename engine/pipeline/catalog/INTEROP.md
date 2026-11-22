# SeeAlpha Catalog / NautilusTrader v2 RC9 interoperability gate

This crate reads existing Nautilus v2 Arrow/Parquet files without moving,
rewriting, repairing, or deleting source history.

## APIs

- Catalog::read_bars: strict Binance close-minus-1ms bars, precision and
  instrument identity checks; bounded reads in [start_ns, end_ns).
- Catalog::read_records: schema-preserving non-Bar / raw-Bar projection,
  nanoseconds as **decimal strings** in JSON (never floating-point),
  non-finite values fail closed.
- Catalog::bar_gaps: reports missing aligned Bar windows without fabricating
  bars or starting collector work.
- Catalog::append_bars: existing immutable Bar publisher with stricter
  declared price/size precision validation.
- Catalog::append_record_batches: writes **non-Bar native events**, including
  TradeTick, QuoteTick, OrderBookDepth, OptionGreeks, FundingRateUpdate,
  InstrumentStatus, InstrumentClose, Instruments and feature/supplement types.
  The batches must carry an authoritative pinned Nautilus Arrow schema and
  metadata. Rust does not invent an upstream schema. Identity, time order,
  matching schema, explicit decimals, and duplicate TradeTick IDs are checked.

Writers acquire the per-directory .writer.lock (Rust-side flock), write
temporary Parquet under the same Catalog root, sync files, publish by
hard_link (create-if-absent) and sync the destination directory. Exactly
matching logical retries succeed. Overlapping history is refused. Python
code and old Parquet are never rewritten.

## Local isolated test commands

Run the Rust unit suite:

    cargo test --locked -p seealpha-catalog

With the **repo-pinned** Python NautilusTrader/PyArrow dependencies installed,
from the repository root and using a new empty non-production directory:

    uv run python crates/seealpha-catalog/tests/interop.py prepare /tmp/sa-catalog-parity
    SEEALPHA_CATALOG_PARITY_ROOT=/tmp/sa-catalog-parity cargo test --locked -p seealpha-catalog --test native_parity -- --ignored
    uv run python crates/seealpha-catalog/tests/interop.py verify /tmp/sa-catalog-parity

The first step writes Python NativeCatalog Bar, TradeTick, QuoteTick,
FundingRateUpdate and OrderBookDepth fixtures, with original SHA-256 hashes.
Rust reads and publishes copies in a separate directory, verifying Rust-side
logical equality. Python then queries both catalogs using NativeCatalog,
compares model fields and confirms source SHA-256 hashes have not changed.

The opt-in test must fail when a pinned Nautilus schema is incompatible:
it is NOT an automatically passing or mocked parity assertion.

OptionGreeks, instruments, lifecycle and custom types still require
additional real RC9-produced fixtures and native model round-trips before
claiming complete parity. Generic schema-preserving Arrow support alone is
not proof of upstream model equivalence.

## Outstanding integration and integrity constraints

1. **Cross-language locking**: Python NativeCatalog writers do not take the
   Rust .writer.lock. Never run old Python and Rust writers concurrently
   in the same root. Introduce a shared lock or enforce single-writer handover.
2. **History**: filenames and Parquet row ranges do not certify Binance
   source continuity, checksums, Trade ID completeness, REST/WS handover,
   option settlement or authoritative archive provenance. Bar gap reporting
   is read-only and does not repair.
3. **Bar time convention**: strict read_bars/append_bars use Binance
   close-minus-1ms. Raw native Bar records with other event-time conventions
   can be inspected using read_records(RecordKind::Bars,...), not coerced
   into the strict CatalogBar.
4. **Durability**: atomic publication requires same-filesystem hard links.
   External writers not cooperating in this lock can still race by creating
   overlapping files with distinct names.
5. **Gate**: the migration audit records paused CI in
   .github/workflows-paused/. Execute Cargo fmt, clippy, tests, Python
   parity and workspace CI in a provisioned environment, record the actual
   results on the PR and resolve discrepancies before a safe merge.

The catalog is an offline, bounded data foundation. It does not start
subscriptions, modify a trading ledger, or enable live risk.
