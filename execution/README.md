# Execution and order management

**Logical business boundary**. Existing order journal, order commands, and verified reconciliation are currently in `application/runtime`, with signed exchange IO in `connectivity/venue` and durable financial state in `accounting/ledger`.

This directory does not yet contain a standalone execution crate. Never replay an unknown order, invent fills, or treat transport acknowledgement as a settlement fact during future refactoring.
