# Risk management

**Logical business boundary**. Existing risk clipping is implemented in `portfolio/core`; additional order admission gates and recovery controls live in `application/runtime`.

This directory does not yet contain a standalone Rust risk crate. Do not split shared transaction boundaries or move risk checks without tests for order admission, maximum exposure, idempotency, and unknown-result recovery.
