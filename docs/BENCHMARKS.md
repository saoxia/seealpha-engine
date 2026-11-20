# Hot-path benchmarks

Measured on the migration host with CPython 3.12 and the workspace release
profile (`lto = thin`, `codegen-units = 16`). Each workload runs in its own
process. The timer starts after the input is built. Peak RSS is `VmHWM`.
The number below is the median of three runs. Both sides return the same
result for each workload.

| Workload | N | Python seconds | Rust seconds | Python peak RSS | Rust peak RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Event-gap scan | 200,000 | 0.0306 | 0.00162 | 42,084 KiB | 7,380 KiB |
| Decimal net and quantize | 200,000 | 0.120 | 0.00557 | 14,648 KiB | 2,628 KiB |
| Trade to OHLCV aggregation | 200,000 | 0.0244 | 0.00788 | 82,396 KiB | 24,448 KiB |
| Closed-kline validation | 200,000 | 0.0222 | 0.00115 | 50,372 KiB | 13,548 KiB |
| Idempotent SQLite request read | 20,000 | 0.0634 | 0.0690 | 15,180 KiB | 4,004 KiB |

The Rust kline check is `validate_klines` and does the production quality
checks. The Python kline loop is the same alignment and OHLC scan. Gap,
net, and aggregation results match (`100000`, `-800002`, `34` bars). The
SQLite loop is a repeated primary-key read of one pre-registered request.
Its release timing is in the same range as CPython; the resident set is
smaller. It is not evidence that a networked database got faster.

Reproduce:

```bash
python3 benchmarks/python_hot_path.py gaps 200000
cargo run --locked --release -p seealpha-bench -- gaps 200000
```

The same commands accept `net`, `aggregate`, `klines`, and `sqlite`.
