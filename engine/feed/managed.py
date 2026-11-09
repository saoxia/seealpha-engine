"""Fixed managed entry point for one public minute subscription."""

import argparse
import asyncio
import fcntl
import json
import signal
from pathlib import Path

from core.market.instruments import normalize_symbol

from .aggregation import PERIODS
from .service import serve, write_status


def main():
    parser = argparse.ArgumentParser(description="Managed public minute feed; no execution client")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--symbol")
    parser.add_argument("--intervals", nargs="+", choices=tuple(PERIODS))
    parser.add_argument("--options-config", type=Path, help="Runtime snapshot for the shared options collector")
    parser.add_argument("--policy", help="Validated pair configuration JSON")
    parser.add_argument("--warmup-minutes", type=int, default=60)
    parser.add_argument("--role", choices=("combined", "collection", "calculation"), default="combined")
    args = parser.parse_args()
    def stop(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, stop)
    if not 15 <= args.warmup_minutes <= 135 * 1440:
        parser.error("Invalid warmup")
    if args.options_config:
        if args.role == "calculation":
            parser.error("Options batch collection does not run local calculation")
        if not args.data_root.is_dir() or not args.root.resolve().is_relative_to(args.data_root.resolve()):
            parser.error("Data mount unavailable")
        args.root.mkdir(parents=True, exist_ok=True)
        with (args.root / "writer.lock").open("a") as lease:
            fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            from .options_service import serve_batch
            try:
                asyncio.run(serve_batch(args.options_config, args.root.parent, args.catalog))
            except KeyboardInterrupt:
                pass
        return
    if not args.symbol or not args.intervals:
        parser.error("A single subscription requires symbol and intervals")
    policy = json.loads(args.policy) if args.policy else None
    args.symbols = [normalize_symbol(args.symbol, policy["product"] if policy else "futures_um")]
    args.duration_seconds = None
    # The caller checks the mounted data directory; never silently recreate a lost mount.
    if not args.data_root.is_dir() or not args.root.resolve().is_relative_to(args.data_root.resolve()):
        parser.error("Data mount unavailable")
    args.root.mkdir(parents=True, exist_ok=True)
    with (args.root / ("calculation.lock" if args.role == "calculation" else "writer.lock")).open("a") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            if args.policy:
                from .pair_service import serve_pair
                if policy["product"] == "options":
                    if args.role == "calculation":
                        raise ValueError("期权直接保存交易所闭合 K 线，不启动本地派生")
                    from .options_service import serve_options
                    asyncio.run(serve_options(policy, args.root, args.catalog))
                elif args.role == "calculation":
                    from .pair_calculation import serve_calculation
                    asyncio.run(serve_calculation(json.loads(args.policy), args.root, args.catalog))
                else:
                    asyncio.run(serve_pair(json.loads(args.policy), args.root, args.catalog, calculate=args.role == "combined"))
            else:
                asyncio.run(serve(args, args.root))
        except KeyboardInterrupt:
            pass
        except Exception as exc:
            import time
            now = int(time.time() * 1000)
            status_root = args.root / "calculation" if args.role == "calculation" else args.root
            status_root.mkdir(parents=True, exist_ok=True)
            write_status(status_root, {"state": "DEGRADED", "data_ready": False,
                                    "reason": str(exc), "checked_ms": now, "valid_until_ms": now})
            raise


if __name__ == "__main__":
    main()
