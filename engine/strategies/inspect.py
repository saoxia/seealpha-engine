"""Short-lived package validation worker.

Executing package prepare code must never happen in the business API or
inside the resident Engine HTTP process. This worker has no account credentials
in its environment and terminates after a single bounded request.

A subprocess with time/resource limits is not a hardened OS sandbox.
Untrusted strategy code requires a separately restricted container before
allowing arbitrary third-party authors.
"""

import base64
import io
import json
import sys
from contextlib import redirect_stdout
from dataclasses import asdict

from engine.strategies.package import PackageError, open_package

MAX_STDIN = 400_000


def main():
    try:
        raw = sys.stdin.buffer.read(MAX_STDIN + 1)
        if len(raw) > MAX_STDIN:
            raise ValueError("策略包请求过大。")
        body = json.loads(raw)
        if not isinstance(body, dict):
            raise TypeError("策略包无效。")
        archive = base64.b64decode(body["archive"], validate=True)
        with redirect_stdout(io.StringIO()):
            meta, prepared = open_package(archive, body.get("parameters", {}))
        payload = {"metadata": asdict(meta), "parameters": prepared}
    except (PackageError, ValueError, TypeError, KeyError) as error:
        message = str(error) if isinstance(error, PackageError) else "策略包无效。"
        payload = {"error": message}
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
