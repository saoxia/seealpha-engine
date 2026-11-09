"""Read-only compatibility smoke against an independently deployed SeeAlpha Engine.

Requires only Python standard library. Never sends orders, account credentials,
strategy code, production state mutation, or exchange requests.
"""

import argparse
import ipaddress
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

CONTRACT_HEADER = "X-SeeAlpha-Engine-Contract-Version"
CONTRACT_VERSION = "2"


def request_status(base_url, token, *, contract=CONTRACT_VERSION, timeout=5):
    headers = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if contract is not None:
        headers[CONTRACT_HEADER] = contract
    req = Request(base_url + "/health", headers=headers, method="GET")
    try:
        with urlopen(req, timeout=timeout) as response:
            return response.status, response.headers.get(CONTRACT_HEADER), json.loads(response.read())
    except HTTPError as error:
        # Do not log body or credentials; only return the declared status/version.
        return error.code, error.headers.get(CONTRACT_HEADER), None


def validate_target(url):
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("ENGINE_URL must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("ENGINE_URL must not embed credentials, a path, or a query")
    if parsed.scheme == "http":
        try:
            local = ipaddress.ip_address(parsed.hostname).is_loopback
        except ValueError:
            local = parsed.hostname == "localhost"
        if not local:
            raise ValueError("Bearer token must not be sent over non-loopback HTTP; use HTTPS")
    return url.rstrip("/")


def check(base_url, token, *, timeout=5):
    if not token:
        raise ValueError("Set ENGINE_TOKEN in the environment; no fallback default is accepted")
    status, version, _ = request_status(base_url, None, timeout=timeout)
    if status != 401:
        raise RuntimeError(f"Unauthenticated /health should return 401, got {status}")
    status, version, health = request_status(base_url, token, timeout=timeout)
    if status != 200 or version != CONTRACT_VERSION:
        raise RuntimeError(f"Authenticated /health should return 200 with v2 header, got {status}/{version}")
    if not isinstance(health, dict) or health.get("contract_version") != CONTRACT_VERSION:
        raise RuntimeError("Engine /health JSON contract_version does not match v2")
    status, version, _ = request_status(base_url, token, contract="incompatible", timeout=timeout)
    if status != 426 or version != CONTRACT_VERSION:
        raise RuntimeError(f"Incompatible contract should return 426 with v2 header, got {status}/{version}")


def main():
    parser = argparse.ArgumentParser(description="Read-only SeeAlpha Engine HTTP v2 compatibility check")
    parser.add_argument("--url", default=os.getenv("ENGINE_URL", "http://127.0.0.1:8766"))
    parser.add_argument("--timeout", type=float, default=5)
    args = parser.parse_args()
    try:
        if args.timeout <= 0 or args.timeout > 60:
            raise ValueError("timeout must be > 0 and <= 60 seconds")
        check(validate_target(args.url), os.getenv("ENGINE_TOKEN"), timeout=args.timeout)
    except (ValueError, RuntimeError, URLError, TimeoutError) as error:
        print(f"Engine handshake FAILED: {error}", file=sys.stderr)
        return 1
    print(f"Engine handshake OK: HTTP v{CONTRACT_VERSION}, authentication and incompatibility rejection")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
