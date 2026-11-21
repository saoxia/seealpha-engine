"""Legacy Python Engine entrypoint retired in favor of the native Rust HTTP server."""


def main():
    raise SystemExit(
        "The Python Engine HTTP API has been retired. "
        "Start the native service with: cargo run --locked --release -p seealpha-engine"
    )


if __name__ == "__main__":
    main()
