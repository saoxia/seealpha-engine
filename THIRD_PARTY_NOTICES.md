# Third-party software / 第三方软件清单说明

**This is a compliance starting point, not a complete SBOM or an assertion that every dependency is Apache-2.0. / 此文件不是完整软件物料清单。**

Third-party components keep their own copyright, patent and licensing terms. The SeeAlpha Apache-2.0 license covers only code for which its contributors have authority to grant that license.

- Platform: see `platform/pyproject.toml`, `platform/uv.lock`, `web/package.json` and `web/package-lock.json` (where present) for Python/JavaScript dependencies.
- Engine: see `pyproject.toml`, `uv.lock` and Rust Cargo manifests/locks for NautilusTrader, data libraries and Rust dependencies. NautilusTrader is an independently licensed upstream dependency; verify and honor the terms applying to the exact redistributed version.
- Harness: see `pyproject.toml` and container dependencies for DeepSeek Harness SDK, MCP, Python packages and their transitive dependencies. The upstream SDK is **not** re-licensed by this repository's LICENSE.
- External trading-venue market data and AI model services have their own usage, redistribution and privacy terms.

Before publishing a binary, Docker image or bundled distribution: generate a version-specific inventory; verify SPDX identifiers and actual license texts for direct/transitive/vendored components; preserve required notices; and check corresponding-source or other redistribution obligations when applicable. Do not strip upstream headers or claim that no third-party restrictions exist.
