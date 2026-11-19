"""Engine process settings. authentication is opt-in."""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class EngineConfig:
    state_dir: Path
    token: str = ""
    auth_mode: str = "none"
    host: str = "127.0.0.1"
    port: int = 8766
    project: Path = PROJECT
    tenant_id: str | None = None
    tenant_purpose: str | None = None
    research_signing_key: str | None = None
    backtest_max_workers: int = 1
    backtest_timeout_seconds: int = 3600

    @classmethod
    def from_environment(cls):
        state = Path(os.getenv("ENGINE_STATE", str(PROJECT / ".local/state/engine")))
        if not state.is_absolute():
            state = PROJECT / state
        return cls(
            state_dir=state,
            token=os.getenv("ENGINE_TOKEN", ""),
            auth_mode=os.getenv("ENGINE_AUTH_MODE", "none"),
            host=os.getenv("ENGINE_HOST", "127.0.0.1"),
            port=int(os.getenv("ENGINE_PORT", "8766")),
            project=Path(os.getenv("ENGINE_PROJECT", str(PROJECT))),
            tenant_id=os.getenv("ENGINE_TENANT_ID"),
            tenant_purpose=os.getenv("ENGINE_TENANT_PURPOSE"),
            research_signing_key=os.getenv("ENGINE_RESEARCH_SIGNING_KEY"),
            backtest_max_workers=int(os.getenv("ENGINE_BACKTEST_MAX_WORKERS", "1")),
            backtest_timeout_seconds=int(os.getenv("ENGINE_BACKTEST_TIMEOUT_SECONDS", "3600")),
        )
