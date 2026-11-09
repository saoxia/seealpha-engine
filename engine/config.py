"""Engine process settings. The default token is for local development only."""

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class EngineConfig:
    state_dir: Path
    token: str
    host: str = "127.0.0.1"
    port: int = 8766
    project: Path = PROJECT
    tenant_id: str | None = None
    tenant_purpose: str | None = None

    @classmethod
    def from_environment(cls):
        state = Path(os.getenv("ENGINE_STATE", str(PROJECT / ".local/state/engine")))
        if not state.is_absolute():
            state = PROJECT / state
        return cls(
            state_dir=state,
            token=os.getenv("ENGINE_TOKEN", "seealpha-local-engine"),
            host=os.getenv("ENGINE_HOST", "127.0.0.1"),
            port=int(os.getenv("ENGINE_PORT", "8766")),
            project=Path(os.getenv("ENGINE_PROJECT", str(PROJECT))),
            tenant_id=os.getenv("ENGINE_TENANT_ID"),
            tenant_purpose=os.getenv("ENGINE_TENANT_PURPOSE"),
        )
