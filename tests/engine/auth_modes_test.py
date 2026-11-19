"""Engine HTTP authentication modes: opt-in token and loopback-only default."""
import pytest
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig

TOKEN = "platform-generated-test-token-" + "x" * 48


def test_default_none_requires_no_bearer(tmp_path, monkeypatch):
    monkeypatch.delenv("ENGINE_AUTH_MODE", raising=False)
    monkeypatch.delenv("ENGINE_TOKEN", raising=False)
    config = EngineConfig.from_environment()
    assert config.auth_mode == "none"
    assert config.token == ""

    app = create_app(EngineConfig(tmp_path / "state", project=tmp_path))
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get(
            "/health", headers={"Authorization": "Bearer wrong"}
        ).status_code == 200


def test_token_mode_requires_correct_external_bearer(tmp_path):
    app = create_app(EngineConfig(
        tmp_path / "state", token=TOKEN, auth_mode="token", project=tmp_path,
        host="0.0.0.0",
    ))
    with TestClient(app) as client:
        assert client.get("/health").status_code == 401
        assert client.get(
            "/health", headers={"Authorization": "Bearer wrong"}
        ).status_code == 401
        assert client.get(
            "/health", headers={"Authorization": f"Bearer {TOKEN}"}
        ).status_code == 200


@pytest.mark.parametrize("mode,token,host", [
    ("invalid", "", "127.0.0.1"),
    ("token", "", "127.0.0.1"),
    ("token", "too-short", "127.0.0.1"),
    ("none", "", "0.0.0.0"),
    ("none", "", "::"),
    ("none", "", "example.internal"),
])
def test_unsafe_or_invalid_auth_configuration_fails_at_startup(
    tmp_path, mode, token, host,
):
    with pytest.raises(ValueError, match="ENGINE_AUTH_MODE|ENGINE_TOKEN|loopback"):
        create_app(EngineConfig(
            tmp_path / "state", project=tmp_path,
            auth_mode=mode, token=token, host=host,
        ))


def test_environment_requires_explicit_token_opt_in(monkeypatch):
    monkeypatch.setenv("ENGINE_TOKEN", TOKEN)
    monkeypatch.delenv("ENGINE_AUTH_MODE", raising=False)
    assert EngineConfig.from_environment().auth_mode == "none"
    monkeypatch.setenv("ENGINE_AUTH_MODE", "token")
    config = EngineConfig.from_environment()
    assert config.auth_mode == "token"
    assert config.token == TOKEN
