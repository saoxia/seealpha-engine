"""Start the real worker entry points from a separate data directory."""

import subprocess
import sys

import pytest

from engine.supervisor import popen


@pytest.mark.parametrize("module", [
    "engine.feed.managed",
    "engine.pipeline.pair_data",
    "engine.pipeline.data_management",
])
def test_worker_module_loads_outside_checkout(tmp_path, monkeypatch, module):
    monkeypatch.delenv("PYTHONPATH", raising=False)
    process = popen(
        [sys.executable, "-m", module, "--help"],
        cwd=tmp_path, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    try:
        output, _ = process.communicate(timeout=30)
        assert process.returncode == 0, output.decode(errors="replace")
        assert b"usage:" in output
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()


def test_worker_preserves_supplied_pythonpath(tmp_path):
    extra_path = tmp_path / "extra"
    extra_path.mkdir()
    (extra_path / "worker_fixture.py").write_text("VALUE = 42\n")
    env = {"PYTHONPATH": str(extra_path)}
    with popen(
        [sys.executable, "-c", "import engine, worker_fixture; assert worker_fixture.VALUE == 42"],
        cwd=tmp_path, env=env,
    ) as process:
        assert process.wait(timeout=10) == 0
    assert env == {"PYTHONPATH": str(extra_path)}
