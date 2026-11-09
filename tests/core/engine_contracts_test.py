"""Contracts and independent source boundaries for SeeAlpha Engine."""

import ast
import subprocess
import sys
from pathlib import Path

from core.engine_contracts import (
    ENGINE_CONTRACT_VERSION,
    AccountSnapshot,
    EngineConflict,
    QueryError,
)
from engine.connectors.binance.queries import AccountSnapshot as EngineAccountSnapshot
from engine.connectors.binance.queries import QueryError as EngineQueryError
from engine.runtime import EngineConflict as RuntimeEngineConflict

ROOT = Path(__file__).resolve().parents[2]


def test_engine_transport_types_keep_identity():
    assert ENGINE_CONTRACT_VERSION == "2"
    assert QueryError is EngineQueryError
    assert EngineConflict is RuntimeEngineConflict
    assert AccountSnapshot is EngineAccountSnapshot
    assert EngineConflict("rejected").message == "rejected"


def test_engine_and_core_do_not_import_platform_source():
    for folder in ("engine", "core"):
        for source in (ROOT / folder).rglob("*.py"):
            tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    names = [node.module]
                for name in names:
                    assert name.split(".", 1)[0] not in {"api", "web"}, (str(source), name)


def test_shared_transport_contract_is_importable_without_engine_runtime():
    script = (
        "import sys; from pathlib import Path; "
        "sys.path.insert(0, str(Path(sys.argv[1]))); "
        "import core.engine_contracts; "
        "assert 'engine.runtime' not in sys.modules; "
        "assert 'nautilus_trader' not in sys.modules"
    )
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(ROOT)],
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert result.returncode == 0, result.stderr
