"""Shared HTTP contract stays importable without the Rust runtime or Nautilus."""

import ast
import subprocess
import sys
import unittest
from pathlib import Path

from core.engine_contracts import ENGINE_CONTRACT_VERSION, EngineConflict

ROOT = Path(__file__).resolve().parents[2]


class ContractTest(unittest.TestCase):
    def test_version_and_conflict_message(self):
        self.assertEqual(ENGINE_CONTRACT_VERSION, "2")
        self.assertEqual(EngineConflict("rejected").message, "rejected")

    def test_remaining_python_does_not_import_platform_packages(self):
        for folder in ("core", "python", "packages"):
            for source in (ROOT / folder).rglob("*.py"):
                tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
                for node in ast.walk(tree):
                    names = []
                    if isinstance(node, ast.Import):
                        names = [alias.name for alias in node.names]
                    elif isinstance(node, ast.ImportFrom) and node.module:
                        names = [node.module]
                    for name in names:
                        self.assertNotIn(name.split(".", 1)[0], {"api", "web"}, (str(source), name))

    def test_contract_import_does_not_load_nautilus(self):
        script = (
            "import sys; from pathlib import Path; "
            "sys.path.insert(0, str(Path(sys.argv[1]))); "
            "import core.engine_contracts; "
            "assert 'nautilus_trader' not in sys.modules"
        )
        result = subprocess.run(
            [sys.executable, "-I", "-c", script, str(ROOT)],
            capture_output=True, text=True, timeout=30, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
