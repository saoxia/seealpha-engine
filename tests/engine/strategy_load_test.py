"""Issued strategy packages stay inside one tenant and reach the coordinator as targets."""

import ast
import base64
import io
import json
import zipfile
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from engine.app import create_app
from engine.config import EngineConfig
from engine.runtime import EngineConflict, EngineRuntime
from engine.strategies.loading import clip_member_targets
from engine.strategies.package import (
    PackageError,
    archive_directory,
    extract,
    load_entry,
    namespace_for,
    parse_metadata,
    release_namespace,
)
from engine.strategies.target_netting import MemberTarget, execution_delta
from engine.strategies.targets import TargetHolding, signed_quantity

PROJECT = Path(__file__).resolve().parents[2]
EXAMPLE = PROJECT / "packages" / "ema_crossover"
CLOSES = [100] * 8 + [101, 102, 104, 108, 116, 130, 120, 100, 80, 60, 40]


def crossover_bars():
    bars = []
    previous = CLOSES[0]
    for close in CLOSES:
        high = max(previous, close) + Decimal("0.5")
        low = min(previous, close) - Decimal("0.5")
        bars.append({
            "open": format(Decimal(previous), "f"),
            "high": format(high, "f"),
            "low": format(low, "f"),
            "close": format(Decimal(close), "f"),
        })
        previous = close
    return bars


def ema_archive(version="1"):
    if version == "1":
        return archive_directory(EXAMPLE)
    return _rewritten_archive(EXAMPLE, version=version)


def _rewritten_archive(root, *, version):
    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        dest = Path(directory) / "pkg"
        dest.mkdir()
        for path in root.iterdir():
            if path.is_file():
                (dest / path.name).write_bytes(path.read_bytes())
        document = json.loads((dest / "seealpha-package.json").read_text(encoding="utf-8"))
        document["version"] = version
        (dest / "seealpha-package.json").write_text(json.dumps(document), encoding="utf-8")
        return archive_directory(dest)


def issued_payload(archive, purpose, issuance_id, parameters):
    metadata = parse_metadata(archive)
    return {
        "account_id": "desk",
        "issuance_id": issuance_id,
        "code_key": metadata.name,
        "version": metadata.version,
        "grant_purpose": purpose,
        "parameters": parameters,
        "entry_point": metadata.entry_point,
        "package_hash": metadata.digest,
        "archive": base64.b64encode(archive).decode(),
    }


def ema_parameters(allocation="1", risk_limit="1"):
    return {
        "symbol": "ETHUSDT",
        "fast_period": 3,
        "slow_period": 5,
        "allocation": allocation,
        "risk_limit": risk_limit,
    }


def _load(runtime, tenant_id, purpose, issuance_id, allocation="1", risk_limit="1"):
    return runtime.receive_strategy(tenant_id, issued_payload(
        ema_archive(), purpose, issuance_id, ema_parameters(allocation, risk_limit),
    ))


def test_target_holding_contract_and_risk_clip_before_netting():
    holding = TargetHolding(symbol="ETHUSDT", quantity=Decimal(1))
    assert signed_quantity(holding, Decimal(2)) == Decimal(1)
    weighted = TargetHolding(symbol="ETHUSDT", weight=Decimal(1))
    assert signed_quantity(weighted, Decimal(2)) == Decimal(2)
    flat = TargetHolding(symbol="ETHUSDT", weight=Decimal(0))
    assert signed_quantity(flat, Decimal(2)) == Decimal(0)
    with pytest.raises(ValueError, match="其中之一"):
        TargetHolding(symbol="ETHUSDT", quantity=Decimal(1), weight=Decimal(1))
    with pytest.raises(ValueError, match="其中之一"):
        TargetHolding(symbol="ETHUSDT")

    targets = [
        MemberTarget("a", Decimal(1)),
        MemberTarget("b", Decimal(1)),
    ]
    clipped = clip_member_targets(targets, Decimal("0.5"))
    net = sum((item.signed_quantity for item in clipped), Decimal(0))
    assert net == Decimal("0.5")
    assert execution_delta(net, Decimal(0)) == Decimal("0.5")
    assert clip_member_targets(targets, Decimal(5)) == targets


def test_package_metadata_rejects_a_path_that_escapes_the_archive(tmp_path):
    metadata = parse_metadata(ema_archive())
    assert metadata.name == "ema_crossover"
    assert metadata.entry_point == "strategy:EmaCrossover"
    assert metadata.digest == parse_metadata(ema_archive()).digest
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("seealpha-package.json", (EXAMPLE / "seealpha-package.json").read_bytes())
        archive.writestr("../outside.py", "x = 1\n")
    with pytest.raises(PackageError, match="路径"):
        parse_metadata(buffer.getvalue())
    extracted = tmp_path / "pkg"
    extract(ema_archive(), extracted)
    namespace = namespace_for("dev", "1", "abc")
    module, strategy_type = load_entry(extracted, metadata.entry_point, namespace)
    try:
        assert strategy_type.__name__ == "EmaCrossover"
        assert module.__name__.startswith("seealpha_issued.tenant_dev.issue_1.")
        assert module.prepare({"fast_period": 3, "slow_period": 5})["risk_limit"] == "1"
    finally:
        release_namespace(namespace)


def test_engine_loads_only_issued_packages_for_the_matching_tenant(tmp_path):
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        runtime.ensure_tenant("prod", "production")
        runtime.ensure_tenant("old", "development", "archived")
        slipped = io.BytesIO()
        with zipfile.ZipFile(slipped, "w") as archive:
            archive.writestr("seealpha-package.json", json.dumps({
                "format": 1, "name": "ema_crossover", "version": "1",
                "entry_point": "strategy:EmaCrossover",
            }))
            archive.writestr("strategy.py", "import strategy_python\n")
        foreign = parse_metadata(slipped.getvalue())
        with pytest.raises(EngineConflict, match="strategy_python"):
            runtime.receive_strategy("dev", issued_payload(
                slipped.getvalue(), "development", "1", {},
            ))
        assert foreign.name == "ema_crossover"
        with pytest.raises(EngineConflict, match="归档"):
            _load(runtime, "old", "development", "1")
        with pytest.raises(EngineConflict, match="正式授权"):
            _load(runtime, "prod", "development", "9")
        with pytest.raises(EngineConflict, match="不一致"):
            _load(runtime, "dev", "production", "9")
        mismatched = issued_payload(ema_archive(), "development", "3", ema_parameters())
        mismatched["package_hash"] = "0" * 64
        with pytest.raises(EngineConflict, match="哈希|不一致"):
            runtime.receive_strategy("dev", mismatched)
        loaded = _load(runtime, "dev", "development", "1")
        assert loaded["tenant_id"] == "dev"
        assert loaded["entry_point"] == "strategy:EmaCrossover"
        assert loaded["parameters"]["symbol"] == "ETHUSDT"
        assert "archive" not in loaded
        assert runtime.store.strategy_loads("prod") == []
        assert [row["issuance_id"] for row in runtime.store.strategy_loads("dev")] == ["1"]
        with pytest.raises(EngineConflict, match="没有已下发"):
            runtime.preview_strategy("prod", "desk", crossover_bars())
        production = _load(runtime, "prod", "production", "2", allocation="1", risk_limit="1")
        assert production["grant_purpose"] == "production"
        assert runtime.store.strategy_loads("dev", "desk")[0]["issuance_id"] == "1"
        blob = b"".join(path.read_bytes() for path in (tmp_path / "state").glob("book.sqlite*"))
        assert b"api_key" not in blob and b"secret" not in blob
    finally:
        runtime.close()


def test_two_tenants_load_different_packages_without_an_allowlist(tmp_path):
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        runtime.ensure_tenant("other", "development")
        _load(runtime, "dev", "development", "1", allocation="1", risk_limit="1")
        other = _package_archive(tmp_path / "flat", quantity="0")
        runtime.receive_strategy("other", issued_payload(
            other, "development", "9", {"symbol": "ETHUSDT", "allocation": "1", "risk_limit": "1"},
        ))
        first = runtime.preview_strategy("dev", "desk", crossover_bars())
        second = runtime.preview_strategy("other", "desk", crossover_bars()[:4])
    finally:
        runtime.close()
    assert {row["code_key"] for row in first["holdings"]} == {"ema_crossover"}
    assert {row["quantity"] for row in second["holdings"]} == {"0"}
    assert first["tenant_id"] == "dev"
    assert second["tenant_id"] == "other"


def test_ema_targets_are_clipped_and_ordered_by_the_coordinator(tmp_path):
    runtime = EngineRuntime(tmp_path / "state", tmp_path)
    try:
        runtime.ensure_tenant("dev", "development")
        _load(runtime, "dev", "development", "1", allocation="1", risk_limit="0.5")
        _load(runtime, "dev", "development", "2", allocation="1", risk_limit="0.5")
        result = runtime.preview_strategy("dev", "desk", crossover_bars())
    finally:
        runtime.close()
    assert [row["quantity"] for row in result["holdings"]] == ["1", "0", "1", "0"]
    assert {row["code_key"] for row in result["holdings"]} == {"ema_crossover"}
    assert [(row["side"], Decimal(row["quantity"])) for row in result["orders"]] == [
        ("BUY", Decimal("0.5")),
        ("SELL", Decimal("0.5")),
    ]
    assert all(row["tags"] == ["portfolio-net-delta"] for row in result["orders"])
    assert {row["strategy_id"] for row in result["orders"]} == set(result["coordinator_ids"])
    assert set(result["coordinator_ids"]).isdisjoint(result["member_ids"])
    assert Decimal(result["positions"][0]["quantity"]) == Decimal(0)


def test_example_strategy_does_not_submit_orders_or_import_research_packages():
    example = (EXAMPLE / "strategy.py").read_text(encoding="utf-8")
    tree = ast.parse(example)
    called = {
        node.attr for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and node.attr in {"submit_order", "close_all_positions", "cancel_all_orders"}
    }
    assert called == set()
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
    assert not any(name.startswith(("strategy_python", "engine.")) for name in modules)
    for path in (PROJECT / "engine").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        parsed = ast.parse(source, filename=str(path))
        imported = set()
        for node in ast.walk(parsed):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
        research = {name for name in imported if name.startswith("strategy_python")}
        assert research == set(), path


def test_engine_http_receive_is_tenant_scoped(tmp_path):
    app = create_app(EngineConfig(tmp_path / "state", token="engine-test", project=tmp_path))
    headers = {"Authorization": "Bearer engine-test"}
    body = issued_payload(ema_archive(), "development", "4", {
        "symbol": "ETHUSDT", "fast_period": 3, "slow_period": 5, "allocation": "1",
    })
    with TestClient(app) as client:
        assert client.post("/tenants/dev/strategy-loads", headers=headers, json=body).status_code == 409
        assert client.put(
            "/tenants/dev", headers=headers, json={"purpose": "development"},
        ).status_code == 200
        created = client.post("/tenants/dev/strategy-loads", headers=headers, json=body)
        assert created.status_code == 200, created.text
        assert created.json()["code_key"] == "ema_crossover"
        assert "archive" not in created.json()
        other = client.get("/tenants/prod/strategy-loads", headers=headers)
        assert other.json()["loads"] == []
        listed = client.get("/tenants/dev/strategy-loads", headers=headers, params={"account_id": "desk"})
        assert [row["issuance_id"] for row in listed.json()["loads"]] == ["4"]
        preview = client.post(
            "/tenants/dev/accounts/desk/strategy-preview",
            headers=headers,
            json={"bars": crossover_bars()},
        )
        assert preview.status_code == 200, preview.text
        assert preview.json()["orders"][0]["tags"] == ["portfolio-net-delta"]
        assert Decimal(preview.json()["orders"][0]["quantity"]) == Decimal(1)


def _package_archive(root, quantity):
    root.mkdir()
    (root / "seealpha-package.json").write_text(json.dumps({
        "format": 1,
        "name": "hold_flat" if quantity == "0" else "hold_long",
        "title": "固定持仓",
        "version": "1",
        "entry_point": "strategy:Hold",
        "parameters": {"symbol": "ETHUSDT", "allocation": "1", "risk_limit": "1"},
    }), encoding="utf-8")
    (root / "strategy.py").write_text(
        "from decimal import Decimal\n"
        "from nautilus_trader.trading import Strategy\n"
        "def prepare(parameters):\n"
        "    return {'symbol': 'ETHUSDT', 'allocation': '1', 'risk_limit': '1'}\n"
        "class Hold(Strategy):\n"
        "    def __init__(self, config):\n"
        "        super().__init__(config)\n"
        "        self._sent = False\n"
        "    def on_start(self):\n"
        "        self.subscribe_bars(self.bar_type)\n"
        "    def on_bar(self, bar):\n"
        "        if self._sent:\n"
        "            return\n"
        "        self._sent = True\n"
        f"        self.publish(symbol='ETHUSDT', quantity=Decimal('{quantity}'))\n",
        encoding="utf-8",
    )
    return archive_directory(root)
