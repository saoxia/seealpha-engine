"""Extract and load an issued strategy package.

A package is a zip archive. ``seealpha-package.json`` at the archive root names
the package, version, and entry point. The engine does not keep a list of
package names. Each load uses a module name that includes the tenant id, so two
tenants do not share the imported module.
"""

import hashlib
import importlib.machinery
import importlib.util
import io
import json
import re
import shutil
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import uuid4

from nautilus_trader.trading import StrategyConfig

from engine.strategies.targets import TargetHolding


class PackageError(ValueError):
    """The archive, entry point, or parameters do not match the package contract."""


METADATA_NAME = "seealpha-package.json"
MAX_ARCHIVE_BYTES = 256 * 1024
MAX_MEMBERS = 32
SECRET_FIELDS = {"api_key", "secret", "passphrase", "password", "token", "credential", "credentials"}
ALLOWED_SUFFIXES = {".py", ".json", ".md", ".txt"}
_NAME = re.compile(r"^[a-z][a-z0-9_]{1,40}$")
_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")
_ENTRY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$")
_IDENTIFIER = re.compile(r"[^0-9A-Za-z_]")


@dataclass(frozen=True)
class PackageMetadata:
    name: str
    title: str
    version: str
    entry_point: str
    description: str | None
    parameters: dict
    digest: str


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def archive_directory(root: Path) -> bytes:
    """Zip a package directory. The metadata file must sit at the archive root."""
    root = Path(root)
    metadata = root / METADATA_NAME
    if not metadata.is_file():
        raise PackageError("策略包缺少 seealpha-package.json。")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        paths = [path for path in root.rglob("*") if path.is_file() and "__pycache__" not in path.parts]
        for path in sorted(paths):
            relative = path.relative_to(root).as_posix()
            if Path(relative).suffix.lower() not in ALLOWED_SUFFIXES:
                continue
            info = zipfile.ZipInfo(relative)
            info.date_time = (2026, 1, 1, 0, 0, 0)
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
    data = buffer.getvalue()
    parse_metadata(data)
    return data


def parse_metadata(data: bytes) -> PackageMetadata:
    if not isinstance(data, (bytes, bytearray)) or not data or len(data) > MAX_ARCHIVE_BYTES:
        raise PackageError("策略包无效。")
    try:
        with zipfile.ZipFile(io.BytesIO(bytes(data))) as archive:
            _reject_members(archive)
            try:
                payload = archive.read(METADATA_NAME)
            except KeyError:
                raise PackageError("策略包缺少 seealpha-package.json。") from None
    except zipfile.BadZipFile:
        raise PackageError("策略包无效。") from None
    return _metadata(payload, digest(bytes(data)))


def extract(data: bytes, dest: Path) -> PackageMetadata:
    meta = parse_metadata(data)
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(bytes(data))) as archive:
        for info in archive.infolist():
            target = _member_path(info.filename, dest)
            if target is None:
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(info))
    if _imports_removed_tree(dest):
        raise PackageError("不能从 strategy_python 加载策略。")
    return meta


def namespace_for(tenant_id, issuance_id, token: str) -> str:
    tenant = _IDENTIFIER.sub("_", str(tenant_id)).strip("_") or "tenant"
    issuance = _IDENTIFIER.sub("_", str(issuance_id)).strip("_") or "issue"
    suffix = _IDENTIFIER.sub("", token)[:12] or "load"
    return f"seealpha_issued.tenant_{tenant}.issue_{issuance}.{suffix}"


def load_entry(root: Path, entry_point: str, namespace: str):
    module_name, separator, attribute = entry_point.partition(":")
    if not separator or not _ENTRY.fullmatch(entry_point):
        raise PackageError("策略包入口无效。")
    relative = Path(*module_name.split(".")).with_suffix(".py")
    file_path = (Path(root) / relative).resolve()
    if not file_path.is_relative_to(Path(root).resolve()) or not file_path.is_file():
        raise PackageError("策略包入口无效。")
    full_name = f"{namespace}.{module_name}"
    parent = types_module(namespace, root)
    spec = importlib.util.spec_from_file_location(full_name, file_path)
    if spec is None or spec.loader is None:
        raise PackageError("策略包入口无效。")
    module = importlib.util.module_from_spec(spec)
    module.__package__ = parent.__name__
    sys.modules[full_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        sys.modules.pop(full_name, None)
        raise PackageError("策略包入口无法加载。") from exc
    strategy_type = getattr(module, attribute, None)
    if not isinstance(strategy_type, type):
        raise PackageError("策略包入口无效。")
    return module, strategy_type


def types_module(namespace: str, root: Path):
    package = sys.modules.get(namespace)
    if package is None:
        package = importlib.util.module_from_spec(
            importlib.machinery.ModuleSpec(namespace, None, is_package=True)
        )
        package.__name__ = namespace
        package.__path__ = [str(root)]
        package.__package__ = namespace
        sys.modules[namespace] = package
    return package


def release_namespace(namespace: str) -> None:
    prefix = namespace + "."
    for name in list(sys.modules):
        if name == namespace or name.startswith(prefix):
            sys.modules.pop(name, None)


def prepare_parameters(module, parameters):
    if not isinstance(parameters, dict):
        raise PackageError("策略参数无效。")
    if set(parameters) & SECRET_FIELDS:
        raise PackageError("策略参数不能包含密钥。")
    preparer = getattr(module, "prepare", None)
    try:
        prepared = dict(parameters) if preparer is None else preparer(dict(parameters))
    except PackageError:
        raise
    except (TypeError, ValueError) as exc:
        raise PackageError(str(exc) or "策略参数无效。") from exc
    if not isinstance(prepared, dict) or set(prepared) & SECRET_FIELDS:
        raise PackageError("策略参数不能包含密钥。" if isinstance(prepared, dict) else "策略参数无效。")
    for key, value in prepared.items():
        if not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, (str, int)):
            raise PackageError("策略参数只能包含字符串和整数。")
    symbol = prepared.get("symbol")
    if not isinstance(symbol, str) or not 1 <= len(symbol) <= 20:
        raise PackageError("策略参数需要 symbol。")
    for label in ("allocation", "risk_limit"):
        _positive_decimal(prepared.get(label), label)
    return prepared


def build_strategy(strategy_type, parameters, instrument, bar_type, order_id_tag):
    strategy = strategy_type(StrategyConfig(order_id_tag=order_id_tag))
    strategy.parameters = parameters
    strategy.instrument_id = instrument.id
    strategy.bar_type = bar_type
    strategy.published = []
    strategy.target_sink = None
    strategy.publish = _publish.__get__(strategy, strategy.__class__)
    strategy.submit_order = _blocked
    strategy.close_all_positions = _blocked
    strategy.cancel_all_orders = _blocked
    return strategy


def open_package(data: bytes, parameters):
    """Validate an archive and return its metadata plus prepared parameters."""
    meta = parse_metadata(data)
    directory = Path(tempfile.mkdtemp())
    namespace = namespace_for("register", meta.name, uuid4().hex)
    try:
        extract(data, directory)
        module, _strategy_type = load_entry(directory, meta.entry_point, namespace)
        prepared = prepare_parameters(module, parameters if parameters is not None else {})
    finally:
        release_namespace(namespace)
        shutil.rmtree(directory, ignore_errors=True)
    return meta, prepared


def _publish(self, *, symbol, quantity=None, weight=None):
    holding = TargetHolding(
        symbol=str(symbol),
        quantity=None if quantity is None else _target_decimal(quantity),
        weight=None if weight is None else _target_decimal(weight),
    )
    self.published.append(holding)
    sink = self.target_sink
    if sink is not None:
        sink(self, holding)


def _blocked(*_args, **_kwargs):
    raise RuntimeError("策略只能公布目标持仓")


def _target_decimal(value):
    if isinstance(value, bool) or not isinstance(value, Decimal) or not value.is_finite():
        raise PackageError("目标数量和权重必须是有限 Decimal。")
    return value


def _positive_decimal(value, label):
    labels = {"allocation": "分配数量", "risk_limit": "风险额度"}
    name = labels.get(label, label)
    try:
        amount = Decimal(value)
    except (InvalidOperation, TypeError, ValueError):
        raise PackageError(f"{name}必须是大于零的十进制数。") from None
    digits = amount.as_tuple()
    if (
        isinstance(value, bool)
        or not isinstance(value, str)
        or not amount.is_finite()
        or amount <= 0
        or digits.exponent < -8
        or len(digits.digits) > 30
    ):
        raise PackageError(f"{name}必须是大于零的十进制数。")


def _metadata(payload: bytes, package_hash: str) -> PackageMetadata:
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        raise PackageError("策略包说明无效。") from None
    if not isinstance(document, dict) or document.get("format") != 1:
        raise PackageError("策略包说明无效。")
    name = document.get("name")
    version = document.get("version")
    entry_point = document.get("entry_point")
    if not isinstance(name, str) or not _NAME.fullmatch(name):
        raise PackageError("策略包名称无效。")
    if "strategy_python" in name:
        raise PackageError("不能从 strategy_python 加载策略。")
    if not isinstance(version, str) or not _VERSION.fullmatch(version):
        raise PackageError("策略包版本无效。")
    if not isinstance(entry_point, str) or not _ENTRY.fullmatch(entry_point):
        raise PackageError("策略包入口无效。")
    title = document.get("title", name)
    if not isinstance(title, str) or not 1 <= len(title.strip()) <= 100:
        raise PackageError("策略包名称无效。")
    description = document.get("description")
    if description is not None and (not isinstance(description, str) or len(description) > 500):
        raise PackageError("策略包说明无效。")
    parameters = document.get("parameters", {})
    if not isinstance(parameters, dict):
        raise PackageError("策略包说明无效。")
    return PackageMetadata(
        name=name,
        title=title.strip(),
        version=version,
        entry_point=entry_point,
        description=None if description is None else description.strip(),
        parameters=parameters,
        digest=package_hash,
    )


def _reject_members(archive: zipfile.ZipFile) -> None:
    infos = archive.infolist()
    if len(infos) > MAX_MEMBERS:
        raise PackageError("策略包无效。")
    total = 0
    for info in infos:
        if info.flag_bits & 0x1:
            raise PackageError("策略包无效。")
        total += info.file_size
        if total > MAX_ARCHIVE_BYTES:
            raise PackageError("策略包无效。")
        _member_path(info.filename, Path("/"))


def _member_path(name: str, root: Path):
    if not name or name.endswith("/"):
        return None
    path = Path(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        raise PackageError("策略包路径无效。")
    if path.suffix.lower() not in ALLOWED_SUFFIXES or "__pycache__" in path.parts:
        raise PackageError("策略包包含不支持的文件。")
    if any(part.startswith("strategy_python") for part in path.parts):
        raise PackageError("不能从 strategy_python 加载策略。")
    return root / path


def _imports_removed_tree(root: Path) -> bool:
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if "strategy_python" in text:
            return True
    return False
