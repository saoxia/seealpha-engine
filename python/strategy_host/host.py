"""Run one issued strategy package and return published target holdings.

The process exits after a single JSON request. It does not read exchange
credentials, open sockets, or submit orders.
"""

import importlib.util
import json
import re
import sys
import tempfile
import zipfile
from decimal import Decimal, InvalidOperation
from pathlib import Path

HOST_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(HOST_ROOT))

MAX_ARCHIVE_BYTES = 256 * 1024
MAX_MEMBERS = 32
ALLOWED_SUFFIXES = {".py", ".json", ".md", ".txt"}
NAME = re.compile(r"^[a-z][a-z0-9_]{1,40}$")
VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")
ENTRY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*$")
SECRET_FIELDS = {"api_key", "secret", "passphrase", "password", "token", "credential", "credentials"}


class PackageError(ValueError):
    pass


class Bar:
    def __init__(self, bar_type, open_price, high, low, close, volume):
        self.bar_type = bar_type
        self.open = open_price
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume


class BarType:
    def __init__(self, symbol, interval):
        self.symbol = symbol
        self.interval = interval

    def __eq__(self, other):
        return isinstance(other, BarType) and self.symbol == other.symbol and self.interval == other.interval

    def __hash__(self):
        return hash((self.symbol, self.interval))


def main():
    try:
        request = json.load(sys.stdin)
        result = handle(request)
    except PackageError as error:
        result = {"ok": False, "error": str(error) or "策略包无效。"}
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        result = {"ok": False, "error": str(error) or "策略执行失败。"}
    json.dump(result, sys.stdout, ensure_ascii=False, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


def handle(request):
    if not isinstance(request, dict):
        raise PackageError("策略请求无效。")
    parameters = request.get("parameters") or {}
    package_dir = request.get("package_dir")
    archive_path = request.get("archive_path")
    cleanup = None
    if package_dir:
        root = Path(package_dir).resolve()
        if not (root / "seealpha-package.json").is_file():
            raise PackageError("策略包缺少 seealpha-package.json。")
    elif archive_path:
        root = Path(tempfile.mkdtemp(prefix="seealpha-strategy-"))
        cleanup = root
        extract_archive(Path(archive_path), root)
    else:
        raise PackageError("策略包无效。")
    try:
        metadata = json.loads((root / "seealpha-package.json").read_text(encoding="utf-8"))
        entry_point = _metadata_entry(metadata)
        module, strategy_type = load_entry(root, entry_point)
        prepared = prepare_parameters(module, parameters)
        if request.get("op") == "prepare":
            return {"ok": True, "prepared": prepared, "publications": []}
        symbol = str(request.get("symbol") or prepared.get("symbol") or "")
        interval = str(request.get("interval") or "1m")
        publications = run_strategy(strategy_type, prepared, symbol, interval, request.get("bars") or [])
        return {"ok": True, "prepared": prepared, "publications": publications}
    finally:
        if cleanup is not None:
            import shutil
            shutil.rmtree(cleanup, ignore_errors=True)


def run_strategy(strategy_type, prepared, symbol, interval, bars):
    if not isinstance(bars, list) or len(bars) > 5000:
        raise PackageError("K 线数量超过上限。")
    bar_type = BarType(symbol, interval)
    strategy = strategy_type(object())
    strategy.parameters = prepared
    strategy.bar_type = bar_type
    publications = []
    index_box = {"index": -1}

    def publish(*, symbol, quantity=None, weight=None):
        if weight is not None or isinstance(quantity, bool) or not isinstance(quantity, Decimal):
            raise PackageError("目标数量必须是有限 Decimal。")
        if not quantity.is_finite():
            raise PackageError("目标数量必须是有限 Decimal。")
        publications.append({
            "symbol": str(symbol),
            "quantity": format(quantity, "f"),
            "bar_index": index_box["index"],
        })

    def blocked(*_args, **_kwargs):
        raise RuntimeError("策略只能公布目标持仓")

    strategy.publish = publish
    strategy.submit_order = blocked
    strategy.close_all_positions = blocked
    strategy.cancel_all_orders = blocked
    strategy.on_start()
    for index, raw in enumerate(bars):
        if not isinstance(raw, dict):
            raise PackageError("K 线无效。")
        close = Decimal(str(raw["close"]))
        bar = Bar(
            bar_type,
            Decimal(str(raw.get("open", raw["close"]))),
            Decimal(str(raw.get("high", raw["close"]))),
            Decimal(str(raw.get("low", raw["close"]))),
            close,
            Decimal(str(raw.get("volume", "0"))),
        )
        index_box["index"] = index
        for indicator in getattr(strategy, "_registered", []):
            indicator.update_raw(close)
        strategy.on_bar(bar)
    return publications


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
    except (TypeError, ValueError) as error:
        raise PackageError(str(error) or "策略参数无效。") from None
    if not isinstance(prepared, dict) or set(prepared) & SECRET_FIELDS:
        raise PackageError("策略参数无效。")
    for key, value in prepared.items():
        if not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, (str, int)):
            raise PackageError("策略参数只能包含字符串和整数。")
    symbol = prepared.get("symbol")
    if not isinstance(symbol, str) or not 1 <= len(symbol) <= 20:
        raise PackageError("策略参数需要 symbol。")
    for label in ("allocation", "risk_limit"):
        _positive_decimal(prepared.get(label), label)
    return prepared


def load_entry(root, entry_point):
    module_name, separator, attribute = entry_point.partition(":")
    if not separator or not ENTRY.fullmatch(entry_point):
        raise PackageError("策略包入口无效。")
    file_path = root.joinpath(*module_name.split(".")).with_suffix(".py").resolve()
    if not file_path.is_relative_to(root.resolve()) or not file_path.is_file():
        raise PackageError("策略包入口无效。")
    spec = importlib.util.spec_from_file_location(f"seealpha_strategy_{attribute}", file_path)
    if spec is None or spec.loader is None:
        raise PackageError("策略包入口无效。")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    strategy_type = getattr(module, attribute, None)
    if not isinstance(strategy_type, type):
        raise PackageError("策略包入口无效。")
    return module, strategy_type


def extract_archive(archive_path, dest):
    data = archive_path.read_bytes()
    if not data or len(data) > MAX_ARCHIVE_BYTES:
        raise PackageError("策略包无效。")
    try:
        with zipfile.ZipFile(archive_path) as archive:
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
                target = _member_path(info.filename, dest)
                if target is None:
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.read(info))
    except zipfile.BadZipFile as error:
        raise PackageError("策略包无效。") from error
    for path in dest.rglob("*.py"):
        if "strategy_python" in path.read_text(encoding="utf-8"):
            raise PackageError("不能从 strategy_python 加载策略。")


def _metadata_entry(document):
    if not isinstance(document, dict) or document.get("format") != 1:
        raise PackageError("策略包说明无效。")
    name = document.get("name")
    version = document.get("version")
    entry_point = document.get("entry_point")
    if not isinstance(name, str) or not NAME.fullmatch(name) or "strategy_python" in name:
        raise PackageError("策略包名称无效。")
    if not isinstance(version, str) or not VERSION.fullmatch(version):
        raise PackageError("策略包版本无效。")
    if not isinstance(entry_point, str) or not ENTRY.fullmatch(entry_point):
        raise PackageError("策略包入口无效。")
    return entry_point


def _member_path(name, root):
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


if __name__ == "__main__":
    raise SystemExit(main())
