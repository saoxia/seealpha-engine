"""Invoke an issued basket strategy's own snapshot and publication callbacks.

Only the offline research runner installs this adapter. The strategy package is
hash-verified on extraction and cannot reach a live order path: build_strategy
blocks native submit_order/close_all_positions, and the only accepted output is
a complete research target published through BasketResearchMember.
"""
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from types import MethodType
from uuid import uuid4

from engine.runtime import EngineConflict
from engine.strategies.package import (
    PackageError,
    build_strategy,
    extract,
    load_entry,
    namespace_for,
    prepare_parameters,
    release_namespace,
)


class IssuedBasketMember:
    def __init__(self, strategy):
        self.strategy = strategy
        self.subscription = None
        self.publisher = None
        self.published = False

        def subscribe(_strategy, *, symbol, products, data, option_symbols=()):
            if self.subscription is not None:
                raise EngineConflict("同一策略不能重复订阅组合市场。")
            self.subscription = {
                "symbol": symbol, "products": products, "data": data,
                "option_symbols": option_symbols,
            }

        def publish(_strategy, *, targets, constraints=None, request_id=None):
            if self.publisher is None:
                raise EngineConflict("组合目标只能在有效历史快照内发布。")
            if self.published:
                raise EngineConflict("一个快照只能发布一组完整组合目标。")
            self.publisher.publish_basket(
                targets=targets, constraints=constraints, request_id=request_id,
            )
            self.published = True

        strategy.subscribe_market_bundle = MethodType(subscribe, strategy)
        strategy.publish_basket = MethodType(publish, strategy)
        strategy.target_sink = self._reject_single
        if not callable(getattr(strategy, "on_market_snapshot", None)):
            raise EngineConflict("跨产品策略必须提供 on_market_snapshot(snapshot)。")
        strategy.on_start()
        if self.subscription is None:
            raise EngineConflict("策略未声明 subscribe_market_bundle 历史订阅。")

    @staticmethod
    def _reject_single(_strategy, _holding):
        raise EngineConflict("跨产品策略只能通过 publish_basket 发布完整组合目标。")

    def propose(self, publisher, snapshot):
        publisher.subscribe_market_bundle(**self.subscription)
        self.publisher = publisher
        self.published = False
        try:
            self.strategy.on_market_snapshot(snapshot)
            if not self.published:
                raise EngineConflict("策略未发布完整目标；模拟组合已冻结。")
        finally:
            self.publisher = None


@contextmanager
def issued_basket_members(loads, instrument, bar_type):
    """Load one isolated, approved archive per member; always release its namespace."""
    members = {}
    namespaces = []
    try:
        with TemporaryDirectory(prefix="issued-basket-") as temporary:
            root = Path(temporary)
            for index, load in enumerate(loads):
                archive = load.get("archive")
                if not isinstance(archive, (bytes, bytearray)) or not archive:
                    raise EngineConflict("跨产品策略缺少已授权归档。")
                folder = root / str(index)
                namespace = namespace_for(
                    load["tenant_id"], load["issuance_id"], uuid4().hex,
                )
                namespaces.append(namespace)
                try:
                    meta = extract(archive, folder)
                    if (
                        meta.digest != load["package_hash"]
                        or meta.name != load["code_key"]
                        or meta.version != load["version"]
                        or meta.entry_point != load["entry_point"]
                    ):
                        raise EngineConflict("跨产品策略归档与下发身份不一致。")
                    module, strategy_class = load_entry(folder, meta.entry_point, namespace)
                    parameters = prepare_parameters(module, load["parameters"])
                    if parameters != load["parameters"]:
                        raise EngineConflict("跨产品策略参数与已授权版本不一致。")
                    strategy = build_strategy(
                        strategy_class, parameters, instrument, bar_type, f"B{index}",
                    )
                    member_id = str(load["issuance_id"])
                    if member_id in members:
                        raise EngineConflict("组合成员标识重复。")
                    members[member_id] = IssuedBasketMember(strategy)
                except PackageError as exc:
                    raise EngineConflict(str(exc)) from exc
            yield members
    finally:
        for namespace in namespaces:
            release_namespace(namespace)
