"""Read-only queries through Nautilus public factories, never private HTTP clients."""

import asyncio
from decimal import Decimal
from traceback import clear_frames

from nautilus_trader.adapters.binance import (
    BinanceDataClientConfig,
    BinanceDataClientFactory,
    BinanceEnvironment,
    BinanceExecutionClientConfig,
    BinanceExecutionClientFactory,
    BinanceInstrumentProviderConfig,
    BinanceProductType,
    load_binance_instruments,
)
from nautilus_trader.common import Environment, LoggerConfig, LogLevel
from nautilus_trader.live import LiveExecutionEngineConfig, LiveNode
from nautilus_trader.model import AccountId, OmsType, TraderId
from nautilus_trader.trading import Strategy

from core.engine_contracts import AccountSnapshot, QueryError


def _clear_exception_frames(error):
    """Release native frame locals before an error can be collected on another thread."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        clear_frames(current.__traceback__)
        pending.extend(related for related in (current.__cause__, current.__context__) if related is not None)


def environment(value):
    if value not in {"production", "sandbox"}:
        raise ValueError("不支持的 Binance 环境。")
    return BinanceEnvironment.LIVE if value == "production" else BinanceEnvironment.TESTNET


async def instruments(env, *, http_url=None):
    config = BinanceDataClientConfig(
        product_type=BinanceProductType.USD_M,
        environment=environment(env),
        base_url_http=http_url,
        instrument_provider=BinanceInstrumentProviderConfig(
            load_all=True,
            query_commission_rates=False,
        ),
        max_retries=1,
    )
    try:
        loaded = await asyncio.wait_for(load_binance_instruments(config), 30)
    except Exception:  # noqa: BLE001 - Native failures must not expose signed request details.
        raise QueryError("Binance 品种查询失败，请检查网络与环境后重试。") from None
    return [item for item in loaded if str(item.id.symbol).endswith("-PERP")]


class SnapshotReady(Strategy):
    """Observe startup reconciliation only. No order or configuration commands."""

    def __init__(self):
        super().__init__()
        self.loop, self.ready = asyncio.get_running_loop(), asyncio.Event()

    def on_start(self):
        self.loop.call_soon_threadsafe(self.ready.set)


async def account_snapshot(
    account,
    *,
    http_url=None,
    websocket_url=None,
    timeout=30,
    log_level=LogLevel.OFF,
    operation_records=None,
    reconciliation_lookback_minutes=1,
):
    """Keep the native node's entire lifetime on a worker thread with its own loop."""
    def read():
        try:
            return asyncio.run(_account_snapshot(
                account,
                http_url=http_url,
                websocket_url=websocket_url,
                timeout=timeout,
                log_level=log_level,
                operation_records=operation_records,
                reconciliation_lookback_minutes=reconciliation_lookback_minutes,
            ))
        except Exception as exc:  # noqa: BLE001 - Native tracebacks must stay on this thread.
            message = str(exc) if isinstance(exc, QueryError) else None
            # Failed tasks retain native objects through exception frame locals.
            # Release those references here, before another thread can collect them.
            _clear_exception_frames(exc)
            if message is not None:
                return message
            return "Binance 账户查询失败，请检查凭证、权限、环境和网络。"

    result = await asyncio.to_thread(read)
    if isinstance(result, str):
        raise QueryError(result)
    return result


def account_node(account, *, http_url=None, websocket_url=None, timeout=30,
                 log_level=LogLevel.OFF, reconciliation_lookback_minutes=1, manual=False, manual_position_side=None):
    """Build a public native account node; manual execution requires explicit opt-in."""
    account_id = AccountId(f"BINANCE-{account['id']}")
    credentials = account["credentials"]
    if credentials["apiKey"] == credentials["secret"]:
        raise QueryError(
            "已保存的 API Key 与 Secret Key 相同，无法生成有效签名。"
            "请编辑账户，填写同一组 API 对应的 Key 和 Secret Key。"
        )
    config = BinanceExecutionClientConfig(
        account_id=account_id,
        product_type=BinanceProductType.USD_M,
        environment=environment(account["environment"]),
        api_key=credentials["apiKey"],
        api_secret=credentials["secret"],
        base_url_http=http_url,
        base_url_ws=websocket_url,
        use_ws_trading=False,
        oms_type=OmsType.NETTING if manual and manual_position_side == "BOTH" else OmsType.HEDGING,
        use_position_ids=True,
        instrument_provider=BinanceInstrumentProviderConfig(
            load_all=True,
            query_commission_rates=False,
        ),
        max_retries=0 if manual else 1,
    )
    builder = LiveNode.builder(
        f"account-{'manual' if manual else 'read'}-{account['id']}",
        TraderId(f"{'MANUAL' if manual else 'READ'}-{account['id']:03d}"),
        Environment.LIVE,
    )
    builder.with_delay_post_stop_secs(0).with_delay_shutdown_secs(0)
    builder.with_timeout_connection(timeout).with_timeout_reconciliation(timeout)
    builder.with_timeout_portfolio(5).with_timeout_disconnection_secs(3)
    builder.with_logging(LoggerConfig(stdout_level=log_level))
    builder.with_exec_engine_config(
        LiveExecutionEngineConfig(
            reconciliation=True,
            reconciliation_startup_delay_secs=0,
            reconciliation_lookback_mins=reconciliation_lookback_minutes,
            # Reconciliation creates local synthetic cache entries for external
            # positions; it does not submit these orders to the exchange.
            generate_missing_orders=True,
            inflight_check_interval_ms=0,
        )
    )
    builder.add_exec_client("BINANCE", BinanceExecutionClientFactory(), config)
    # Execution providers keep their own instrument registry; a data client publishes
    # the same complete universe into the node cache before position reconciliation.
    builder.add_data_client(
        "BINANCE",
        BinanceDataClientFactory(),
        BinanceDataClientConfig(
            product_type=BinanceProductType.USD_M,
            environment=environment(account["environment"]),
            base_url_http=http_url,
            base_url_ws=websocket_url,
            instrument_provider=BinanceInstrumentProviderConfig(
                load_all=True, query_commission_rates=False
            ),
            max_retries=0 if manual else 1,
        ),
    )
    node = builder.build()
    return node, account_id


async def _account_snapshot(
    account,
    *,
    http_url=None,
    websocket_url=None,
    timeout=30,
    log_level=LogLevel.OFF,
    operation_records=None,
    reconciliation_lookback_minutes=1,
):
    """Bounded native node; snapshots are single-flight/cached by the owning service.

    Reconciliation loads the complete USD-M instrument scope, not just UI watchlist symbols.
    Configuration never sets futures leverage/margin type or installs trading strategies.
    """
    node, account_id = account_node(
        account, http_url=http_url, websocket_url=websocket_url, timeout=timeout,
        log_level=log_level, reconciliation_lookback_minutes=reconciliation_lookback_minutes,
    )
    observer = SnapshotReady()
    ready = observer.ready
    node.add_strategy(observer)
    ledger_observer = None
    if operation_records is not None:
        from .operations import LedgerObserver

        ledger_observer = LedgerObserver(account)
        node.add_strategy(ledger_observer)
    cache, handle = node.cache, node.handle()
    task = asyncio.create_task(node.run_async())
    waiter = asyncio.create_task(ready.wait())
    try:
        done, _ = await asyncio.wait(
            [task, waiter], timeout=timeout, return_when=asyncio.FIRST_COMPLETED
        )
        if task in done:
            await task
            raise QueryError("Binance 原生账户节点已停止。")
        if waiter not in done:
            raise QueryError("Binance 账户查询超时，未返回完整快照。")
        native_account = cache.account(account_id)
        if native_account is None or native_account.last_event is None:
            raise QueryError("Binance 未返回账户快照。")
        event = native_account.last_event
        balances = {
            str(currency): str(money.as_decimal())
            for currency, money in native_account.balances_total().items()
        }
        positions = []
        for position in cache.positions_open(account_id=account_id):
            instrument = cache.instrument(position.instrument_id)
            if instrument is None:
                raise QueryError("Binance 仓位包含未解析品种，拒绝返回不完整结果。")
            positions.append(
                {
                    "id": str(position.id),
                    "instrument_id": str(position.instrument_id),
                    "symbol": f"{instrument.base_currency}/{instrument.quote_currency}:{instrument.settlement_currency}",
                    "side": str(position.side).split(".")[-1].lower(),
                    "contracts": str(position.quantity.as_decimal()),
                    "contract_size": str(instrument.multiplier.as_decimal()),
                    "entry_price": str(Decimal(str(position.avg_px_open))),
                }
            )
        if ledger_observer is not None:
            ledger_observer.capture_cached()
            operation_records.extend(ledger_observer.drain())
        return AccountSnapshot(balances, event.info, positions, event.ts_init // 1_000_000_000)
    except QueryError:
        raise
    except Exception:  # noqa: BLE001 - Sanitize errors from the native boundary.
        # Native errors can embed signed URLs or credentials. Never expose their text.
        raise QueryError("Binance 账户查询失败，请检查凭证、权限、环境和网络。") from None
    finally:
        waiter.cancel()
        handle.stop()
        try:
            await asyncio.wait_for(asyncio.shield(task), 6)
        except (Exception, asyncio.CancelledError):  # noqa: BLE001 - Always release the node task.
            task.cancel()
        await asyncio.gather(task, waiter, return_exceptions=True)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                _clear_exception_frames(error)
        node.dispose()
