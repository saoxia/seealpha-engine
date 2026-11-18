"""Isolated, one-shot backtest entry point, launched only by the Engine scheduler.

The worker never binds an HTTP socket, configures exchange execution clients,
or handles credentials. The scheduler owns cancellation and task state.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from engine.runtime import EngineRuntime
from engine.task_queue import TaskJournal, _validate_id, _validate_tenant

_LOG = logging.getLogger(__name__)


def run(state_root: Path, project: Path, tenant_id: str, task_id: str) -> None:
    tenant_id = _validate_tenant(tenant_id)
    _validate_id(task_id)
    runtime = EngineRuntime(state_root, project)
    try:
        record = runtime.store.task_record(tenant_id, task_id)
        tenant = runtime.store.tenant(tenant_id)
        if (record is None or record.get("kind") != "backtest"
                or record.get("status") != "RUNNING"
                or record.get("tenant_id") != tenant_id
                or tenant is None or tenant.get("purpose") != "development"
                or tenant.get("status") != "active"):
            raise RuntimeError("回测任务未获有效执行授权。")
        parameters = record["parameters"]
        result = runtime.run_backtest(
            tenant_id, parameters["account_id"],
            parameters["start"], parameters["end"], parameters["interval"],
            parameters["starting_balance"], parameters["quote_currency"],
            publish_event=False, product=parameters.get("product", "futures_um"),
        )
        if result.get("tenant_id") != tenant_id or result.get("account_id") != parameters["account_id"]:
            raise RuntimeError("回测结果归属不一致。")
        output = state_root / "tasks" / tenant_id / f"{task_id}.result.json"
        TaskJournal._atomic(output, result)
    finally:
        runtime.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="SeeAlpha isolated backtest worker")
    parser.add_argument("--state-root", type=Path, required=True)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--task-id", required=True)
    args = parser.parse_args()
    try:
        run(args.state_root, args.project, args.tenant_id, args.task_id)
    except Exception:  # noqa: BLE001 - isolate arbitrary strategy exceptions within the child
        _LOG.exception("Backtest worker aborted")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
