"""Pure immutable venue-evidence projection contract for options ledger."""
import hashlib
import json
from decimal import Decimal, localcontext


def normalize_option_record(row, account, kind):
    """Project validated read-only EAPI evidence into the existing ledger contract."""
    record = {"account_id": account["id"], "account_name": account["account_name"],
              "exchange": account["exchange"], "environment": account["environment"],
              "symbol": row.get("symbol") or "", "evidence": "venue", "strategy_id": None}
    with localcontext() as context:
        context.prec = 220
        if kind == "cashflow":
            record.update(event_type="cashflow", timestamp=row["createDate"], amount=row["amount"],
                          asset=row["asset"], income_type="OPTIONS_" + row["type"], transaction_id=str(row["id"]))
        elif kind in {"fill", "order"}:
            status = "filled" if kind == "fill" else row["status"].lower()
            if status == "cancelled":
                status = "canceled"
            if status not in {"filled", "accepted", "new", "partially_filled", "canceled", "rejected", "expired"}:
                raise ValueError("期权订单状态未知")
            event_type = "fill" if kind == "fill" else {
                "canceled": "cancel", "rejected": "reject", "expired": "expire",
            }.get(status, "place")
            record.update(event_type=event_type, status=status, order_id=str(row["orderId"]),
                          timestamp=row["time"] if kind == "fill" else row["updateTime"],
                          side=row["side"].lower(), order_type=row["type"].lower(),
                          amount=row["quantity"], price=row["price"],
                          filled=row["quantity"] if kind == "fill" else row["executedQty"],
                          cost=format(Decimal(row["quantity"]) * Decimal(row["price"]), "f"))
            if kind == "fill":
                record.update(trade_id=str(row["tradeId"]), fee_cost=format(-Decimal(row["fee"]), "f"),
                              fee_currency=row["quoteAsset"])
            else:
                record["client_order_id"] = row.get("clientOrderId") or None
        else:
            raise ValueError("不支持的期权操作证据")
    record["event_id"] = "options:" + hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
    return record


