"""Exercise accounting only from complete exchange settlement and cash evidence."""
from dataclasses import replace
from decimal import Decimal

import pytest

from engine.strategies.basket_settlement import (
    BasketSettlementError,
    ExpiringOptionContract,
    VerifiedOptionSettlement,
    reconcile_verified_option_expiry,
)
from engine.strategies.basket_targets import LegId

NOW = 1_800_000_000_000_000_000
EXPIRY = NOW - 1_000_000_000


def _case():
    put = LegId("development", "virtual", "options", "ETH-261225-1800-P")
    call = LegId("development", "virtual", "options", "ETH-261225-2200-C")
    contracts = (
        ExpiringOptionContract(put, EXPIRY, Decimal(1800), Decimal(1), Decimal("0.1")),
        ExpiringOptionContract(call, EXPIRY, Decimal(2200), Decimal(1), Decimal("0.1")),
    )
    quantities = {put: Decimal("0.2"), call: Decimal("0.2")}
    settlement = (
        VerifiedOptionSettlement(
            put, "exercise-put", NOW, Decimal(1700), Decimal("0.2"),
            Decimal(20), Decimal(2), Decimal(18),
        ),
        VerifiedOptionSettlement(
            call, "exercise-call", NOW, Decimal(1700), Decimal("0.2"),
            Decimal(0), Decimal(0), Decimal(0),
        ),
    )
    return contracts, quantities, settlement


def _reconcile(contracts, positions, records, **kwargs):
    return reconcile_verified_option_expiry(
        contracts, positions, records,
        perpetual_position_at_expiry=kwargs.pop("perpetual_position_at_expiry", Decimal(0)),
        account_cash_before=kwargs.pop("account_cash_before", Decimal(1000)),
        account_cash_after=kwargs.pop("account_cash_after", Decimal(1018)),
        now_ns=kwargs.pop("now_ns", NOW), **kwargs,
    )


def test_verified_long_put_cashes_out_with_actual_fee_and_balanced_usdt_ledger():
    contracts, positions, records = _case()
    report = _reconcile(contracts, positions, records)
    assert report.verified
    assert report.gross_option_credit == Decimal(20)
    assert report.exercise_fees == Decimal(2)
    assert report.net_option_cash_credit == Decimal(18)
    assert report.account_cash_after == Decimal(1018)
    assert report.event_ids == ("exercise-call", "exercise-put")


def test_expiry_does_not_fill_missing_exercise_records_or_infer_from_bars():
    contracts, positions, records = _case()
    with pytest.raises(BasketSettlementError, match="完整"):
        _reconcile(contracts, positions, records[:1])
    with pytest.raises(BasketSettlementError, match="缺少"):
        _reconcile(contracts, {contracts[0].leg: Decimal("0.2")}, records)
    with pytest.raises(BasketSettlementError, match="永续"):
        _reconcile(
            contracts, positions, records,
            perpetual_position_at_expiry=Decimal("0.1"),
        )
    with pytest.raises(BasketSettlementError, match="现金"):
        _reconcile(contracts, positions, records, account_cash_after=Decimal(1019))


def test_wrong_fee_price_source_quantity_or_duplicate_event_rejected():
    contracts, positions, records = _case()
    variants = (
        (replace(records[0], record_source="candle_close_estimate"), "来源"),
        (replace(records[0], settlement_index_price=Decimal(1750)), "交割价"),
        (replace(records[0], gross_credit=Decimal(10)), "总额"),
        (replace(records[0], exercise_fee=Decimal(21)), "费用"),
        (replace(records[0], net_credit=Decimal(17)), "费用"),
        (replace(records[0], settled_quantity=Decimal("0.3")), "数量"),
        (replace(records[0], settlement_id=records[1].settlement_id), "身份"),
    )
    for bad, error in variants:
        with pytest.raises(BasketSettlementError, match=error):
            _reconcile(contracts, positions, (bad, records[1]))


def test_mixed_account_missing_unit_or_wrong_expiry_rejected():
    contracts, positions, records = _case()
    foreign = replace(
        contracts[1],
        leg=LegId("development", "other", "options", "ETH-261225-2200-C"),
    )
    with pytest.raises(BasketSettlementError, match="多个账户"):
        _reconcile((contracts[0], foreign), positions, records)
    with pytest.raises(BasketSettlementError, match="乘数"):
        _reconcile(
            (replace(contracts[0], unit=Decimal(0)), contracts[1]), positions, records,
        )
    with pytest.raises(BasketSettlementError, match="到期"):
        _reconcile(
            (replace(contracts[0], expiry_ns=NOW + 100), contracts[1]),
            positions, records,
        )
    with pytest.raises(BasketSettlementError, match="最小步长"):
        _reconcile(contracts, {**positions, contracts[0].leg: Decimal("0.25")}, records)


def test_unevidenced_nonzero_option_cash_transfer_is_rejected():
    contracts, positions, records = _case()
    with pytest.raises(BasketSettlementError, match="现金"):
        _reconcile(contracts, positions, records, account_cash_after=Decimal(1020))
