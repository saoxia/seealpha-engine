"""Validated order intents shared by the Platform edge and Engine HTTP boundary."""

from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

TRADING_SYMBOL = r"^[A-Z0-9]{2,30}(?:USDT|USDC)-PERP$"


class SubmitOrderValues(BaseModel):
    account_revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_id: UUID
    symbol: str = Field(pattern=TRADING_SYMBOL)
    side: Literal["buy", "sell"]
    order_type: Literal["limit", "market"]
    quantity: str = Field(pattern=r"^\d{1,16}(?:\.\d{1,18})?$", max_length=35)
    price: str | None = Field(default=None, pattern=r"^\d{1,16}(?:\.\d{1,18})?$", max_length=35)
    time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC"
    post_only: bool = False
    reduce_only: bool = False
    position_side: Literal["BOTH", "LONG", "SHORT"] | None = Field(
        default=None, description="Legacy explicit leg; new tickets use position_effect and the actual account mode."
    )
    position_effect: Literal["open", "close"] = "open"
    expected_position_mode: Literal["oneway", "hedge"] | None = None
    environment: Literal["production", "sandbox"]

    @model_validator(mode="after")
    def validate_order(self):
        if Decimal(self.quantity) <= 0:
            raise ValueError("数量必须大于零。")
        if self.order_type == "limit" and (self.price is None or Decimal(self.price) <= 0):
            raise ValueError("限价单需要有效价格。")
        if self.order_type == "market" and (
            self.price is not None or self.post_only or self.time_in_force != "GTC"
        ):
            raise ValueError("市价单不能设置限价、Post Only 或限价有效期。")
        if self.post_only and self.time_in_force != "GTC":
            raise ValueError("Post Only 仅支持 GTC。")
        if self.position_side in {"LONG", "SHORT"} and self.reduce_only:
            raise ValueError("双向持仓请按持仓方向和买卖方向平仓，不能传 Reduce Only。")
        return self


class CancelOrderValues(BaseModel):
    account_revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_id: UUID
    symbol: str = Field(pattern=TRADING_SYMBOL)
    client_order_id: str = Field(min_length=1, max_length=100, pattern=r"^[A-Za-z0-9._:/-]+$")
    environment: Literal["production", "sandbox"]



class AlgoOrderValues(BaseModel):
    """Venue-hosted USD-M conditional order, never a browser-local trigger."""

    model_config = {"extra": "forbid"}
    account_revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_id: UUID
    symbol: str = Field(pattern=TRADING_SYMBOL)
    environment: Literal["production", "sandbox"]
    side: Literal["buy", "sell"]
    order_type: Literal["STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET", "TRAILING_STOP_MARKET"]
    quantity: str = Field(pattern=r"^\d{1,16}(?:\.\d{1,18})?$")
    trigger_price: str | None = Field(default=None, pattern=r"^\d{1,16}(?:\.\d{1,18})?$")
    price: str | None = Field(default=None, pattern=r"^\d{1,16}(?:\.\d{1,18})?$")
    activation_price: str | None = Field(default=None, pattern=r"^\d{1,16}(?:\.\d{1,18})?$")
    callback_rate: str | None = Field(default=None, pattern=r"^\d{1,2}(?:\.\d{1,4})?$")
    working_type: Literal["MARK_PRICE", "CONTRACT_PRICE"] = "MARK_PRICE"
    position_effect: Literal["close"] = "close"
    expected_position_mode: Literal["oneway", "hedge"]
    price_protect: bool = False

    @model_validator(mode="after")
    def validate_algo(self):
        if Decimal(self.quantity) <= 0:
            raise ValueError("条件单数量必须大于零")
        trailing = self.order_type == "TRAILING_STOP_MARKET"
        limit = self.order_type in {"STOP", "TAKE_PROFIT"}
        if trailing:
            if self.trigger_price is not None or self.price is not None or self.callback_rate is None:
                raise ValueError("跟踪止损需要回调率且不能填写普通触发价或限价")
            if not Decimal("0.1") <= Decimal(self.callback_rate) <= Decimal("20"):
                raise ValueError("回调率范围为 0.1–20%")
            if self.activation_price is not None and Decimal(self.activation_price) <= 0:
                raise ValueError("激活价格必须大于零")
        elif (self.trigger_price is None or Decimal(self.trigger_price) <= 0
              or self.activation_price is not None or self.callback_rate is not None
              or (limit and (self.price is None or Decimal(self.price) <= 0))
              or (not limit and self.price is not None)):
            raise ValueError("条件单触发价、委托价或跟踪参数组合无效")
        if self.price_protect and self.order_type not in {"STOP_MARKET", "TAKE_PROFIT_MARKET"}:
            raise ValueError("价格保护仅适用于市价触发单")
        return self


class AlgoCancelValues(BaseModel):
    model_config = {"extra": "forbid"}
    account_revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_id: UUID
    symbol: str = Field(pattern=TRADING_SYMBOL)
    client_algo_id: str = Field(pattern=r"^[.A-Za-z0-9_:/-]{1,36}$")
    environment: Literal["production", "sandbox"]


class LeverageChangeValues(BaseModel):
    model_config = {"extra": "forbid"}
    account_revision: str = Field(pattern=r"^[a-f0-9]{64}$")
    request_id: UUID
    symbol: str = Field(pattern=TRADING_SYMBOL)
    environment: Literal["production", "sandbox"]
    leverage: int = Field(ge=1, le=125, strict=True)
