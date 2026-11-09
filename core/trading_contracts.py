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
