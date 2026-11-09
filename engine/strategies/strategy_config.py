"""Project strategy parameters backed by the NautilusTrader v2 native config.

The annotation-based parameter declarations are project-owned. The native base
continues to own runtime fields and is passed directly to the Rust strategy.
"""

import json
from typing import Annotated, ClassVar, get_args, get_origin, get_type_hints

from nautilus_trader.trading import StrategyConfig as NativeStrategyConfig

PositiveInt = Annotated[int, "positive"]
PositiveFloat = Annotated[float, "positive"]


class StrategyConfig(NativeStrategyConfig):
    parameter_names: ClassVar[tuple[str, ...]] = ()

    def __init_subclass__(cls, frozen=True, kw_only=True, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.parameter_names = tuple(
            dict.fromkeys(
                name
                for base in reversed(cls.__mro__)
                for name in getattr(base, "__annotations__", {})
                if name != "parameter_names"
            )
        )

    def __new__(cls, **kwargs):
        native_fields = {
            name for name, value in NativeStrategyConfig.__dict__.items()
            if isinstance(value, property) or type(value).__name__ == "getset_descriptor"
        }
        from nautilus_trader.model import InstrumentId, OmsType, StrategyId, TimeInForce
        native_values = {name: kwargs[name] for name in native_fields if name in kwargs}
        for name in native_fields & set(cls.parameter_names):
            if name not in native_values and hasattr(cls, name):
                native_values[name] = getattr(cls, name)
        for name, value_type in (("strategy_id", StrategyId), ("oms_type", OmsType),
                          ("market_exit_time_in_force", TimeInForce)):
            if isinstance(native_values.get(name), str):
                native_values[name] = value_type.from_str(native_values[name])
        if native_values.get("external_order_instrument_ids") is not None:
            native_values["external_order_instrument_ids"] = [
                InstrumentId.from_str(item) if isinstance(item, str) else item
                for item in native_values["external_order_instrument_ids"]
            ]
        return NativeStrategyConfig.__new__(cls, **native_values)

    def __init__(self, **kwargs):
        native_fields = {
            name for name, value in NativeStrategyConfig.__dict__.items()
            if isinstance(value, property) or type(value).__name__ == "getset_descriptor"
        }
        unknown = kwargs.keys() - set(self.parameter_names) - native_fields
        if unknown:
            raise TypeError(f"Unknown strategy parameters: {sorted(unknown)}")
        super().__init__()
        for name in self.parameter_names:
            if name in kwargs:
                value = kwargs[name]
            elif hasattr(type(self), name):
                value = getattr(type(self), name)
            else:
                raise TypeError(f"Missing required strategy parameter: {name}")
            object.__setattr__(self, name, value)
        object.__setattr__(self, "_parameters_frozen", True)

    def __setattr__(self, name, value):
        if getattr(self, "_parameters_frozen", False) and name in self.parameter_names:
            raise AttributeError(f"Strategy parameters are immutable: {name}")
        super().__setattr__(name, value)

    def json(self):
        values = {name: getattr(self, name) for name in self.parameter_names}
        for name in NativeStrategyConfig.__dict__:
            if not name.startswith("_"):
                value = getattr(self, name, None)
                if not callable(value):
                    values[name] = value
        return json.dumps(values, default=str, allow_nan=False)

    def __eq__(self, other):
        return type(self) is type(other) and self.json() == other.json()

    @classmethod
    def parse(cls, payload):
        from nautilus_trader.model import BarType, InstrumentId
        values = json.loads(payload)
        for name, hint in get_type_hints(cls, include_extras=True).items():
            if name not in values:
                continue
            if hint is InstrumentId:
                values[name] = InstrumentId.from_str(values[name])
            elif hint is BarType:
                values[name] = BarType.from_str(values[name])
            elif (get_origin(hint) is Annotated and "positive" in get_args(hint)[1:]
                  and (isinstance(values[name], bool) or values[name] <= 0)):
                raise ValueError(f"{name} must be positive")
        return cls(**values)
