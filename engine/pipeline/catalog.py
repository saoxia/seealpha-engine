"""Project storage boundary over the NautilusTrader v2 public catalog API.

Filename/layout helpers belong to this project rather than upstream private
Python methods. Native data is written and decoded by the Rust catalog.
"""

import re
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
from nautilus_trader.model import (
    Bar,
    BarType,
    NautilusDataType,
    TradeTick,
    register_custom_data_class,
)
from nautilus_trader.model.custom import customdataclass
from nautilus_trader.persistence import (
    BarDataWrangler as NativeBarWrangler,
)
from nautilus_trader.persistence import (
    ParquetDataCatalog as NativeCatalog,
)
from nautilus_trader.persistence import (
    TradeTickDataWrangler as NativeTradeWrangler,
)


@customdataclass(kw_only=True)
class TradeQuote:
    """Raw quote quantity cannot be reconstructed from rounded price and size."""
    trade_id: str
    quote_quantity: float


register_custom_data_class(TradeQuote)

def urisafe_identifier(identifier):
    return str(identifier).replace("/", "")


def _timestamps_to_filename(first, last):
    def formatted(value):
        ts = pd.Timestamp(value, unit="ns", tz="UTC")
        return ts.strftime("%Y-%m-%dT%H-%M-%S") + f"-{value % 1_000_000_000:09d}Z"
    return f"{formatted(first)}_{formatted(last)}.parquet"


def _parse_filename_timestamps(filename):
    pattern = r"(\d{4}-\d\d-\d\d)T(\d\d)-(\d\d)-(\d\d)-(\d{9})Z"
    match = re.fullmatch(pattern + "_" + pattern + r"\.parquet", Path(filename).name)
    if match is None:
        return None
    parts = match.groups()
    return tuple(
        pd.Timestamp(f"{parts[i]}T{parts[i+1]}:{parts[i+2]}:{parts[i+3]}.{parts[i+4]}Z").value
        for i in (0, 5)
    )


def _nanos(value):
    return None if value is None else (value if isinstance(value, int) else pd.Timestamp(value).value)


class ParquetDataCatalog:
    """Local catalog with the stable operations used by the project's pipelines."""

    def __init__(self, path, fs_protocol="file", **kwargs):
        if fs_protocol != "file":
            raise ValueError("This project's catalog requires a local mounted filesystem")
        self.path = str(Path(path).resolve())
        self.fs_protocol = fs_protocol
        if any((Path(self.path) / "data" / name).exists() for name in ("bar", "trade_tick")):
            raise ValueError("Legacy v1 catalog detected; rebuild into data/catalog from raw archives")
        Path(self.path).mkdir(parents=True, exist_ok=True)
        self.native = NativeCatalog(self.path, **kwargs)

    def _make_path(self, data_cls, identifier=None):
        name = {Bar: "bars", TradeTick: "trades"}.get(data_cls)
        if name is None:
            if identifier is None:
                return str(Path(self.path) / "features")
            return str(Path(self.path) / "features" / urisafe_identifier(identifier)
                       / "data" / "custom" / data_cls.__name__)
        root = Path(self.path) / "data" / name
        return str(root if identifier is None else root / urisafe_identifier(identifier))

    def write_data(self, data):
        if not data:
            return
        first = data[0]
        if isinstance(first, Bar):
            return self.native.write_bars(data)
        if isinstance(first, TradeTick):
            return self.native.write_trade_ticks(data)
        return self.native.write_instruments(data)

    def instruments(self, instrument_ids=None, **kwargs):
        return self.native.instruments(instrument_ids=instrument_ids, **kwargs)

    def bars(self, bar_types=None, start=None, end=None, **kwargs):
        return self.native.query_bars(identifiers=bar_types, start=_nanos(start), end=_nanos(end), **kwargs)

    def trade_ticks(self, instrument_ids=None, start=None, end=None, **kwargs):
        return self.native.query_trade_ticks(
            identifiers=instrument_ids, start=_nanos(start), end=_nanos(end), **kwargs
        )

    def get_intervals(self, data_cls, identifier=None):
        data_type = {Bar: NautilusDataType.Bar, TradeTick: NautilusDataType.TradeTick}[data_cls]
        return self.native.get_intervals(data_type, identifier)

    def bar_batches(self, bar_type, start, end, batch_size=50_000):
        """Decode bounded native Arrow batches, with an exclusive end boundary."""
        wrangler = None
        reader = self.native.query_data_arrow_stream(
            NautilusDataType.Bar, [str(bar_type)], _nanos(start), _nanos(end) - 1,
            display=False,
        )
        reader = pa.RecordBatchReader._import_from_c_capsule(reader)
        for batch in reader:
            if wrangler is None:
                metadata = batch.schema.metadata
                wrangler = NativeBarWrangler(str(bar_type), int(metadata[b"price_precision"]),
                                             int(metadata[b"size_precision"]))
            for offset in range(0, batch.num_rows, batch_size):
                yield wrangler.process_record_batch_bytes(_ipc(batch.slice(offset, batch_size)))

    def delete_data_range(self, data_cls, identifier=None, start=None, end=None):
        data_type = {Bar: NautilusDataType.Bar, TradeTick: NautilusDataType.TradeTick}[data_cls]
        return self.native.delete_data_range(data_type, identifier, _nanos(start), _nanos(end))

    def supplement_catalog(self, instrument_id):
        root = Path(self.path) / "supplements" / urisafe_identifier(instrument_id)
        root.mkdir(parents=True, exist_ok=True)
        return NativeCatalog(str(root))

    def feature_catalog(self, identifier):
        root = Path(self.path) / "features" / urisafe_identifier(identifier)
        root.mkdir(parents=True, exist_ok=True)
        return NativeCatalog(str(root))

    def custom_data(self, data_cls, instrument_ids=None, start=None, end=None):
        if instrument_ids is None:
            root = Path(self.path) / "features"
            instrument_ids = [p.name for p in root.iterdir() if p.is_dir()] if root.exists() else []
        records = []
        for identifier in instrument_ids:
            records.extend(self.feature_catalog(identifier).query_custom_data(
                NautilusDataType.Custom(data_cls.__name__), start=_nanos(start), end=_nanos(end),
            ))
        return sorted(records, key=lambda row: row.data.ts_event)


def _ipc(batch):
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)
    return sink.getvalue().to_pybytes()


def _decimal(values, precision):
    array = pa.array(values)
    if pa.types.is_floating(array.type):
        array = array.cast(pa.string())
    decimal = array.cast(pa.decimal128(38, 16))
    return pc.round(decimal, ndigits=precision, round_mode="half_to_even")


class BarDataWrangler:
    """Normalize columns once and decode model objects in the native Arrow wrangler."""

    def __init__(self, bar_type, instrument):
        self.bar_type = BarType.from_str(str(bar_type))
        self.instrument = instrument
        self.native = NativeBarWrangler(str(bar_type), instrument.price_precision,
                                        instrument.size_precision)

    def process(self, frame):
        if frame.empty:
            return []
        pp, sp = self.instrument.price_precision, self.instrument.size_precision
        names = ["open", "high", "low", "close", "volume", "ts_event", "ts_init"]
        arrays = [_decimal(frame[name], sp if name == "volume" else pp) for name in names[:5]]
        timestamp = pa.array(frame.index).cast(pa.timestamp("ns", tz="UTC"))
        metadata = {b"bar_type":str(self.bar_type).encode(),
                    b"instrument_id":str(self.instrument.id).encode(),
                    b"price_precision":str(pp).encode(), b"size_precision":str(sp).encode(),
                    b"type_name":b"Bar"}
        batch = pa.RecordBatch.from_arrays([*arrays, timestamp, timestamp], names=names)
        return self.native.process_record_batch_bytes(_ipc(batch.replace_schema_metadata(metadata)))


class TradeTickDataWrangler:
    """Arrow normalization followed by native trade decoding."""

    def __init__(self, instrument):
        self.instrument = instrument
        self.native = NativeTradeWrangler(str(instrument.id), instrument.price_precision,
                                          instrument.size_precision)

    def process(self, frame):
        if frame.empty:
            return []
        inst = self.instrument
        timestamp = pa.array(frame.index).cast(pa.timestamp("ns", tz="UTC"))
        side = pa.array(frame.side).dictionary_encode().cast(pa.dictionary(pa.int8(), pa.string()))
        batch = pa.RecordBatch.from_arrays([
            _decimal(frame.price, inst.price_precision),
            _decimal(frame.quantity, inst.size_precision), side,
            pa.array(frame.trade_id.astype(str), type=pa.string()), timestamp, timestamp,
        ], names=["price", "size", "aggressor_side", "trade_id", "ts_event", "ts_init"])
        metadata = {b"instrument_id":str(inst.id).encode(),
                    b"price_precision":str(inst.price_precision).encode(),
                    b"size_precision":str(inst.size_precision).encode(), b"type_name":b"TradeTick"}
        return self.native.process_record_batch_bytes(_ipc(batch.replace_schema_metadata(metadata)))
