"""Small protobuf wire helpers for Modal compatibility.

The Modal client repo in this workspace contains ``api.proto`` but not generated
Python modules. These helpers avoid coupling the API image to generated Modal SDK
code while still letting the compatibility gateway parse and emit the few proto
messages needed for sandbox/image flows.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterable

WIRE_VARINT = 0
WIRE_FIXED64 = 1
WIRE_LEN = 2
WIRE_FIXED32 = 5


@dataclass(frozen=True)
class PBValue:
    wire_type: int
    value: int | bytes


class RawMessage:
    def __init__(self, data: bytes = b"") -> None:
        self.data = bytes(data or b"")

    @classmethod
    def FromString(cls, data: bytes) -> "RawMessage":
        return cls(data)

    def SerializeToString(self) -> bytes:
        return self.data


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    value = 0
    while pos < len(data):
        b = data[pos]
        pos += 1
        value |= (b & 0x7F) << shift
        if not b & 0x80:
            return value, pos
        shift += 7
        if shift > 70:
            break
    raise ValueError("invalid protobuf varint")


def fields(data: bytes) -> dict[int, list[PBValue]]:
    out: dict[int, list[PBValue]] = {}
    pos = 0
    size = len(data or b"")
    while pos < size:
        key, pos = _read_varint(data, pos)
        field_no = key >> 3
        wire_type = key & 0x7
        if field_no <= 0:
            raise ValueError("invalid protobuf field number")
        if wire_type == WIRE_VARINT:
            value, pos = _read_varint(data, pos)
            item = PBValue(wire_type, value)
        elif wire_type == WIRE_LEN:
            length, pos = _read_varint(data, pos)
            end = pos + length
            if end > size:
                raise ValueError("protobuf length-delimited field exceeds buffer")
            item = PBValue(wire_type, data[pos:end])
            pos = end
        elif wire_type == WIRE_FIXED32:
            end = pos + 4
            if end > size:
                raise ValueError("protobuf fixed32 field exceeds buffer")
            item = PBValue(wire_type, data[pos:end])
            pos = end
        elif wire_type == WIRE_FIXED64:
            end = pos + 8
            if end > size:
                raise ValueError("protobuf fixed64 field exceeds buffer")
            item = PBValue(wire_type, data[pos:end])
            pos = end
        else:
            raise ValueError(f"unsupported protobuf wire type {wire_type}")
        out.setdefault(field_no, []).append(item)
    return out


def messages(items: dict[int, list[PBValue]], field_no: int) -> list[bytes]:
    return [
        bytes(v.value)
        for v in items.get(field_no, [])
        if v.wire_type == WIRE_LEN and isinstance(v.value, (bytes, bytearray))
    ]


def strings(items: dict[int, list[PBValue]], field_no: int) -> list[str]:
    out: list[str] = []
    for raw in messages(items, field_no):
        try:
            out.append(raw.decode("utf-8"))
        except UnicodeDecodeError:
            out.append("")
    return out


def string(items: dict[int, list[PBValue]], field_no: int, default: str = "") -> str:
    vals = strings(items, field_no)
    return vals[-1] if vals else default


def bytes_value(items: dict[int, list[PBValue]], field_no: int, default: bytes = b"") -> bytes:
    vals = messages(items, field_no)
    return vals[-1] if vals else default


def integer(items: dict[int, list[PBValue]], field_no: int, default: int = 0) -> int:
    vals = [v.value for v in items.get(field_no, []) if v.wire_type == WIRE_VARINT]
    return int(vals[-1]) if vals else default


def boolean(items: dict[int, list[PBValue]], field_no: int, default: bool = False) -> bool:
    vals = [v.value for v in items.get(field_no, []) if v.wire_type == WIRE_VARINT]
    return bool(vals[-1]) if vals else default


def float32(items: dict[int, list[PBValue]], field_no: int, default: float = 0.0) -> float:
    vals = [v.value for v in items.get(field_no, []) if v.wire_type == WIRE_FIXED32]
    if not vals:
        return default
    return float(struct.unpack("<f", bytes(vals[-1]))[0])


def double(items: dict[int, list[PBValue]], field_no: int, default: float = 0.0) -> float:
    vals = [v.value for v in items.get(field_no, []) if v.wire_type == WIRE_FIXED64]
    if not vals:
        return default
    return float(struct.unpack("<d", bytes(vals[-1]))[0])


def string_map(items: dict[int, list[PBValue]], field_no: int) -> dict[str, str]:
    out: dict[str, str] = {}
    for msg in messages(items, field_no):
        entry = fields(msg)
        key = string(entry, 1)
        if key:
            out[key] = string(entry, 2)
    return out


def enc_varint(value: int) -> bytes:
    v = int(value)
    if v < 0:
        v += 1 << 64
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def key(field_no: int, wire_type: int) -> bytes:
    return enc_varint((int(field_no) << 3) | int(wire_type))


def f_int(field_no: int, value: int | None) -> bytes:
    if value is None:
        return b""
    return key(field_no, WIRE_VARINT) + enc_varint(int(value))


def f_bool(field_no: int, value: bool | None) -> bytes:
    if value is None:
        return b""
    return f_int(field_no, 1 if value else 0)


def f_string(field_no: int, value: str | None) -> bytes:
    raw = str(value or "").encode("utf-8")
    if not raw:
        return b""
    return key(field_no, WIRE_LEN) + enc_varint(len(raw)) + raw


def f_bytes(field_no: int, value: bytes | bytearray | None) -> bytes:
    raw = bytes(value or b"")
    if not raw:
        return b""
    return key(field_no, WIRE_LEN) + enc_varint(len(raw)) + raw


def f_message(field_no: int, value: bytes | bytearray | None) -> bytes:
    return f_bytes(field_no, value)


def f_float(field_no: int, value: float | None) -> bytes:
    if value is None:
        return b""
    return key(field_no, WIRE_FIXED32) + struct.pack("<f", float(value))


def f_double(field_no: int, value: float | None) -> bytes:
    if value is None:
        return b""
    return key(field_no, WIRE_FIXED64) + struct.pack("<d", float(value))


def join(parts: Iterable[bytes]) -> bytes:
    return b"".join(p for p in parts if p)
