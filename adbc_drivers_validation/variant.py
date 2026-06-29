# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Basic arrow.parquet.variant handling for nicer comparisons.

N.B. this file was vibecoded. Eventually pyarrow will have native support and
we won't need this.
"""

import dataclasses
import struct

VARIANT_EXTENSION_NAME = "arrow.parquet.variant"


def _int_size(value: int) -> int:
    if value <= 0xFF:
        return 1
    elif value <= 0xFFFF:
        return 2
    elif value <= 0xFFFFFF:
        return 3
    elif value <= 0xFFFFFFFF:
        return 4
    else:
        raise ValueError(f"Variant offset is too large: {value}")


def _read_unsigned(data: bytes, offset: int, size: int) -> int:
    if offset + size > len(data):
        raise ValueError("Invalid Variant bytes: truncated unsigned integer")
    return int.from_bytes(data[offset : offset + size], "little")


def _write_unsigned(value: int, size: int) -> bytes:
    return value.to_bytes(size, "little")


def _primitive_header(type_id: int) -> bytes:
    return bytes([type_id << 2])


class _VariantMetadataBuilder:
    def __init__(self) -> None:
        self._ids: dict[str, int] = {}
        self._keys: list[str] = []

    def add_key(self, key: str) -> int:
        if key in self._ids:
            return self._ids[key]
        key_id = len(self._keys)
        self._ids[key] = key_id
        self._keys.append(key)
        return key_id

    def build(self) -> bytes:
        encoded_keys = [key.encode("utf-8") for key in self._keys]
        keys_size = sum(len(key) for key in encoded_keys)
        offset_size = _int_size(max(keys_size, len(encoded_keys)))

        sorted_unique = encoded_keys == sorted(encoded_keys)
        header = 1 | ((offset_size - 1) << 6)
        if encoded_keys and sorted_unique:
            header |= 1 << 4

        offsets = [0]
        for key in encoded_keys:
            offsets.append(offsets[-1] + len(key))

        chunks = [bytes([header]), _write_unsigned(len(encoded_keys), offset_size)]
        chunks.extend(_write_unsigned(offset, offset_size) for offset in offsets)
        chunks.extend(encoded_keys)
        return b"".join(chunks)


def _parse_metadata(metadata: bytes) -> list[str]:
    if len(metadata) < 3:
        raise ValueError("Invalid Variant metadata: too short")

    version = metadata[0] & 0x0F
    if version != 1:
        raise ValueError(f"Unsupported Variant metadata version: {version}")

    offset_size = ((metadata[0] >> 6) & 0x03) + 1
    minimum_size = 1 + (2 * offset_size)
    if len(metadata) < minimum_size:
        raise ValueError("Invalid Variant metadata: too short for offsets")

    dictionary_size = _read_unsigned(metadata, 1, offset_size)
    offsets_start = 1 + offset_size
    bytes_start = offsets_start + ((dictionary_size + 1) * offset_size)
    if bytes_start > len(metadata):
        raise ValueError("Invalid Variant metadata: dictionary offsets out of range")

    offsets = [
        _read_unsigned(metadata, offsets_start + (index * offset_size), offset_size)
        for index in range(dictionary_size + 1)
    ]
    string_bytes = metadata[bytes_start:]
    if offsets[0] != 0 or offsets[-1] != len(string_bytes):
        raise ValueError("Invalid Variant metadata: invalid dictionary offsets")

    keys = []
    for start, end in zip(offsets, offsets[1:]):
        if end < start or end > len(string_bytes):
            raise ValueError("Invalid Variant metadata: invalid dictionary string")
        keys.append(string_bytes[start:end].decode("utf-8"))
    return keys


class VariantValue:
    def __repr__(self) -> str:
        if not dataclasses.is_dataclass(self):
            return super().__repr__()

        fields = [
            field for field in dataclasses.fields(self) if field.repr and field.compare
        ]
        if not fields:
            return f"{type(self).__name__}()"
        values = [getattr(self, field.name) for field in fields]
        if len(values) == 1:
            return f"{type(self).__name__}({values[0]!r})"
        return f"{type(self).__name__}({', '.join(repr(value) for value in values)})"

    def to_variant_bytes(self) -> tuple[bytes, bytes]:
        metadata = _VariantMetadataBuilder()
        value = self._encode_value(metadata)
        return metadata.build(), value

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True, repr=False)
class VariantNull(VariantValue):
    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(0)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantBool(VariantValue):
    value: bool

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(1 if self.value else 2)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantInt8(VariantValue):
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(3) + struct.pack("<b", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantInt16(VariantValue):
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(4) + struct.pack("<h", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantInt32(VariantValue):
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(5) + struct.pack("<i", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantInt64(VariantValue):
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(6) + struct.pack("<q", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantDecimal4(VariantValue):
    scale: int
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return (
            _primitive_header(8) + bytes([self.scale]) + struct.pack("<i", self.value)
        )


@dataclasses.dataclass(frozen=True, repr=False)
class VariantDecimal8(VariantValue):
    scale: int
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return (
            _primitive_header(9) + bytes([self.scale]) + struct.pack("<q", self.value)
        )


@dataclasses.dataclass(frozen=True, repr=False)
class VariantDecimal16(VariantValue):
    scale: int
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return (
            _primitive_header(10)
            + bytes([self.scale])
            + self.value.to_bytes(16, "little", signed=True)
        )


@dataclasses.dataclass(frozen=True, repr=False)
class VariantFloat(VariantValue):
    value: float

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(14) + struct.pack("<f", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantDouble(VariantValue):
    value: float

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(7) + struct.pack("<d", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantDate(VariantValue):
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(11) + struct.pack("<i", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantTimestampMicros(VariantValue):
    value: int
    utc: bool = False

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(12 if self.utc else 13) + struct.pack("<q", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantTimeMicros(VariantValue):
    value: int

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(17) + struct.pack("<q", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantTimestampNanos(VariantValue):
    value: int
    utc: bool = False

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(18 if self.utc else 19) + struct.pack("<q", self.value)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantBinary(VariantValue):
    value: bytes

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        return _primitive_header(15) + struct.pack("<I", len(self.value)) + self.value


@dataclasses.dataclass(frozen=True, repr=False)
class VariantString(VariantValue):
    value: str
    force_long: bool = dataclasses.field(default=False, compare=True, repr=True)

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        encoded = self.value.encode("utf-8")
        if not self.force_long and len(encoded) <= 0x3F:
            return bytes([(len(encoded) << 2) | 1]) + encoded
        return _primitive_header(16) + struct.pack("<I", len(encoded)) + encoded


@dataclasses.dataclass(frozen=True, repr=False)
class VariantUUID(VariantValue):
    value: bytes

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        if len(self.value) != 16:
            raise ValueError("Variant UUID values must be exactly 16 bytes")
        return _primitive_header(20) + self.value


@dataclasses.dataclass(frozen=True, repr=False)
class VariantArray(VariantValue):
    values: tuple[VariantValue, ...]

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        encoded_values = [value._encode_value(metadata) for value in self.values]
        data = b"".join(encoded_values)
        offset_size = _int_size(len(data))
        is_large = len(encoded_values) > 0xFF
        count_size = 4 if is_large else 1
        value_header = ((1 if is_large else 0) << 2) | (offset_size - 1)

        offsets = [0]
        for encoded in encoded_values:
            offsets.append(offsets[-1] + len(encoded))

        chunks = [
            bytes([(value_header << 2) | 3]),
            _write_unsigned(len(encoded_values), count_size),
        ]
        chunks.extend(_write_unsigned(offset, offset_size) for offset in offsets)
        chunks.append(data)
        return b"".join(chunks)


@dataclasses.dataclass(frozen=True, repr=False)
class VariantObject(VariantValue):
    fields: tuple[tuple[str, VariantValue], ...]

    def _encode_value(self, metadata: _VariantMetadataBuilder) -> bytes:
        fields = tuple(sorted(self.fields, key=lambda item: item[0]))
        ids = [(key, metadata.add_key(key), value) for key, value in fields]
        encoded_values = [value._encode_value(metadata) for _, _, value in ids]
        data = b"".join(encoded_values)
        max_id = max((field_id for _, field_id, _ in ids), default=0)
        id_size = _int_size(max_id)
        offset_size = _int_size(len(data))
        is_large = len(fields) > 0xFF
        count_size = 4 if is_large else 1
        value_header = (
            ((1 if is_large else 0) << 4) | ((id_size - 1) << 2) | (offset_size - 1)
        )

        offsets = [0]
        for encoded in encoded_values:
            offsets.append(offsets[-1] + len(encoded))

        chunks = [
            bytes([(value_header << 2) | 2]),
            _write_unsigned(len(ids), count_size),
        ]
        chunks.extend(_write_unsigned(field_id, id_size) for _, field_id, _ in ids)
        chunks.extend(_write_unsigned(offset, offset_size) for offset in offsets)
        chunks.append(data)
        return b"".join(chunks)


def _decode_variant_value(
    metadata: list[str], value: bytes, offset: int = 0
) -> tuple[VariantValue, int]:
    if offset >= len(value):
        raise ValueError("Invalid Variant value: empty")

    value_metadata = value[offset]
    basic_type = value_metadata & 0x03
    value_header = value_metadata >> 2
    cursor = offset + 1

    if basic_type == 0:
        match value_header:
            case 0:
                return VariantNull(), cursor
            case 1:
                return VariantBool(True), cursor
            case 2:
                return VariantBool(False), cursor
            case 3:
                return VariantInt8(
                    struct.unpack_from("<b", value, cursor)[0]
                ), cursor + 1
            case 4:
                return VariantInt16(
                    struct.unpack_from("<h", value, cursor)[0]
                ), cursor + 2
            case 5:
                return VariantInt32(
                    struct.unpack_from("<i", value, cursor)[0]
                ), cursor + 4
            case 6:
                return VariantInt64(
                    struct.unpack_from("<q", value, cursor)[0]
                ), cursor + 8
            case 7:
                return VariantDouble(
                    struct.unpack_from("<d", value, cursor)[0]
                ), cursor + 8
            case 8:
                scale = value[cursor]
                number = struct.unpack_from("<i", value, cursor + 1)[0]
                return VariantDecimal4(scale, number), cursor + 5
            case 9:
                scale = value[cursor]
                number = struct.unpack_from("<q", value, cursor + 1)[0]
                return VariantDecimal8(scale, number), cursor + 9
            case 10:
                scale = value[cursor]
                number = int.from_bytes(
                    value[cursor + 1 : cursor + 17], "little", signed=True
                )
                return VariantDecimal16(scale, number), cursor + 17
            case 11:
                return VariantDate(
                    struct.unpack_from("<i", value, cursor)[0]
                ), cursor + 4
            case 12:
                return (
                    VariantTimestampMicros(
                        struct.unpack_from("<q", value, cursor)[0],
                        utc=True,
                    ),
                    cursor + 8,
                )
            case 13:
                return (
                    VariantTimestampMicros(struct.unpack_from("<q", value, cursor)[0]),
                    cursor + 8,
                )
            case 14:
                return VariantFloat(
                    struct.unpack_from("<f", value, cursor)[0]
                ), cursor + 4
            case 15:
                size = _read_unsigned(value, cursor, 4)
                start = cursor + 4
                end = start + size
                if end > len(value):
                    raise ValueError("Invalid Variant binary: length out of range")
                return VariantBinary(value[start:end]), end
            case 16:
                size = _read_unsigned(value, cursor, 4)
                start = cursor + 4
                end = start + size
                if end > len(value):
                    raise ValueError("Invalid Variant string: length out of range")
                return VariantString(
                    value[start:end].decode("utf-8"), force_long=True
                ), end
            case 17:
                return (
                    VariantTimeMicros(struct.unpack_from("<q", value, cursor)[0]),
                    cursor + 8,
                )
            case 18:
                return (
                    VariantTimestampNanos(
                        struct.unpack_from("<q", value, cursor)[0],
                        utc=True,
                    ),
                    cursor + 8,
                )
            case 19:
                return (
                    VariantTimestampNanos(struct.unpack_from("<q", value, cursor)[0]),
                    cursor + 8,
                )
            case 20:
                end = cursor + 16
                if end > len(value):
                    raise ValueError("Invalid Variant UUID: truncated value")
                return VariantUUID(value[cursor:end]), end
            case _:
                raise ValueError(f"Unsupported Variant primitive type: {value_header}")

    elif basic_type == 1:
        size = value_header
        end = cursor + size
        if end > len(value):
            raise ValueError("Invalid Variant short string: length out of range")
        return VariantString(value[cursor:end].decode("utf-8"), force_long=False), end

    elif basic_type == 2:
        offset_size = (value_header & 0x03) + 1
        id_size = ((value_header >> 2) & 0x03) + 1
        count_size = 4 if ((value_header >> 4) & 0x01) else 1
        count = _read_unsigned(value, cursor, count_size)
        cursor += count_size
        ids = [
            _read_unsigned(value, cursor + (index * id_size), id_size)
            for index in range(count)
        ]
        cursor += count * id_size
        offsets = [
            _read_unsigned(value, cursor + (index * offset_size), offset_size)
            for index in range(count + 1)
        ]
        cursor += (count + 1) * offset_size
        data_start = cursor
        fields = []
        max_end = data_start + offsets[-1]
        if max_end > len(value):
            raise ValueError("Invalid Variant object: field data out of range")
        for field_id, field_offset in zip(ids, offsets):
            if field_id >= len(metadata):
                raise ValueError("Invalid Variant object: field ID out of range")
            field_value, field_end = _decode_variant_value(
                metadata, value, data_start + field_offset
            )
            fields.append((metadata[field_id], field_value))
            max_end = max(max_end, field_end)
        return VariantObject(tuple(fields)), max_end

    elif basic_type == 3:
        offset_size = (value_header & 0x03) + 1
        count_size = 4 if ((value_header >> 2) & 0x01) else 1
        count = _read_unsigned(value, cursor, count_size)
        cursor += count_size
        offsets = [
            _read_unsigned(value, cursor + (index * offset_size), offset_size)
            for index in range(count + 1)
        ]
        cursor += (count + 1) * offset_size
        data_start = cursor
        if data_start + offsets[-1] > len(value):
            raise ValueError("Invalid Variant array: element data out of range")
        values = []
        max_end = data_start + offsets[-1]
        for item_offset in offsets[:-1]:
            item, item_end = _decode_variant_value(
                metadata, value, data_start + item_offset
            )
            values.append(item)
            max_end = max(max_end, item_end)
        return VariantArray(tuple(values)), max_end

    raise ValueError(f"Unsupported Variant basic type: {basic_type}")


def parse_variant(metadata: bytes, value: bytes) -> VariantValue:
    parsed_metadata = _parse_metadata(metadata)
    parsed_value, end = _decode_variant_value(parsed_metadata, value)
    if end != len(value):
        raise ValueError("Invalid Variant value: trailing bytes")
    return parsed_value
