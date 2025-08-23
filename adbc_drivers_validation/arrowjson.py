# Copyright (c) 2025 ADBC Drivers Contributors
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

import base64
import json
import re
import typing

import pyarrow

month_day_nano_regexp = re.compile(r"(-?\d+)M(-?\d+)d(-?\d+)ns")


def load_schema(source: typing.TextIO) -> pyarrow.Schema:
    doc = json.load(source)
    return schema_from_dict(doc)


def load_table(source: typing.TextIO, schema: pyarrow.Schema) -> pyarrow.Table:
    rows = [json.loads(line) for line in source]
    return table_from_rows(rows, schema)


def array_from_values(values: list, field: pyarrow.Field) -> pyarrow.Array:
    if not field.nullable and any(value is None for value in values):
        raise ValueError(
            f"Field '{field.name}' is not nullable but contains None values"
        )

    if pyarrow.types.is_binary(field.type) or pyarrow.types.is_large_binary(field.type):
        values = [
            base64.b64decode(value) if value is not None else None for value in values
        ]
    elif pyarrow.types.is_decimal(field.type):
        return pyarrow.array(
            [str(v) if v is not None else None for v in values], type=pyarrow.string()
        ).cast(field.type)
    elif field.type.id == 37:
        # month_day_nano
        # format: 0M0d0ns
        parsed_values = []
        for value in values:
            if value is None:
                parsed_values.append(None)
            else:
                match = month_day_nano_regexp.match(value)
                if not match:
                    raise ValueError(f"Invalid month_day_nano value: {value}")
                months, days, nanos = map(int, match.groups())
                parsed_values.append(pyarrow.MonthDayNano([months, days, nanos]))
        values = parsed_values
    # XXX(https://github.com/apache/arrow/issues/47123)
    elif isinstance(field.type, (pyarrow.ExtensionType, pyarrow.BaseExtensionType)):
        storage_type = field.type.storage_type
        arr = array_from_values(
            values, pyarrow.field(field.name, storage_type, field.nullable)
        )
        return field.type.wrap_array(arr)
    elif isinstance(field.type, pyarrow.ListType):
        # We have to synthesize it manually because of extension types
        offsets = [0]
        backing = []
        mask = []
        for value in values:
            if value is None:
                offsets.append(offsets[-1])
                mask.append(True)
            else:
                backing.extend(value)
                offsets.append(offsets[-1] + len(value))
                mask.append(False)
        backing = array_from_values(backing, field.type.value_field)
        return pyarrow.ListArray.from_arrays(
            pyarrow.array(offsets, type=pyarrow.int32()),
            backing,
            type=field.type,
            mask=pyarrow.array(mask, type=pyarrow.bool_()) if field.nullable else None,
        )
    elif isinstance(field.type, pyarrow.StructType):
        children = structlike_from_rows(values, field.type.fields)
        return pyarrow.StructArray.from_arrays(children, type=field.type)
    return pyarrow.array(values, type=field.type)


def structlike_from_rows(
    rows: list[dict], fields: list[pyarrow.Field]
) -> list[pyarrow.Array]:
    cols = []
    for field in fields:
        values = [row.pop(field.name, None) for row in rows]
        array = array_from_values(values, field)
        cols.append(array)

    for row in rows:
        if row:
            raise ValueError(f"Extra fields in row: {row}")

    return cols


def table_from_rows(rows: list[dict], schema: pyarrow.Schema) -> pyarrow.Table:
    cols = structlike_from_rows(rows, list(schema))
    table = pyarrow.Table.from_arrays(cols, schema=schema)
    return table


def schema_from_dict(source: dict) -> pyarrow.Schema:
    field = field_from_dict(source)
    if not isinstance(field.type, pyarrow.StructType):
        raise ValueError("Top-level field must be a struct")
    return pyarrow.schema(field.type.fields)


def field_from_dict(source: dict) -> pyarrow.Field:
    """Create a PyArrow Field from a dictionary representation.

    Parameters
    ----------
    source : dict
        Dictionary containing field specification.
        Must have a 'format' key, may have 'name', 'flags', and 'children' keys.

    Returns
    -------
    pyarrow.Field
        The constructed field from the dictionary specification.
    """
    name = source.get("name", "")
    type_format = source["format"]
    flags = source.get("flags", [])
    children = source.get("children", None)
    metadata = source.get("metadata", None)

    # Must go bottom-up because PyArrow nests child fields into the types
    child_fields = None
    if children is not None:
        child_fields = [field_from_dict(child) for child in children]

    data_type = parse_type_format(type_format, child_fields)
    nullable = "nullable" in flags

    # Note that we treat extension types as regular types with metadata and
    # not their "real" type for convenience. This breaks PyArrow since (of
    # course) extension types don't compare equal to their "physical"
    # representation as they are treated as distinct types (even though they
    # aren't quite types). To fix this, round trip through IPC to "restore"
    # things.
    field = pyarrow.field(name, data_type, nullable, metadata)
    schema = pyarrow.schema([field])
    return pyarrow.ipc.read_schema(schema.serialize())[0]


def parse_type_format(
    type_format: str, children: list[pyarrow.Field] | None = None
) -> pyarrow.DataType:
    """Parse an Arrow format type string into a PyArrow DataType.

    Parameters
    ----------
    type_format : str
        The format string to parse.
    children : list of pyarrow.Field, optional
        List of child fields for nested types.

    Returns
    -------
    pyarrow.DataType
        A PyArrow DataType corresponding to the format string.

    Notes
    -----
    Format strings follow the Arrow C Data Interface specification:
    https://arrow.apache.org/docs/format/CDataInterface.html#data-type-description-format-strings
    """
    if not type_format:
        raise ValueError("Empty type format string")

    # Nested Types
    if type_format.startswith("+"):
        nested_type = type_format[1] if len(type_format) > 1 else ""
        children = children or []

        if nested_type == "s":  # Struct
            return pyarrow.struct(children)

        elif nested_type == "l":  # List
            if len(children) != 1:
                raise ValueError("List must have exactly one child")
            return pyarrow.list_(children[0])

        elif nested_type == "L":  # Large List
            if len(children) != 1:
                raise ValueError("Large List must have exactly one child")
            return pyarrow.large_list(children[0])

        elif nested_type == "vl":  # List View
            if len(children) != 1:
                raise ValueError("List View must have exactly one child")
            return pyarrow.list_view(children[0])

        elif nested_type == "vL":  # Large List View
            if len(children) != 1:
                raise ValueError("Large List View must have exactly one child")
            return pyarrow.large_list_view(children[0])

        elif type_format.startswith("+w:"):  # Fixed Size List
            if len(children) != 1:
                raise ValueError("Fixed Size List must have exactly one child")
            size = int(type_format[3:])
            return pyarrow.list_(children[0], size)

        elif nested_type == "m":  # Map
            if len(children) != 2:
                raise ValueError("Map must have exactly two children")
            key_type, item_type = children
            return pyarrow.map_(key_type, item_type)

        elif type_format.startswith("+ud:") or type_format.startswith("+us:"):  # Union
            mode = "dense" if type_format[2] == "d" else "sparse"

            type_ids = [int(id_) for id_ in type_format[4:].split(",")]
            if len(type_ids) != len(children):
                # Default to sequential IDs if mismatch
                type_ids = list(range(len(children)))
            return pyarrow.union(children, type_ids, mode)

        elif nested_type == "r":  # Run-End Encoded
            if len(children) != 2:
                raise ValueError("Run-End Encoded must have exactly two children")
            run_end_type, value_type = children
            return pyarrow.run_end_encoded(run_end_type, value_type)

        else:
            raise ValueError(f"Unknown nested type with format '{type_format}'")

    if children:
        raise ValueError(f"Children are not allowed for type format '{type_format}'")

    # Handle primitive types with single character codes
    if len(type_format) == 1:
        if type_format == "n":
            return pyarrow.null()
        elif type_format == "b":
            return pyarrow.bool_()
        elif type_format == "c":
            return pyarrow.int8()
        elif type_format == "C":
            return pyarrow.uint8()
        elif type_format == "s":
            return pyarrow.int16()
        elif type_format == "S":
            return pyarrow.uint16()
        elif type_format == "i":
            return pyarrow.int32()
        elif type_format == "I":
            return pyarrow.uint32()
        elif type_format == "l":
            return pyarrow.int64()
        elif type_format == "L":
            return pyarrow.uint64()
        elif type_format == "e":
            return pyarrow.float16()
        elif type_format == "f":
            return pyarrow.float32()
        elif type_format == "g":
            return pyarrow.float64()

    # Variable-Length Types
    if type_format == "z":
        return pyarrow.binary()
    elif type_format == "Z":
        return pyarrow.large_binary()
    elif type_format == "vz":
        return pyarrow.binary_view()
    elif type_format == "u":
        return pyarrow.string()
    elif type_format == "U":
        return pyarrow.large_string()
    elif type_format == "vu":
        return pyarrow.string_view()

    # Fixed Width Binary
    if type_format.startswith("w:"):
        width = int(type_format[2:])
        return pyarrow.fixed_size_binary(width)

    # Decimal
    if type_format.startswith("d:"):
        parts = [int(part) for part in type_format[2:].split(",")]

        if len(parts) == 3:
            # New Arrow C Data Interface format: d:precision,scale,bitwidth
            precision, scale, bitwidth = parts
            if bitwidth == 32:
                return pyarrow.decimal32(precision, scale)
            elif bitwidth == 64:
                return pyarrow.decimal64(precision, scale)
            elif bitwidth == 128:
                return pyarrow.decimal128(precision, scale)
            elif bitwidth == 256:
                return pyarrow.decimal256(precision, scale)
            else:
                raise ValueError(f"Unsupported decimal bitwidth: {bitwidth}")

        elif len(parts) == 2:
            # Backward compatibility: d:precision,scale -> decimal128 (legacy behavior.)
            # d:10,2 will still work and will be mapped to decimal128
            precision, scale = parts
            return pyarrow.decimal128(precision, scale)

        else:
            raise ValueError(f"Invalid decimal format: {type_format}")

    # Temporal Types
    if type_format.startswith("t"):
        if type_format.startswith("td"):  # Date
            unit = type_format[2]
            if unit == "D":
                return pyarrow.date32()
            elif unit == "m":
                return pyarrow.date64()

        elif type_format.startswith("tt"):  # Time
            unit = type_format[2]
            if unit == "s":
                return pyarrow.time32("s")
            elif unit == "m":
                return pyarrow.time32("ms")
            elif unit == "u":
                return pyarrow.time64("us")
            elif unit == "n":
                return pyarrow.time64("ns")

        elif type_format.startswith("ts"):  # Timestamp
            unit = type_format[2]
            timezone = None
            if ":" in type_format:
                timezone = type_format.split(":", 1)[1]

            if unit == "s":
                return pyarrow.timestamp("s", timezone)
            elif unit == "m":
                return pyarrow.timestamp("ms", timezone)
            elif unit == "u":
                return pyarrow.timestamp("us", timezone)
            elif unit == "n":
                return pyarrow.timestamp("ns", timezone)

        elif type_format.startswith("tD"):  # Duration
            unit = type_format[2]
            if unit == "s":
                return pyarrow.duration("s")
            elif unit == "m":
                return pyarrow.duration("ms")
            elif unit == "u":
                return pyarrow.duration("us")
            elif unit == "n":
                return pyarrow.duration("ns")

        elif type_format.startswith("ti"):  # Interval
            unit = type_format[2]
            if unit == "M" or unit == "D":
                raise NotImplementedError("PyArrow doesn't actually support this type")
            elif unit == "n":
                return pyarrow.month_day_nano_interval()

    raise ValueError(f"Unknown type format: {type_format}")
