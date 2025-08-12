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

"""Assertion helpers for pytest."""

import dataclasses
import difflib
import typing

import pyarrow
import whenever

T = typing.TypeVar("T", pyarrow.Schema, pyarrow.Table)


@dataclasses.dataclass
class NaiveTime:
    hour: int
    minute: int
    second: int
    nanosecond: int


def make_nullable(value: T) -> T:
    """Make all top-level schema fields nullable."""
    match value:
        case pyarrow.Schema():
            fields = []
            for field in value:
                if isinstance(field.type, pyarrow.StructType):
                    field_type = pyarrow.struct(
                        [
                            pyarrow.field(
                                child.name,
                                child.type,
                                nullable=True,
                                metadata=child.metadata,
                            )
                            for child in field.type.fields
                        ]
                    )
                    fields.append(
                        pyarrow.field(
                            field.name,
                            field_type,
                            nullable=True,
                            metadata=field.metadata,
                        )
                    )
                else:
                    fields.append(
                        pyarrow.field(
                            field.name,
                            field.type,
                            nullable=True,
                            metadata=field.metadata,
                        )
                    )
            return pyarrow.schema(fields)
        case pyarrow.Table():
            schema = make_nullable(value.schema)
            return pyarrow.Table.from_arrays(value.columns, schema=schema)
        case _:
            raise TypeError(f"Unsupported type: {type(value)}")


def scalar_to_py_smart(value: pyarrow.Scalar) -> typing.Any:
    """
    Convert a PyArrow Scalar to a Python object.

    Unlike `pyarrow.Scalar.as_py()`, this function aims to properly handle
    times, timestamps, and intervals.
    """

    if value is None or not value.is_valid:
        return None
    elif isinstance(value, (pyarrow.Time32Scalar, pyarrow.Time64Scalar)):
        # Needed because to_pylist uses the wrong object for times, losing precision...
        match value.type.unit:
            case "s":
                return NaiveTime(
                    hour=value.value // 3600,
                    minute=(value.value % 3600) // 60,
                    second=value.value % 60,
                    nanosecond=0,
                )
            case "ms":
                return NaiveTime(
                    hour=value.value // 3600000,
                    minute=(value.value % 3600000) // 60000,
                    second=(value.value % 60000) // 1000,
                    nanosecond=(value.value % 1000) * 1000000,
                )
            case "us":
                return NaiveTime(
                    hour=value.value // 3600000000,
                    minute=(value.value % 3600000000) // 60000000,
                    second=(value.value % 60000000) // 1000000,
                    nanosecond=(value.value % 1000000) * 1000,
                )
            case "ns":
                return NaiveTime(
                    hour=value.value // 3600000000000,
                    minute=(value.value % 3600000000000) // 60000000000,
                    second=(value.value % 60000000000) // 1000000000,
                    nanosecond=value.value % 1000000000,
                )
            case _:
                raise NotImplementedError(str(value))
    elif isinstance(value, pyarrow.TimestampScalar):
        match value.type.unit:
            case "s":
                nanos = value.value * 1_000_000_000
            case "ms":
                nanos = value.value * 1_000_000
            case "us":
                nanos = value.value * 1000
            case "ns":
                nanos = value.value

        instant = whenever.Instant.from_timestamp_nanos(nanos)
        if value.type.tz is None or value.type.tz == "":
            # A bit sketch
            naive = whenever.PlainDateTime.parse_common_iso(
                instant.format_common_iso()[:-1]
            )
            return str(naive)
        elif value.type.tz == "UTC":
            return str(instant)
        elif value.type.tz[0] in ("+", "-"):
            negative = value.type.tz[0] == "-"
            hours, _, minutes = value.type.tz[1:].partition(":")
            tz_offset = whenever.TimeDelta(hours=int(hours), minutes=int(minutes))
            if negative:
                tz_offset = -tz_offset
            offset = instant.to_fixed_offset(tz_offset)
            return str(offset)
        zoned = instant.to_tz(value.type.tz)
        return str(zoned)
    elif isinstance(value, pyarrow.MonthDayNanoIntervalScalar):
        mdn = value.as_py()
        if mdn is None:
            return None
        else:
            return f"{mdn[0]}M{mdn[1]}d{mdn[2]}ns"

    return value.as_py()


def to_pylist(table: pyarrow.Table) -> list[dict[str, typing.Any]]:
    """Convert a PyArrow Table to a list of dictionaries."""
    rows = []
    for row_idx in range(table.num_rows):
        row = {}
        for col_idx in range(table.num_columns):
            value = table.column(col_idx)[row_idx]
            col_name = table.schema[col_idx].name
            row[col_name] = scalar_to_py_smart(value)
        rows.append(row)
    return rows


def compare_fields(
    expected: pyarrow.Field, actual: pyarrow.Field, field_path: tuple[str] = ()
) -> None:
    path = ".".join(field_path)
    if field_path:
        path += "."

    # IMO, this is a design flaw in PyArrow/Arrow C++ (but is not a flaw in
    # Nanoarrow/Arrow Java): child fields are part of the _type_. So to
    # generically manipulate a field like we want to do here, we have to know
    # the type of the field.  It would help if there were accessors to list
    # the children for us based on the type, instead of having to hardcode
    # this knowledge all over the place.

    assert expected.name == actual.name, (
        f"Field names do not match: {path}{expected.name} != {path}{actual.name}"
    )
    # TODO: we should compare type.id, then manually recurse into child fields
    # and provide field_path so that we can have a nicer diff of nested types.
    # But this is all much more annoying than necessary (also because there's
    # no built in to stringify type.id)

    assert expected.type == actual.type, (
        f"Field types do not match: {path}{expected.name} ({expected.type}) != {path}{actual.name} ({actual.type})"
    )
    assert expected.nullable == actual.nullable, (
        f"Field nullability does not match: {path}{expected.name} ({expected.nullable}) != {path}{actual.name} ({actual.nullable})"
    )

    # Another design flaw in PyArrow is that there is apparently no generic
    # extension type, so an extension type that isn't registered gets
    # invisibly treated as the storage type.  Also because extension types are
    # really just metadata, they silently compare equal.  (It's a little
    # inconsistent: are extension types separate types or not?  The global
    # registry also has shades of the Protobuf issues; an explicit registry is
    # probably more annoying but would be preferable personally so that I
    # don't have to handle both treatments of extension types here.  Either I
    # want no extension types or I want all extension types, instead I have to
    # handle it both ways.)

    # There's no need to handle ExtensionType/BaseExtensionType here
    # explicitly.  They would have compared equal or unequal already.  This is
    # only to handle normal-types-with-invisible-extension-metadata.
    expected_extension_name = (expected.metadata or {}).get(
        b"ARROW:extension:name", None
    )
    expected_extension_metadata = (expected.metadata or {}).get(
        b"ARROW:extension:metadata", None
    )

    actual_extension_name = (actual.metadata or {}).get(b"ARROW:extension:name", None)
    actual_extension_metadata = (actual.metadata or {}).get(
        b"ARROW:extension:metadata", None
    )

    assert expected_extension_name == actual_extension_name, (
        f"Field extension names do not match: {path}{expected.name} ({expected_extension_name}) != {path}{actual.name} ({actual_extension_name})"
    )

    assert expected_extension_metadata == actual_extension_metadata, (
        f"Field extension metadata does not match: {path}{expected.name} ({expected_extension_metadata}) != {path}{actual.name} ({actual_extension_metadata})"
    )


def compare_schemas(
    expected: pyarrow.Schema,
    actual: pyarrow.Schema,
) -> None:
    expected = make_nullable(expected)
    actual = make_nullable(actual)
    assert len(expected) == len(actual), "Schemas do not match"

    for expected_field, actual_field in zip(expected, actual):
        compare_fields(expected_field, actual_field, ())


def compare_tables(
    expected: pyarrow.Table,
    actual: pyarrow.Table,
    meta: dict[str, typing.Any] | None = None,
):
    """Compare two Arrow tables for equality."""
    meta = meta or {}
    expected = make_nullable(expected)
    actual = make_nullable(actual)

    if sort_keys := meta.get("sort-keys", None):
        expected = expected.sort_by(sort_keys)
        actual = actual.sort_by(sort_keys)

    compare_schemas(expected.schema, actual.schema)
    if expected != actual:
        # XXX: to_pylist uses the wrong object for times, losing precision...
        expected_repr = repr(expected.schema).splitlines() + [
            repr(line) for line in to_pylist(expected)
        ]
        actual_repr = repr(actual.schema).splitlines() + [
            repr(line) for line in to_pylist(actual)
        ]
        diff = difflib.unified_diff(
            expected_repr,
            actual_repr,
            fromfile="expected",
            tofile="actual",
            lineterm="",
        )
        raise AssertionError(f"Tables do not match! Diff:\n{'\n'.join(diff)}\n")


def match_fields(
    actual: dict[str, typing.Any], expected: dict[str, typing.Any]
) -> None:
    """Check values of a subset of dictionary fields."""
    subset = {key: actual[key] for key in expected if key in actual}
    assert subset == expected
