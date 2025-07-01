# Copyright (c) 2025 Columnar Technologies, Inc.
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
                            pyarrow.field(child.name, child.type, nullable=True)
                            for child in field.type.fields
                        ]
                    )
                    fields.append(pyarrow.field(field.name, field_type, nullable=True))
                else:
                    fields.append(pyarrow.field(field.name, field.type, nullable=True))
            return pyarrow.schema(fields)
        case pyarrow.Table():
            schema = make_nullable(value.schema)
            return pyarrow.Table.from_arrays(value.columns, schema=schema)
        case _:
            raise TypeError(f"Unsupported type: {type(value)}")


def to_pylist(table: pyarrow.Table) -> list[dict[str, typing.Any]]:
    """Convert a PyArrow Table to a list of dictionaries."""
    rows = []
    for row_idx in range(table.num_rows):
        row = {}
        for col_idx in range(table.num_columns):
            value = table.column(col_idx)[row_idx]
            col_name = table.schema[col_idx].name
            if value is None:
                row[col_name] = None
            elif isinstance(value, (pyarrow.Time32Scalar, pyarrow.Time64Scalar)):
                # Needed because to_pylist uses the wrong object for times, losing precision...
                match value.type.unit:
                    case "s":
                        row[col_name] = NaiveTime(
                            hour=value.value // 3600,
                            minute=(value.value % 3600) // 60,
                            second=value.value % 60,
                            nanosecond=0,
                        )
                    case "ms":
                        row[col_name] = NaiveTime(
                            hour=value.value // 3600000,
                            minute=(value.value % 3600000) // 60000,
                            second=(value.value % 60000) // 1000,
                            nanosecond=(value.value % 1000) * 1000000,
                        )
                    case "us":
                        row[col_name] = NaiveTime(
                            hour=value.value // 3600000000,
                            minute=(value.value % 3600000000) // 60000000,
                            second=(value.value % 60000000) // 1000000,
                            nanosecond=(value.value % 1000000) * 1000,
                        )
                    case "ns":
                        row[col_name] = NaiveTime(
                            hour=value.value // 3600000000000,
                            minute=(value.value % 3600000000000) // 60000000000,
                            second=(value.value % 60000000000) // 1000000000,
                            nanosecond=value.value % 1000000000,
                        )
                    case _:
                        raise NotImplementedError(str(value))
            elif isinstance(value, pyarrow.MonthDayNanoIntervalScalar):
                mdn = value.as_py()
                if mdn is None:
                    row[col_name] = None
                else:
                    row[col_name] = f"{mdn[0]}M{mdn[1]}d{mdn[2]}ns"
            else:
                row[col_name] = value.as_py()
        rows.append(row)

    return rows


def compare_tables(
    expected: pyarrow.Table, actual: pyarrow.Table, meta: dict[str, typing.Any]
):
    """Compare two Arrow tables for equality."""
    expected = make_nullable(expected)
    actual = make_nullable(actual)

    if sort_keys := meta.get("sort-keys", None):
        expected = expected.sort_by(sort_keys)
        actual = actual.sort_by(sort_keys)

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
