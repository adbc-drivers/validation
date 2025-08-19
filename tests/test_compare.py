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

import typing

import pyarrow
import pytest

import adbc_drivers_validation.compare as compare


def test_compare_fields():
    f1 = pyarrow.field("a", pyarrow.int32(), nullable=True)
    f2 = pyarrow.field("b", pyarrow.int32(), nullable=True)
    f3 = pyarrow.field("a", pyarrow.int64(), nullable=True)
    f4 = pyarrow.field("a", pyarrow.int32(), nullable=False)
    f5 = pyarrow.field(
        "a",
        pyarrow.int32(),
        nullable=True,
        metadata={"key": "value"},
    )
    f6 = pyarrow.field(
        "a",
        pyarrow.int32(),
        nullable=True,
        metadata={"ARROW:extension:name": "value"},
    )
    f6 = pyarrow.ipc.read_schema(pyarrow.schema([f6]).serialize())[0]

    compare.compare_fields(f1, f1)
    compare.compare_fields(f2, f2)
    compare.compare_fields(f3, f3)
    compare.compare_fields(f4, f4)
    compare.compare_fields(f5, f5)
    compare.compare_fields(f6, f6)
    compare.compare_fields(f1, f5)
    compare.compare_fields(f5, f1)

    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f2)
    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f3)
    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f4)
    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f6)


def test_compare_schemas_nullability():
    # Should ignore nullability
    s1 = pyarrow.schema([pyarrow.field("a", pyarrow.int32(), nullable=True)])
    s2 = pyarrow.schema([pyarrow.field("a", pyarrow.int32(), nullable=False)])
    compare.compare_schemas(s1, s2)


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param(pyarrow.scalar(None), None, id="null"),
        pytest.param(
            pyarrow.scalar(3661, pyarrow.time32("s")),
            compare.NaiveTime(hour=1, minute=1, second=1, nanosecond=0),
            id="time32[s]",
        ),
        pytest.param(
            pyarrow.scalar(3661001, pyarrow.time32("ms")),
            compare.NaiveTime(hour=1, minute=1, second=1, nanosecond=1000000),
            id="time32[ms]",
        ),
        pytest.param(
            pyarrow.scalar(3661001001, pyarrow.time64("us")),
            compare.NaiveTime(hour=1, minute=1, second=1, nanosecond=1001000),
            id="time64[us]",
        ),
        pytest.param(
            pyarrow.scalar(3661001001001, pyarrow.time64("ns")),
            compare.NaiveTime(hour=1, minute=1, second=1, nanosecond=1001001),
            id="time64[ns]",
        ),
        pytest.param(
            pyarrow.scalar(1, pyarrow.timestamp("s")),
            "1970-01-01T00:00:01",
            id="timestamp[s]",
        ),
        pytest.param(
            pyarrow.scalar(1, pyarrow.timestamp("s", "UTC")),
            "1970-01-01T00:00:01Z",
            id="timestamp[s, UTC]",
        ),
        pytest.param(
            pyarrow.scalar(1, pyarrow.timestamp("s", "Asia/Tokyo")),
            "1970-01-01T09:00:01+09:00[Asia/Tokyo]",
            id="timestamp[s, Asia/Tokyo]",
        ),
        pytest.param(
            pyarrow.scalar(1, pyarrow.timestamp("s", "+09:00")),
            "1970-01-01T09:00:01+09:00",
            id="timestamp[s, +09:00]",
        ),
        pytest.param(
            pyarrow.scalar(1, pyarrow.timestamp("s", "-00:30")),
            "1969-12-31T23:30:01-00:30",
            id="timestamp[s, -00:30]",
        ),
        pytest.param(
            pyarrow.scalar(1, pyarrow.timestamp("ns")),
            "1970-01-01T00:00:00.000000001",
            id="timestamp[ns]",
        ),
        pytest.param(
            pyarrow.scalar(1, pyarrow.timestamp("ns", "UTC")),
            "1970-01-01T00:00:00.000000001Z",
            id="timestamp[ns, UTC]",
        ),
    ],
)
def test_scalar_to_py_smart(value: pyarrow.Scalar, expected: typing.Any) -> None:
    assert compare.scalar_to_py_smart(value) == expected


@pytest.mark.parametrize(
    "table, sort_keys, expected",
    [
        pytest.param(
            pyarrow.Table.from_pylist(
                [
                    {"idx": 1, "value": 0},
                    {"idx": 0, "value": 1},
                    {"idx": 2, "value": 2},
                ]
            ),
            [("idx", "descending")],
            pyarrow.Table.from_pylist(
                [
                    {"idx": 2, "value": 2},
                    {"idx": 1, "value": 0},
                    {"idx": 0, "value": 1},
                ]
            ),
            id="int",
        ),
        pytest.param(
            pyarrow.Table.from_pydict(
                {
                    "idx": pyarrow.opaque(pyarrow.int64(), "test", "").wrap_array(
                        pyarrow.array([1, 0, 2])
                    ),
                    "value": pyarrow.array([0, 1, 2]),
                }
            ),
            [("idx", "descending")],
            pyarrow.Table.from_pydict(
                {
                    "idx": pyarrow.opaque(pyarrow.int64(), "test", "").wrap_array(
                        pyarrow.array([2, 1, 0])
                    ),
                    "value": pyarrow.array([2, 0, 1]),
                }
            ),
            id="extension",
        ),
    ],
)
def test_sort_oblivious(
    table: pyarrow.Table, sort_keys: list, expected: pyarrow.Table
) -> None:
    result = compare.sort_oblivious(table, sort_keys)
    assert result.equals(expected)
