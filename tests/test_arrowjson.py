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
import decimal
import io

import pyarrow
import pytest

from adbc_drivers_validation import arrowjson


def test_parse_int32_schema():
    f = io.StringIO("""
{
    "format": "+s",
    "children": [
        {
            "name": "res",
            "format": "i",
            "flags": ["nullable"]
        }
    ]
}
    """)
    parsed_schema = arrowjson.load_schema(f)

    # Should be a schema with a single int32 field named 'res'
    assert len(parsed_schema) == 1
    assert parsed_schema.names == ["res"]
    assert parsed_schema.types[0] == pyarrow.int32()
    assert parsed_schema.field("res").nullable


def test_parse_extension_schema():
    f = io.StringIO("""
{
    "format": "+s",
    "children": [
        {
            "name": "res",
            "format": "u",
            "flags": ["nullable"],
            "metadata": {
                "ARROW:extension:name": "geoarrow.wkt"
            }
        }
    ]
}
    """)
    parsed_schema = arrowjson.load_schema(f)

    assert len(parsed_schema) == 1
    assert parsed_schema.names == ["res"]
    assert parsed_schema.types[0] == pyarrow.string()
    assert parsed_schema.field("res").nullable
    assert parsed_schema.field("res").metadata == {
        b"ARROW:extension:name": b"geoarrow.wkt"
    }


def test_parse_type_format_primitives():
    assert arrowjson.parse_type_format("n") == pyarrow.null()
    assert arrowjson.parse_type_format("b") == pyarrow.bool_()
    assert arrowjson.parse_type_format("i") == pyarrow.int32()
    assert arrowjson.parse_type_format("l") == pyarrow.int64()
    assert arrowjson.parse_type_format("g") == pyarrow.float64()


def test_parse_type_format_nested():
    # Simple struct
    struct_type = arrowjson.parse_type_format(
        "+s",
        [pyarrow.field("f1", pyarrow.int32()), pyarrow.field("f2", pyarrow.string())],
    )
    assert isinstance(struct_type, pyarrow.StructType)
    assert len(struct_type) == 2
    assert struct_type.field("f1").type == pyarrow.int32()
    assert struct_type.field("f2").type == pyarrow.string()

    # List
    list_type = arrowjson.parse_type_format(
        "+l", [pyarrow.field("item", pyarrow.int32())]
    )
    assert isinstance(list_type, pyarrow.ListType)
    assert list_type.value_type == pyarrow.int32()

    # Fixed-size list
    fixed_list_type = arrowjson.parse_type_format(
        "+w:3", [pyarrow.field("item", pyarrow.float64())]
    )
    assert isinstance(fixed_list_type, pyarrow.FixedSizeListType)
    assert fixed_list_type.value_type == pyarrow.float64()
    assert fixed_list_type.list_size == 3


def test_from_dict_simple():
    field = arrowjson.field_from_dict(
        {"name": "test_field", "format": "i", "flags": ["nullable"]}
    )

    assert field.name == "test_field"
    assert field.type == pyarrow.int32()
    assert field.nullable


def test_from_dict_nested():
    field = arrowjson.field_from_dict(
        {
            "name": "parent",
            "format": "+s",
            "flags": [],
            "children": [
                {"name": "child1", "format": "i", "flags": ["nullable"]},
                {"name": "child2", "format": "u", "flags": []},
            ],
        }
    )

    assert field.name == "parent"
    assert isinstance(field.type, pyarrow.StructType)
    assert not field.nullable
    assert len(field.type) == 2

    child1 = field.type.field("child1")
    assert child1.name == "child1"
    assert child1.type == pyarrow.int32()
    assert child1.nullable

    child2 = field.type.field("child2")
    assert child2.name == "child2"
    assert child2.type == pyarrow.string()
    assert not child2.nullable


def test_from_dict_list():
    field = arrowjson.field_from_dict(
        {
            "name": "items",
            "format": "+l",
            "flags": ["nullable"],
            "children": [{"name": "item", "format": "f", "flags": []}],
        }
    )

    assert field.name == "items"
    assert isinstance(field.type, pyarrow.ListType)
    assert field.nullable
    assert field.type.value_type == pyarrow.float32()


def test_load_table_extra_fields():
    schema = pyarrow.schema([pyarrow.field("a", pyarrow.int32())])
    data = [{"a": 1, "b": 2}]
    with pytest.raises(ValueError, match="Extra fields in row: {'b': 2}"):
        arrowjson.table_from_rows(data, schema)


@pytest.mark.parametrize(
    "raw,field,expected",
    [
        pytest.param(
            [0, 1, None],
            pyarrow.field("int32", pyarrow.int32(), nullable=True),
            pyarrow.array([0, 1, None], type=pyarrow.int32()),
            id="int32",
        ),
        pytest.param(
            [base64.b64encode(b"hello").decode("utf-8"), None],
            pyarrow.field("binary", pyarrow.binary(), nullable=True),
            pyarrow.array([b"hello", None], type=pyarrow.binary()),
            id="binary",
        ),
        pytest.param(
            [base64.b64encode(b"hello").decode("utf-8"), None],
            pyarrow.field("binary", pyarrow.binary_view(), nullable=True),
            pyarrow.array([b"hello", None], type=pyarrow.binary_view()),
            id="binary_view",
        ),
        pytest.param(
            [123, "0.012345", None],
            pyarrow.field("decimal", pyarrow.decimal128(38, 10), nullable=True),
            pyarrow.array(
                [decimal.Decimal("123"), decimal.Decimal("0.012345"), None],
                type=pyarrow.decimal128(38, 10),
            ),
            id="decimal128(38, 10)",
        ),
    ],
)
def test_array_from_values(
    raw: list, field: pyarrow.Field, expected: pyarrow.Array
) -> None:
    actual = arrowjson.array_from_values(raw, field)
    assert actual == expected


def test_array_from_values_list() -> None:
    values = [None, [], ["{}"], ["{}", "false"]]
    field = pyarrow.field(
        "list_field",
        pyarrow.list_(pyarrow.json_()),
    )
    actual = arrowjson.array_from_values(values, field)
    expected = pyarrow.ListArray.from_arrays(
        pyarrow.array([0, 0, 0, 1, 3], type=pyarrow.int32()),
        pyarrow.json_().wrap_array(
            pyarrow.array(["{}", "{}", "false"], type=pyarrow.string())
        ),
        type=field.type,
        mask=pyarrow.array([True, False, False, False]),
    )
    assert actual == expected


def test_array_from_values_struct() -> None:
    field = pyarrow.field(
        "struct_field",
        pyarrow.struct(
            [
                pyarrow.field("a", pyarrow.int32()),
                pyarrow.field("b", pyarrow.binary()),
            ]
        ),
    )

    values = [
        None,
        {"a": 1, "b": base64.b64encode(b"hello").decode("utf-8")},
        {"a": None, "b": base64.b64encode(b"hello").decode("utf-8")},
    ]
    actual = arrowjson.array_from_values(values, field)
    expected = pyarrow.array(
        [None, {"a": 1, "b": b"hello"}, {"a": None, "b": b"hello"}],
        type=field.type,
    )
    assert actual == expected


def test_parse_type_format_decimal():
    """Test decimal format parsing with various bitwidths and legacy format."""
    # Test decimal32 (precision 1-9)
    decimal32_type = arrowjson.parse_type_format("d:9,2,32")
    assert decimal32_type == pyarrow.decimal32(9, 2)

    # Test decimal64 with precision 10
    decimal64_type_10 = arrowjson.parse_type_format("d:10,2,64")
    assert decimal64_type_10 == pyarrow.decimal64(10, 2)

    # Test decimal64 (precision 1-18)
    decimal64_type = arrowjson.parse_type_format("d:18,4,64")
    assert decimal64_type == pyarrow.decimal64(18, 4)

    # Test decimal128 (precision 1-38)
    decimal128_type = arrowjson.parse_type_format("d:38,10,128")
    assert decimal128_type == pyarrow.decimal128(38, 10)

    # Test decimal256 (precision 1-76)
    decimal256_type = arrowjson.parse_type_format("d:76,20,256")
    assert decimal256_type == pyarrow.decimal256(76, 20)

    # Test legacy format (2 parameters, defaults to decimal128)
    legacy_type = arrowjson.parse_type_format("d:10,2")
    assert legacy_type == pyarrow.decimal128(10, 2)

    # Test unsupported bitwidth
    with pytest.raises(ValueError, match="Unsupported decimal bitwidth: 16"):
        arrowjson.parse_type_format("d:10,2,16")

    # Test invalid format (non-integer parameter)
    with pytest.raises(ValueError, match="invalid literal for int"):
        arrowjson.parse_type_format("d:10,2,extra")

    # Test invalid format (too few parameters)
    with pytest.raises(ValueError, match="Invalid decimal format: d:10"):
        arrowjson.parse_type_format("d:10")
