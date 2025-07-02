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
