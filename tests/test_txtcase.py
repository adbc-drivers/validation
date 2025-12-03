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

import pyarrow
import pytest

from adbc_drivers_validation import txtcase


def test_one_part():
    contents = """
// part: data
data"""
    t = txtcase.TxtCase.loads("test.txt", contents)
    assert t._parts == {"data": "data"}


def test_one_part_multiline():
    contents = """
// part: data
data
    foobar"""
    t = txtcase.TxtCase.loads("test.txt", contents)
    assert t._parts == {"data": "data\n    foobar"}


def test_one_part_comments():
    contents = """
// This is the license header
// part: data
-- This comment is part of the data itself
SELECT 1"""
    t = txtcase.TxtCase.loads("test.txt", contents)
    assert t._parts == {"data": "-- This comment is part of the data itself\nSELECT 1"}


def test_two_part():
    contents = """
// This is the license header
// part: query
-- This comment is part of the data itself
SELECT 1
// part: expected
{"$0": 1}"""
    t = txtcase.TxtCase.loads("test.txt", contents)
    assert t._parts == {
        "query": "-- This comment is part of the data itself\nSELECT 1\n",
        "expected": '{"$0": 1}',
    }


def test_get_part_query():
    contents = """
// part: bind_query
-- This is the bind query
SELECT 'bind_query'
// part: query
-- This is the query
SELECT 'query'
// part: setup_query
-- This is the setup query
SELECT 'setup_query'
"""
    t = txtcase.TxtCase.loads("test.txt", contents)

    expected = "-- This is the bind query\nSELECT 'bind_query'\n"
    assert t.get_part("bind_query") == expected

    expected = "-- This is the query\nSELECT 'query'\n"
    assert t.get_part("query") == expected

    expected = "-- This is the setup query\nSELECT 'setup_query'\n"
    assert t.get_part("setup_query") == expected


def test_get_part_schema():
    contents = """
// part: bind_schema
{
    "format": "+s",
    "children": [
        {
            "name": "res",
            "format": "g",
            "flags": ["nullable"]
        }
    ]
}
// part: expected_schema
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
// part: input_schema
{
    "format": "+s",
    "children": [
        {
            "name": "res",
            "format": "s",
            "flags": ["nullable"]
        }
    ]
}
"""
    t = txtcase.TxtCase.loads("test.txt", contents)

    expected = pyarrow.schema([pyarrow.field("res", pyarrow.float64(), nullable=True)])
    assert t.get_part("bind_schema") == expected

    expected = pyarrow.schema([pyarrow.field("res", pyarrow.int32(), nullable=True)])
    assert t.get_part("expected_schema") == expected

    expected = pyarrow.schema([pyarrow.field("res", pyarrow.int16(), nullable=True)])
    assert t.get_part("input_schema") == expected


def test_get_part_table():
    contents = """
// part: bind
{"res": 1}
{"res": 2}
// part: expected
{"res": 3}
{"res": 4}
// part: input
{"res": 5}
{"res": 6}
"""
    t = txtcase.TxtCase.loads("test.txt", contents)

    schema = pyarrow.schema([pyarrow.field("res", pyarrow.int64(), nullable=True)])

    expected = pyarrow.table({"res": [1, 2]}, schema=schema)
    assert t.get_part("bind", schema) == expected

    expected = pyarrow.table({"res": [3, 4]}, schema=schema)
    assert t.get_part("expected", schema) == expected

    expected = pyarrow.table({"res": [5, 6]}, schema=schema)
    assert t.get_part("input", schema) == expected


def test_get_part_invalid():
    contents = """
// part: query
-- This is the query
SELECT 'query'

// part: expected_schema
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

// part: expected
{"res": 3}
{"res": 4}

// part: unimplemented
{"res": 3}
{"res": 4}
"""
    t = txtcase.TxtCase.loads("test.txt", contents)

    with pytest.raises(KeyError):
        assert t.get_part("nonexistent")

    with pytest.raises(NotImplementedError):
        assert t.get_part("unimplemented")

    with pytest.raises(ValueError):
        assert t.get_part("query", schema=pyarrow.schema([]))

    with pytest.raises(ValueError):
        assert t.get_part("expected")
