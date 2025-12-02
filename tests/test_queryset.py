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

from pathlib import Path

import pyarrow
import pytest

from adbc_drivers_validation import model


def test_txtcase_empty(tmp_path: Path):
    with (tmp_path / "query.txtcase").open("w") as f:
        f.write("\n")

    with pytest.raises(ValueError, match="unknown query type"):
        model.QuerySet.load(tmp_path)


def test_txtcase_select(tmp_path: Path):
    with (tmp_path / "query.txtcase").open("w") as f:
        f.write(
            """
// part: metadata
[tags]
sql-type-name = "INT"

// part: query
SELECT 1

// part: expected_schema
{
    "format": "+s",
    "children": [
        {
            "name": "$0",
            "format": "i",
            "flags": ["nullable"]
        }
    ]
}

// part: expected
{"$0": 1}
"""
        )

    query_set = model.QuerySet.load(tmp_path)
    assert list(query_set.queries) == ["query"]
    query = query_set.queries["query"]

    assert query.metadata() == {"tags": {"sql-type-name": "INT"}}
    assert isinstance(query.query, model.SelectQuery)
    assert query.query.setup_query() is None
    assert query.query.query().strip() == "SELECT 1"
    assert query.query.expected_schema() == pyarrow.schema([("$0", pyarrow.int32())])
    assert query.query.expected_result().to_pylist() == [{"$0": 1}]


def test_txtcase_override(tmp_path: Path):
    (tmp_path / "base").mkdir()
    (tmp_path / "over").mkdir()

    contents = """
// part: metadata
[tags]
sql-type-name = "INT"

// part: query
SELECT 1

// part: expected_schema
{
    "format": "+s",
    "children": [
        {
            "name": "$0",
            "format": "i",
            "flags": ["nullable"]
        }
    ]
}

// part: expected
{"$0": 1}
"""

    with (tmp_path / "base/query1.txtcase").open("w") as f:
        f.write(contents)

    with (tmp_path / "base/query2.txtcase").open("w") as f:
        f.write(contents)

    with (tmp_path / "over/query1.txtcase").open("w") as f:
        f.write(
            """
// part: metadata
[tags]
partial-support = true

// part: expected
{"$0": 2}
"""
        )

    with (tmp_path / "over/query3.txtcase").open("w") as f:
        f.write(contents)

    base = model.QuerySet.load(tmp_path / "base")
    assert list(base.queries) == ["query1", "query2"]
    query_set = model.QuerySet.load(tmp_path / "over", parent=base)
    assert list(query_set.queries) == ["query1", "query2", "query3"]
    query = query_set.queries["query1"]

    assert query.metadata() == {
        "tags": {"partial-support": True, "sql-type-name": "INT"}
    }
    assert isinstance(query.query, model.SelectQuery)
    assert query.query.setup_query() is None
    assert query.query.query().strip() == "SELECT 1"
    assert query.query.expected_schema() == pyarrow.schema([("$0", pyarrow.int32())])
    assert query.query.expected_result().to_pylist() == [{"$0": 2}]
