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

import pytest

import adbc_drivers_validation.quirks as quirks


@pytest.mark.parametrize(
    "statement, dialect, expected",
    [
        # no comment
        ("SELECT 1;\nSELECT 2;", None, ["SELECT 1;", "SELECT 2;"]),
        ("SELECT 1;\nSELECT 2;", "postgres", ["SELECT 1", "SELECT 2"]),
        ("SELECT 1;\nSELECT 2;", "snowflake", ["SELECT 1", "SELECT 2"]),
        # trailing comment
        ("SELECT 1; -- comment\nSELECT 2;", None, ["SELECT 1; -- comment\nSELECT 2;"]),
        ("SELECT 1; -- comment\nSELECT 2;", "postgres", ["SELECT 1", "SELECT 2"]),
        ("SELECT 1; -- comment\nSELECT 2;", "snowflake", ["SELECT 1", "SELECT 2"]),
        # inline comment
        (
            "SELECT 1; /* comment */ SELECT 2;",
            None,
            ["SELECT 1; /* comment */ SELECT 2;"],
        ),
        ("SELECT 1; /* comment */ SELECT 2;", "postgres", ["SELECT 1", "SELECT 2"]),
        ("SELECT 1; /* comment */ SELECT 2;", "snowflake", ["SELECT 1", "SELECT 2"]),
        # leading comment
        (
            "SELECT 1;\n -- comment\nSELECT 2;",
            None,
            ["SELECT 1;", "-- comment\nSELECT 2;"],
        ),
        (
            "SELECT 1;\n -- comment\nSELECT 2;",
            "postgres",
            ["SELECT 1", "/* comment */ SELECT 2"],
        ),
        (
            "SELECT 1;\n -- comment\nSELECT 2;",
            "snowflake",
            ["SELECT 1", "/* comment */ SELECT 2"],
        ),
        ("SELECT 1; -- comment only\n", None, ["SELECT 1; -- comment only"]),
        ("SELECT 1; -- comment only\n", "postgres", ["SELECT 1"]),
        ("SELECT 1; -- comment only\n", "snowflake", ["SELECT 1"]),
        ("-- comment only\n", None, ["-- comment only"]),
        ("-- comment only\n", "postgres", ["-- comment only\n"]),
        ("-- comment only\n", "snowflake", ["-- comment only\n"]),
    ],
)
def test_split_statement(
    statement: str, dialect: str | None, expected: list[str]
) -> None:
    result = quirks.split_statement(statement, dialect)
    assert result == expected
