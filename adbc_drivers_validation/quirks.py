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

"""
Driver-specific functionality needed for testing.
"""

import sqlglot


def split_statement(statement: str, dialect: str | None = None) -> list[str]:
    """
    Split a SQL statement into individual statements.

    Some vendors can't handle multiple queries in a single statement.

    Args:
        statement: The SQL statement(s) to split.
        dialect: Optional SQL dialect for parsing (e.g., "databricks", "postgres", "mysql").
                 If provided, uses sqlglot for robust parsing. If not, uses simple line-based splitting.

    Returns:
        List of individual SQL statements.
    """
    if dialect is not None:
        try:
            parsed = sqlglot.parse(statement, dialect=dialect)
            # Filter out None/empty, comment-only statements, and strip trailing semicolons
            statements = []
            for stmt in parsed:
                if not stmt:
                    continue
                stmt_str = stmt.sql(dialect=dialect, pretty=False).rstrip(";").strip()
                # Skip if it's a comment-only statement (starts with /* or --)
                if stmt_str.startswith("/*") or stmt_str.startswith("--"):
                    continue
                if stmt_str:
                    statements.append(stmt_str)
            return statements if statements else [statement]
        except Exception:
            pass

    statements = []
    current = []

    def flush():
        nonlocal statements
        nonlocal current
        v = "\n".join(current).strip()
        if v:
            statements.append(v)
        current = []

    for line in statement.split("\n"):
        current.append(line)
        if line.strip().endswith(";"):
            flush()
    if current:
        flush()
    return statements
