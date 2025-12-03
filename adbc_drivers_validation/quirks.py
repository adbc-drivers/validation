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


def split_statement(statement: str) -> list[str]:
    """
    Split a SQL statement into individual statements.

    Some vendors can't handle multiple queries in a single statement.
    """

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
