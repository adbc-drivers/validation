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

import contextlib
import typing

import adbc_driver_manager.dbapi

if typing.TYPE_CHECKING:
    from adbc_drivers_validation.model import Query


def merge_into(target: dict[str, typing.Any], values: dict[str, typing.Any]) -> None:
    for key, value in values.items():
        if isinstance(value, dict):
            if key in target:
                merge_into(target[key], value)
            else:
                target[key] = value.copy()
        elif isinstance(value, list):
            target[key] = value[:]
        else:
            target[key] = value


@contextlib.contextmanager
def scoped_trace(msg: str) -> None:
    try:
        yield
    except Exception as e:
        e.add_note(msg)
        raise


@contextlib.contextmanager
def setup_statement(query: "Query", cursor: adbc_driver_manager.dbapi.Cursor) -> None:
    md = query.metadata()
    if "statement" not in md:
        yield
        return

    statement_md = md["statement"]
    if "options" not in statement_md:
        yield
        return

    options = {}
    options_revert = {}
    for key, value in statement_md["options"].items():
        if isinstance(value, dict):
            assert "apply" in value
            assert "revert" in value
            options[key] = value["apply"]
            options_revert[key] = value["revert"]
        else:
            options[key] = value

    cursor.adbc_statement.set_options(**options)
    yield
    cursor.adbc_statement.set_options(**options_revert)
