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
import warnings

import adbc_driver_manager.dbapi
import pyarrow

if typing.TYPE_CHECKING:
    from adbc_drivers_validation.model import Query


def merge_into(target: dict[str, typing.Any], values: dict[str, typing.Any]) -> None:
    """Recursively merge two dictionaries."""
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
    """If an exception is raised, add the given note to it."""
    try:
        yield
    except Exception as e:
        e.add_note(msg)
        raise


@contextlib.contextmanager
def setup_connection(
    query: "Query", conn: adbc_driver_manager.dbapi.Connection
) -> None:
    md = query.metadata()
    connection_md = None

    if "setup" in md:
        if "connection" in md["setup"]:
            connection_md = md["setup"]["connection"]

    if connection_md is None and "connection" in md:
        connection_md = md["connection"]
        warnings.warn(
            f"Toplevel connection in {query.name}.toml is deprecated, use setup.connection instead",
            DeprecationWarning,
        )

    if connection_md is None:
        yield
        return

    options = {}
    options_revert = {}
    for key, value in connection_md["options"].items():
        if isinstance(value, dict):
            assert "apply" in value
            assert "revert" in value
            options[key] = value["apply"]
            options_revert[key] = value["revert"]
        else:
            options[key] = value

    conn.adbc_connection.set_options(**options)
    yield
    conn.adbc_connection.set_options(**options_revert)


@contextlib.contextmanager
def setup_statement(query: "Query", cursor: adbc_driver_manager.dbapi.Cursor) -> None:
    md = query.metadata()
    statement_md = None

    if "setup" in md:
        if "statement" in md["setup"]:
            statement_md = md["setup"]["statement"]

    if statement_md is None and "statement" in md:
        statement_md = md["statement"]
        warnings.warn(
            f"Toplevel statement in {query.name}.toml is deprecated, use setup.statement instead",
            DeprecationWarning,
        )

    if statement_md is None:
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


def arrow_type_name(arrow_type, metadata=None, show_type_parameters=False):
    """Render the name of an Arrow type in a friendly way."""
    # Special handling (sometimes we want params, sometimes not)
    if metadata and (ext := metadata.get(b"ARROW:extension:name")):
        return f"extension<{ext.decode('utf-8')}>"
    if show_type_parameters:
        return str(arrow_type)
    elif isinstance(arrow_type, pyarrow.Decimal32Type):
        return "decimal32"
    elif isinstance(arrow_type, pyarrow.Decimal64Type):
        return "decimal64"
    elif isinstance(arrow_type, pyarrow.Decimal128Type):
        return "decimal128"
    elif isinstance(arrow_type, pyarrow.Decimal256Type):
        return "decimal256"
    elif isinstance(arrow_type, pyarrow.FixedSizeBinaryType):
        return "fixed_size_binary"
    elif isinstance(arrow_type, pyarrow.FixedSizeListType):
        return "fixed_size_binary"
    elif isinstance(arrow_type, pyarrow.ListType):
        return "list"
    elif isinstance(arrow_type, pyarrow.StructType):
        return "struct"
    elif isinstance(arrow_type, pyarrow.TimestampType):
        if arrow_type.tz:
            return f"timestamp[{arrow_type.unit}] (with time zone)"
        return str(arrow_type)
    return str(arrow_type)
