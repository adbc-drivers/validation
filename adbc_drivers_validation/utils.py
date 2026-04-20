# Copyright (c) 2025-2026 ADBC Drivers Contributors
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
import pytest

if typing.TYPE_CHECKING:
    from adbc_drivers_validation.model import DriverQuirks, Query


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
def scoped_trace(msg: str) -> typing.Generator[None, None, None]:
    """If an exception is raised, add the given note to it."""
    try:
        yield
    except Exception as e:
        e.add_note(msg)
        raise


@contextlib.contextmanager
def setup_connection(
    query: "Query", conn: adbc_driver_manager.dbapi.Connection
) -> typing.Generator[None]:
    md = query.metadata()
    connection_md = None

    if md.setup.connection is not None:
        connection_md = md.setup.connection

    if connection_md is None and md.connection is not None:
        connection_md = md.connection
        warnings.warn(
            f"Toplevel connection in {query.name}.toml is deprecated, use setup.connection instead",
            DeprecationWarning,
        )

    if connection_md is None:
        yield
        return

    options = {}
    options_revert = {}
    for key, value in connection_md.options.items():
        options[key] = value.apply
        if revert := value.revert:
            options_revert[key] = revert
        else:
            warnings.warn(
                f"No revert value for connection option {key} in {query.name}, this will likely have unexpected side effects!",
                DeprecationWarning,
            )

    conn.adbc_connection.set_options(**options)
    yield
    conn.adbc_connection.set_options(**options_revert)


@contextlib.contextmanager
def setup_statement(
    query: "Query", cursor: adbc_driver_manager.dbapi.Cursor
) -> typing.Generator[None]:
    md = query.metadata()
    statement_md = None

    if md.setup.statement is not None:
        statement_md = md.setup.statement

    if statement_md is None and md.statement is not None:
        statement_md = md.statement
        warnings.warn(
            f"Toplevel statement in {query.name}.toml is deprecated, use setup.statement instead",
            DeprecationWarning,
        )

    if statement_md is None:
        yield
        return

    options = {}
    options_revert = {}
    for key, value in statement_md.options.items():
        options[key] = value.apply
        if revert := value.revert:
            options_revert[key] = revert

    cursor.adbc_statement.set_options(**options)
    yield
    cursor.adbc_statement.set_options(**options_revert)


def execute_query_without_prepare(
    cursor: adbc_driver_manager.dbapi.Cursor, query: str
) -> pyarrow.Table:
    """
    Execute a query without prepare and return the result.

    This is a helper for executing queries using the lower-level ADBC API
    without going through the DBAPI prepare mechanism.

    Parameters
    ----------
    cursor : adbc_driver_manager.dbapi.Cursor
        The cursor to execute the query.
    query : str
        The SQL query to execute.

    Returns
    -------
    pyarrow.Table
        The result of the query.
    """
    cursor.adbc_statement.set_sql_query(query)
    try:
        handle, _ = cursor.adbc_statement.execute_query()
        with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
            return reader.read_all()
    except Exception as e:
        e.add_note(f"Query: {query}")
        raise


def arrow_type_name(
    arrow_type: pyarrow.DataType,
    metadata: dict[bytes, bytes] | None = None,
    show_type_parameters: bool = False,
) -> str:
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


def assert_field_type_name(
    driver: "DriverQuirks", query: "Query", schema: pyarrow.Schema
) -> None:
    field_name = (f"{driver.name.upper()}:type").encode("utf-8")
    if driver.features.metadata_type_name:
        for i, field in enumerate(schema):
            assert field.metadata is not None
            assert field_name in field.metadata
            assert field.metadata[field_name].decode(
                "utf-8"
            ) == query.metadata().tags.metadata_type_name(i)
    else:
        for field in schema:
            if field.metadata is not None:
                assert field_name not in field.metadata


def generate_tests_by_marks(
    all_quirks: list["DriverQuirks"], metafunc: pytest.Metafunc
) -> bool:
    """
    Parameterize tests for a given driver using marks.

    Instead of adding logic to generate_tests, add
    ``@pytest.mark.requires_features`` to have the test be automatically
    parameterized and skipped (if needed).

    Returns
    -------
    True if the test had marks and was parameterized.
    """

    feature_marks = [
        mark
        for mark in metafunc.definition.iter_markers()
        if mark.name == "requires_features"
    ]
    if not feature_marks:
        return False

    features = set()
    for mark in feature_marks:
        for feature in mark.args[0]:
            features.add(feature)

    combinations = []
    for quirks in all_quirks:
        driver_param = f"{quirks.name}:{quirks.short_version}"
        additional_marks = []
        f = quirks.features
        for feature in features:
            fv = getattr(f, feature)
            if fv is False or fv is None:
                additional_marks.append(
                    pytest.mark.skip(reason=f"{feature} not supported")
                )
                break
        combinations.append(
            pytest.param(driver_param, id=driver_param, marks=additional_marks)
        )
    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )
    return True
