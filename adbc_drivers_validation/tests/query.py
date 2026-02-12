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
Tests that involve running queries.

To use: import TestQuery and generate_tests, and from your own
pytest_generate_tests hook, call generate_tests.
"""

import typing

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import compare, model
from adbc_drivers_validation.model import Query
from adbc_drivers_validation.utils import (
    execute_query_without_prepare,
    scoped_trace,
    setup_connection,
    setup_statement,
)


def generate_tests(all_quirks: list[model.DriverQuirks], metafunc) -> None:
    """Parameterize the tests in this module for the given driver."""
    combinations = []

    for quirks in all_quirks:
        driver_param = f"{quirks.name}:{quirks.short_version}"
        for query in quirks.query_set.queries.values():
            marks = []
            marks.extend(query.pytest_marks)

            if (
                not quirks.features.statement_bind
                and isinstance(query.query, model.SelectQuery)
                and query.query.bind_query(quirks) is not None
            ):
                marks.append(pytest.mark.skip(reason="bind not supported"))

            if metafunc.definition.name == "test_execute_schema":
                if not isinstance(query.query, model.SelectQuery):
                    continue
                if not query.name.startswith("type/select/"):
                    # There's no need to repeat this test multiple times per type
                    continue
                if not quirks.features.statement_execute_schema:
                    marks.append(pytest.mark.skip(reason="not implemented"))
            elif metafunc.definition.name == "test_get_table_schema":
                if not isinstance(query.query, model.SelectQuery):
                    continue
                elif not query.name.startswith("type/select/"):
                    continue
                elif not quirks.features.connection_get_table_schema:
                    marks.append(pytest.mark.skip(reason="not implemented"))
            elif metafunc.definition.name == "test_query":
                if not isinstance(query.query, model.SelectQuery):
                    continue

            combinations.append(
                pytest.param(
                    driver_param, query, id=f"{driver_param}:{query.name}", marks=marks
                )
            )

    metafunc.parametrize(
        "driver,query",
        combinations,
        scope="module",
        indirect=["driver"],
    )


class TestQuery:
    """Tests that involve running queries."""

    def test_query(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.SelectQuery)

        sql = subquery.query()
        expected_result = subquery.expected_result()

        _setup_query(driver, conn, query)

        bind = subquery.bind_query(driver)
        if bind:
            # TODO: also test with stream
            # TODO: also test with executequery, not executeupdate
            # TODO: also test with multiple batches in stream
            # TODO: also test with empty stream/empty batch
            data = subquery.bind_data().combine_chunks().to_batches()[0]
            with conn.cursor() as cursor:
                cursor.adbc_statement.set_sql_query(bind)
                cursor.adbc_statement.bind(data)
                cursor.adbc_statement.execute_update()

        with conn.cursor() as cursor:
            with setup_statement(query, cursor):
                with scoped_trace(f"query: {sql}"):
                    result = execute_query_without_prepare(cursor, sql)

        compare.compare_tables(expected_result, result, query.metadata())

    def test_execute_schema(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.SelectQuery)

        sql = subquery.query()
        expected_schema = subquery.expected_schema()

        _setup_query(driver, conn, query)

        with conn.cursor() as cursor:
            with setup_statement(query, cursor):
                schema = cursor.adbc_execute_schema(sql)

        compare.compare_schemas(expected_schema, schema)

    def test_get_table_schema(
        self,
        driver: model.DriverQuirks,
        conn_factory: typing.Callable[[], adbc_driver_manager.dbapi.Connection],
        query: model.Query,
    ) -> None:
        subquery = query.query

        expected_schema = subquery.expected_schema()

        with conn_factory() as conn:
            with setup_connection(query, conn):
                _setup_query(driver, conn, query)

                table_name = None
                md = query.metadata()
                table_name = md.setup.drop
                if not table_name and isinstance(subquery, model.SelectQuery):
                    # XXX: rather hacky, but extract the table name from the SELECT query
                    # that would normally be executed
                    query_str = subquery.query().split()
                    for i, word in enumerate(query_str):
                        if word.upper() == "FROM":
                            table_name = query_str[i + 1]
                            break

                assert table_name, "Could not determine table name"

                schema = conn.adbc_get_table_schema(table_name)
                # Ignore the first column which is normally used to sort the table
                schema = pyarrow.schema(list(schema)[1:])
                compare.compare_schemas(expected_schema, schema)

    def test_show_queries(
        self,
        driver: model.DriverQuirks,
        query: model.Query,
    ) -> None:
        """Print out basic query metadata for debugging/development."""
        print(query.name.ljust(30), ":", end=" ")
        if isinstance(query.query, model.IngestQuery):
            schema = query.query.expected_schema()
            field = schema[1]
            print(
                str(field.type).ljust(40),
                "=>",
                query.metadata().tags.sql_type_name,
            )
        elif isinstance(query.query, model.SelectQuery):
            schema = query.query.expected_schema()
            field = schema[0]
            if query.query.bind_path is not None:
                print(
                    str(field.type).ljust(40),
                    "=>",
                    query.metadata().tags.sql_type_name,
                )
            else:
                print(
                    (query.metadata().tags.sql_type_name or "(unset type name)").ljust(
                        40
                    ),
                    "=>",
                    field.type,
                )
        else:
            raise TypeError(type(query.query))


def _setup_query(
    driver: model.DriverQuirks,
    conn: adbc_driver_manager.dbapi.Connection,
    query: Query,
) -> None:
    subquery = query.query
    assert isinstance(subquery, model.SelectQuery)
    setup = subquery.setup_query()

    if setup:
        md = query.metadata()
        with conn.cursor() as cursor:
            # Avoid using the regular methods since we don't want to prepare()
            statements = []

            if drop := md.setup.drop:
                statements.append(driver.drop_table(table_name=drop))

            statements.extend(driver.split_statement(setup))
            for statement in statements:
                with scoped_trace(f"setup statement: {statement}"):
                    try:
                        cursor.adbc_statement.set_sql_query(statement)
                        cursor.adbc_statement.execute_update()
                    except adbc_driver_manager.Error as e:
                        # Some databases have no way to do DROP IF EXISTS
                        if driver.is_table_not_found(table_name=None, error=e):
                            continue
                        raise
