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

import contextlib

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import compare, model
from adbc_drivers_validation.model import Query


@contextlib.contextmanager
def scoped_trace(msg: str) -> None:
    try:
        yield
    except Exception as e:
        raise ExceptionGroup(msg, [e]) from None


def generate_tests(quirks: model.DriverQuirks, metafunc) -> None:
    """Parameterize the tests in this module for the given driver."""
    combinations = []

    queries = model.query_set(quirks.queries_path)
    for query in queries.queries.values():
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
        elif metafunc.definition.name == "test_query":
            if not isinstance(query.query, model.SelectQuery):
                continue

        combinations.append(
            pytest.param(
                quirks.name, query, id=f"{quirks.name}:{query.name}", marks=marks
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

        setup = subquery.setup_query()
        sql = subquery.query()
        expected_result = subquery.expected_result()

        if setup:
            with conn.cursor() as cursor:
                # Avoid using the regular methods since we don't want to prepare()
                for statement in driver.split_statement(setup):
                    with scoped_trace(f"setup statement: {statement}"):
                        cursor.adbc_statement.set_sql_query(statement)
                        cursor.adbc_statement.execute_update()

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
            cursor.adbc_statement.set_sql_query(sql)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        compare.compare_tables(expected_result, result, query.metadata())

    def test_execute_schema(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.SelectQuery)

        setup = subquery.setup_query()
        sql = subquery.query()
        expected_schema = subquery.expected_schema()

        if setup:
            with conn.cursor() as cursor:
                # Avoid using the regular methods since we don't want to prepare()
                for statement in driver.split_statement(setup):
                    cursor.adbc_statement.set_sql_query(statement)
                    cursor.adbc_statement.execute_update()

        with conn.cursor() as cursor:
            schema = cursor.adbc_execute_schema(sql)

        compare.compare_schemas(expected_schema, schema)
