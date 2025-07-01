# Copyright (c) 2025 Columnar Technologies, Inc.
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

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import compare, model
from adbc_drivers_validation.model import Query


def pytest_generate_tests(metafunc) -> None:
    # Given the drivers to test, load all the queries, then parametrize the
    # tests in this module with the driver and query.
    combinations = []
    drivers = model.drivers_to_test()

    for driver in drivers:
        driver_manifest = model.driver_manifest(driver)
        queries = model.query_set(driver)
        for query in queries.queries.values():
            marks = []
            if metafunc.definition.name == "test_execute_schema":
                if not isinstance(query.query, model.SelectQuery):
                    continue
                if not query.name.startswith("type/select/"):
                    # There's no need to repeat this test multiple times per type
                    continue
                if not driver_manifest.features.statement_execute_schema:
                    marks.append(pytest.mark.skip(reason="not implemented"))
            elif metafunc.definition.name == "test_query":
                if not isinstance(query.query, model.SelectQuery):
                    continue

            combinations.append(
                pytest.param(driver, query, id=f"{driver}:{query.name}", marks=marks)
            )
    metafunc.parametrize(
        "driver,query",
        combinations,
        scope="module",
        indirect=["driver"],
    )


def test_query(
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

    schema = compare.make_nullable(schema)
    assert schema == expected_schema
