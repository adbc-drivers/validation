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
import pytest

from adbc_drivers_validation import compare, model
from adbc_drivers_validation.model import Query


def pytest_generate_tests(metafunc) -> None:
    combinations = []
    drivers = model.drivers_to_test()

    for driver in drivers:
        driver_manifest = model.driver_manifest(driver)
        queries = model.query_set(driver)
        for query in queries.queries.values():
            marks = []
            if not isinstance(query.query, model.SchemaQuery):
                continue
            if not driver_manifest.features.connection_get_table_schema:
                marks.append(pytest.mark.skip(reason="not implemented"))

            combinations.append(
                pytest.param(driver, query, id=f"{driver}:{query.name}", marks=marks)
            )
    metafunc.parametrize(
        "driver,query",
        combinations,
        scope="module",
        indirect=["driver"],
    )


def test_get_table_schema(
    driver: model.DriverQuirks,
    conn: adbc_driver_manager.dbapi.Connection,
    query: Query,
) -> None:
    subquery = query.query
    assert isinstance(subquery, model.SchemaQuery)

    setup = subquery.setup_query()
    expected_schema = subquery.expected_schema()

    with conn.cursor() as cursor:
        # Avoid using the regular methods since we don't want to prepare()
        for statement in driver.split_statement(setup):
            cursor.adbc_statement.set_sql_query(statement)
            cursor.adbc_statement.execute_update()

    schema = conn.adbc_get_table_schema("test_table_schema")
    result = compare.make_nullable(schema)
    assert result == expected_schema
