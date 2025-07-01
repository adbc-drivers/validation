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

from adbc_drivers_validation import model


def pytest_generate_tests(metafunc) -> None:
    # Given the drivers to test, load all the queries, then parametrize the
    # tests in this module with the driver and query.
    combinations = []
    drivers = model.drivers_to_test()

    for driver in drivers:
        driver_manifest = model.driver_manifest(driver)
        marks = []

        if (
            metafunc.definition.name == "test_parameter_schema"
            and not driver_manifest.features.statement_get_parameter_schema
        ):
            marks.append(
                pytest.mark.xfail(
                    raises=adbc_driver_manager.dbapi.NotSupportedError, strict=True
                )
            )

        if (
            metafunc.definition.name == "test_prepare"
            and not driver_manifest.features.statement_prepare
        ):
            marks.append(
                pytest.mark.xfail(
                    raises=adbc_driver_manager.dbapi.NotSupportedError, strict=True
                )
            )

        combinations.append(pytest.param(driver, id=f"{driver}", marks=marks))

    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )


def test_parameter_schema(
    driver: model.DriverQuirks, conn: adbc_driver_manager.dbapi.Connection
) -> None:
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query(f"SELECT 1 + {driver.bind_parameter(1)}")
        cursor.adbc_statement.prepare()
        handle = cursor.adbc_statement.get_parameter_schema()
        schema = pyarrow.Schema._import_from_c(handle.address)
        assert len(schema) == 1

    with conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query(
            f"SELECT 1 + {driver.bind_parameter(1)}+ {driver.bind_parameter(2)}"
        )
        cursor.adbc_statement.prepare()
        handle = cursor.adbc_statement.get_parameter_schema()
        schema = pyarrow.Schema._import_from_c(handle.address)
        assert len(schema) == 2


def test_prepare(
    driver: model.DriverQuirks, conn: adbc_driver_manager.dbapi.Connection
) -> None:
    with conn.cursor() as cursor:
        cursor.adbc_statement.set_sql_query("SELECT 1")
        cursor.adbc_statement.prepare()
