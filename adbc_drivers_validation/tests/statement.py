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

"""
Tests of statement-level features.

To use: import TestStatement and generate_tests, and from your own
pytest_generate_tests hook, call generate_tests.
"""

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import model


def generate_tests(quirks: model.DriverQuirks, metafunc) -> None:
    """Parameterize the tests in this module for the given driver."""
    marks = []
    combinations = []

    if (
        metafunc.definition.name == "test_parameter_schema"
        and not quirks.features.statement_get_parameter_schema
    ):
        marks.append(
            pytest.mark.xfail(
                raises=adbc_driver_manager.dbapi.NotSupportedError, strict=True
            )
        )

    if (
        metafunc.definition.name == "test_prepare"
        and not quirks.features.statement_prepare
    ):
        marks.append(
            pytest.mark.xfail(
                raises=adbc_driver_manager.dbapi.NotSupportedError, strict=True
            )
        )

    combinations.append(pytest.param(quirks.name, id=quirks.name, marks=marks))

    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )


@pytest.fixture
def requires_transactions(driver: model.DriverQuirks) -> None:
    if not driver.features.connection_transactions:
        pytest.skip("Driver does not support transactions")


class TestStatement:
    def test_parameter_schema(
        self, driver: model.DriverQuirks, conn: adbc_driver_manager.dbapi.Connection
    ) -> None:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                f"SELECT 1 + {driver.bind_parameter(1)}"
            )
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
        self, driver: model.DriverQuirks, conn: adbc_driver_manager.dbapi.Connection
    ) -> None:
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query("SELECT 1")
            cursor.adbc_statement.prepare()

    def test_transaction_toggle(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        requires_transactions: None,
    ) -> None:
        assert conn.adbc_connection.get_option("adbc.connection.autocommit") == "true"

        with pytest.raises(conn.ProgrammingError):
            conn.adbc_connection.commit()

        with pytest.raises(conn.ProgrammingError):
            conn.adbc_connection.rollback()

        conn.adbc_connection.set_options(**{"adbc.connection.autocommit": False})
        assert conn.adbc_connection.get_option("adbc.connection.autocommit") == "false"

        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchall()

        conn.adbc_connection.rollback()
        conn.adbc_connection.commit()

        conn.adbc_connection.set_options(**{"adbc.connection.autocommit": True})
        assert conn.adbc_connection.get_option("adbc.connection.autocommit") == "true"
