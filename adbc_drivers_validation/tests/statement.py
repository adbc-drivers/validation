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
Tests of statement-level features.

To use: import TestStatement and generate_tests, and from your own
pytest_generate_tests hook, call generate_tests.
"""

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import model


def generate_tests(all_quirks: list[model.DriverQuirks], metafunc) -> None:
    """Parameterize the tests in this module for the given driver."""
    marks = []
    combinations = []

    for quirks in all_quirks:
        driver_param = f"{quirks.name}:{quirks.short_version}"
        if (
            metafunc.definition.name == "test_parameter_execute"
            and not quirks.features.statement_bind
        ):
            marks.append(pytest.mark.skip("bind not supported"))

        if metafunc.definition.name == "test_parameter_schema":
            if not quirks.features.statement_bind:
                marks.append(pytest.mark.skip("bind not supported"))
            elif not quirks.features.statement_get_parameter_schema:
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

        combinations.append(pytest.param(driver_param, id=driver_param, marks=marks))

    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )


class TestStatement:
    def test_parameter_execute(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        # N.B. no need to test execute_update since the regular bind tests
        # cover that
        parameters = pyarrow.RecordBatch.from_pydict({"0": [1, 2, 3, 4]})
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                f"SELECT 1 + {driver.bind_parameter(1)}"
            )
            cursor.adbc_statement.bind(parameters)
            cursor.adbc_statement.prepare()
            handle, _ = cursor.adbc_statement.execute_query()
            result = pyarrow.RecordBatchReader._import_from_c(handle.address).read_all()
        assert result[0].to_pylist() == [2, 3, 4, 5]

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
    ) -> None:
        if not driver.features.connection_transactions:
            pytest.skip("Driver does not support transactions")

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

    def test_rows_affected(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        table_name = "test_rows_affected"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            try:
                cursor.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            cursor.adbc_statement.set_sql_query(f"CREATE TABLE {table_name} (id INT)")
            rows_affected = cursor.adbc_statement.execute_update()

            if (
                driver.features.statement_rows_affected
                and driver.features.statement_rows_affected_ddl
            ):
                assert rows_affected == 0
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"INSERT INTO {table_name} (id) VALUES (1)"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"UPDATE {table_name} SET id = id + 1 WHERE id = 1"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                f"DELETE FROM {table_name} WHERE id = 2"
            )
            rows_affected = cursor.adbc_statement.execute_update()
            if driver.features.statement_rows_affected:
                assert rows_affected == 1
            else:
                assert rows_affected == -1

            cursor.adbc_statement.set_sql_query(
                driver.drop_table(table_name="test_rows_affected")
            )
            try:
                cursor.adbc_statement.execute_update()
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
