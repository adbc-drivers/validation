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

from adbc_drivers_validation import model


@pytest.fixture
def requires_transactions(driver: model.DriverQuirks) -> None:
    if not driver.features.connection_transactions:
        pytest.skip("Driver does not support transactions")


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_transaction_toggle(
    conn: adbc_driver_manager.dbapi.Connection, requires_transactions: None
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
