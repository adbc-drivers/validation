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

"""Common pytest hooks, fixtures, and configuration."""

import os
import typing

import adbc_driver_manager.dbapi
import pytest

from adbc_drivers_validation import model

# Rewrite assertions in these modules to have the friendly display
pytest.register_assert_rewrite("adbc_drivers_validation.compare")
pytest.register_assert_rewrite("adbc_drivers_validation.tests.connection")
pytest.register_assert_rewrite("adbc_drivers_validation.tests.ingest")
pytest.register_assert_rewrite("adbc_drivers_validation.tests.query")
pytest.register_assert_rewrite("adbc_drivers_validation.tests.statement")


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    """Add JUnit XML metadata based on test markers and docstrings."""
    for item in items:
        if not hasattr(item, "callspec"):
            continue

        if item.function.__doc__ is not None:
            item.user_properties.append(("doc", item.function.__doc__))

        for marker in item.iter_markers():
            if marker.name not in {"feature"}:
                continue

            for key, value in marker.kwargs.items():
                item.user_properties.append((f"{marker.name}:{key}", value))

        for key, value in item.callspec.params.items():
            if key == "driver":
                item.user_properties.append(("driver", value))
            elif key == "query":
                item.user_properties.append(("query", value.name))

                metadata = value.metadata()
                tags = metadata.get("tags", {})
                for tag_name, tag_value in tags.items():
                    if isinstance(tag_value, list):
                        for value in tag_value:
                            item.user_properties.append((f"tag:{tag_name}", value))
                    else:
                        item.user_properties.append((f"tag:{tag_name}", tag_value))


@pytest.fixture(scope="session")
def manual_test() -> None:
    """Tests that require user input."""
    if os.environ.get("RUN_MANUAL_TESTS") not in {"1", "true", "yes"}:
        pytest.skip("Skipping manual test, set RUN_MANUAL_TESTS=1")


@pytest.fixture(scope="session")
def noci() -> None:
    """Tests that should not run on CI."""
    if os.environ.get("CI") in {"1", "true", "yes"}:
        pytest.skip("fails or should not run on CI")


@pytest.fixture(scope="session")
def conn_factory(
    request,
    driver: model.DriverQuirks,
    driver_path: str,
) -> typing.Callable[[], adbc_driver_manager.dbapi.Connection]:
    db_kwargs = {}
    conn_kwargs = {}
    stmt_kwargs = {}

    for k, v in driver.setup.database.items():
        if isinstance(v, model.FromEnv):
            if v.env not in os.environ:
                pytest.skip(f"Must set {v.env}")
            db_kwargs[k] = os.environ[v.env]
        else:
            db_kwargs[k] = v

    for k, v in driver.setup.statement.items():
        if isinstance(v, model.FromEnv):
            if v.env not in os.environ:
                pytest.skip(f"Must set {v.env}")
            stmt_kwargs[k] = os.environ[v.env]
        else:
            stmt_kwargs[k] = v

    def _factory():
        autocommit = True
        conn = adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs=db_kwargs,
            conn_kwargs=conn_kwargs,
            autocommit=autocommit,
        )
        make_cursor = conn.cursor

        # Inject the default args here
        def _cursor(*args, **kwargs) -> adbc_driver_manager.dbapi.Cursor:
            return make_cursor(adbc_stmt_kwargs=stmt_kwargs)

        conn.cursor = _cursor
        return conn

    return _factory


@pytest.fixture(scope="module")
def conn(
    conn_factory: typing.Callable[[], adbc_driver_manager.dbapi.Connection],
) -> adbc_driver_manager.dbapi.Connection:
    with conn_factory() as conn:
        yield conn
