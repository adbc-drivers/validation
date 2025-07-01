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

import os

import adbc_driver_manager.dbapi
import pytest
import validation.model

pytest.register_assert_rewrite("validation.compare")


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[pytest.Item]
) -> None:
    # Based on marks, add JUnit XML properties
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


@pytest.fixture(scope="module")
def conn(
    request, driver: validation.model.DriverQuirks
) -> adbc_driver_manager.dbapi.Connection:
    path = validation.model.driver_path(driver.driver)

    db_kwargs = {}
    conn_kwargs = {}

    for k, v in driver.setup.database.items():
        if isinstance(v, str):
            db_kwargs[k] = v
        else:
            db_kwargs[k] = os.environ[v.env]

    autocommit = True
    with adbc_driver_manager.dbapi.connect(
        driver=str(path),
        db_kwargs=db_kwargs,
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    ) as conn:
        make_cursor = conn.cursor

        def _cursor(*args, **kwargs) -> adbc_driver_manager.dbapi.Cursor:
            return make_cursor(adbc_stmt_kwargs=driver.setup.statement)

        conn.cursor = _cursor
        yield conn


@pytest.fixture(scope="module")
def driver(request) -> validation.model.DriverQuirks:
    driver = request.param
    return validation.model.driver_manifest(driver)


@pytest.fixture(scope="session")
def manual_test() -> None:
    """Tests that require user input."""
    if os.environ.get("RUN_MANUAL_TESTS") not in {"1", "true", "yes"}:
        pytest.skip("Skipping manual test, set RUN_MANUAL_TESTS=1")
