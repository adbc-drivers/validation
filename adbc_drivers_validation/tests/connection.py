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
Tests of connection-level features.

To use: import TestConnection and generate_tests, and from your own
pytest_generate_tests hook, call generate_tests.
"""

import re
import typing

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import compare, model
from adbc_drivers_validation.utils import scoped_trace


def generate_tests(all_quirks: list[model.DriverQuirks], metafunc) -> None:
    """Parameterize the tests in this module for the given driver."""
    combinations = []
    for quirks in all_quirks:
        driver_param = f"{quirks.name}:{quirks.short_version}"
        marks = []
        if (
            enabled := {
                "test_get_objects_constraints_check": quirks.features.get_objects_constraints_check,
                "test_get_objects_constraints_foreign": quirks.features.get_objects_constraints_foreign,
                "test_get_objects_constraints_primary": quirks.features.get_objects_constraints_primary,
                "test_get_objects_constraints_unique": quirks.features.get_objects_constraints_unique,
            }.get(metafunc.definition.name)
        ) is not None:
            if not enabled:
                marks.append(pytest.mark.skip(reason="not implemented"))
        combinations.append(pytest.param(driver_param, id=driver_param, marks=marks))
    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )


class TestConnection:
    def test_current_catalog(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        assert conn.adbc_current_catalog == driver.features.current_catalog

    def test_current_db_schema(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        assert conn.adbc_current_db_schema == driver.features.current_schema

    def test_set_current_catalog(
        self,
        driver: model.DriverQuirks,
        conn_factory: typing.Callable[[], adbc_driver_manager.dbapi.Connection],
    ):
        if not driver.features.connection_set_current_catalog:
            pytest.skip("not implemented")

        with conn_factory() as conn:
            assert conn.adbc_current_catalog == driver.features.current_catalog
            conn.adbc_current_catalog = driver.features.secondary_catalog
            assert conn.adbc_current_catalog == driver.features.secondary_catalog
            conn.adbc_current_catalog = driver.features.current_catalog
            assert conn.adbc_current_catalog == driver.features.current_catalog

    def test_set_current_schema(
        self,
        driver: model.DriverQuirks,
        conn_factory: typing.Callable[[], adbc_driver_manager.dbapi.Connection],
    ):
        if not driver.features.connection_set_current_schema:
            pytest.skip("not implemented")

        with conn_factory() as conn:
            assert conn.adbc_current_db_schema == driver.features.current_schema
            conn.adbc_current_db_schema = driver.features.secondary_schema
            assert conn.adbc_current_db_schema == driver.features.secondary_schema
            conn.adbc_current_db_schema = driver.features.current_schema
            assert conn.adbc_current_db_schema == driver.features.current_schema

    def test_get_info(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        record_property: typing.Callable[[str, typing.Any], None],
    ) -> None:
        info = conn.adbc_get_info()
        driver_version = info.get("driver_version")
        assert (
            driver_version.startswith("v")
            or driver_version == "unknown"
            or driver_version == "unknown-dirty"
        )
        record_property("driver_version", driver_version)
        assert info.get("driver_name") == driver.driver_name
        assert info.get("vendor_name") == driver.vendor_name
        vendor_version = info.get("vendor_version", "")
        if isinstance(driver.vendor_version, re.Pattern):
            assert driver.vendor_version.match(vendor_version), (
                f"{vendor_version!r} does not match {driver.vendor_version!r}"
            )
        else:
            assert vendor_version == driver.vendor_version
        assert info.get("driver_arrow_version").startswith("v")
        record_property("vendor_version", vendor_version)
        record_property("short_version", driver.short_version)

    def test_get_objects_catalog(
        self, conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
    ) -> None:
        objects = conn.adbc_get_objects(depth="catalogs").read_all().to_pylist()
        catalogs = [obj["catalog_name"] for obj in objects]
        assert list(sorted(set(catalogs))) == list(sorted(catalogs))
        assert driver.features.current_catalog in catalogs

        objects = (
            conn.adbc_get_objects(
                depth="catalogs", catalog_filter=driver.features.current_catalog
            )
            .read_all()
            .to_pylist()
        )
        catalogs = [obj["catalog_name"] for obj in objects]
        assert list(sorted(set(catalogs))) == list(sorted(catalogs))
        assert driver.features.current_catalog in catalogs

        objects = (
            conn.adbc_get_objects(
                depth="catalogs", catalog_filter="thiscatalogdoesnotexist"
            )
            .read_all()
            .to_pylist()
        )
        catalogs = [obj["catalog_name"] for obj in objects]
        assert catalogs == []

    def test_get_objects_schema(
        self, conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
    ) -> None:
        objects = conn.adbc_get_objects(depth="db_schemas").read_all().to_pylist()
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert list(sorted(set(schemas))) == list(sorted(schemas))
        assert (
            driver.features.current_catalog,
            driver.features.current_schema,
        ) in schemas

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas", catalog_filter=driver.features.current_catalog
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert list(sorted(set(schemas))) == list(sorted(schemas))
        assert (
            driver.features.current_catalog,
            driver.features.current_schema,
        ) in schemas

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas", db_schema_filter=driver.features.current_schema
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert list(sorted(set(schemas))) == list(sorted(schemas))
        assert (
            driver.features.current_catalog,
            driver.features.current_schema,
        ) in schemas

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert list(sorted(set(schemas))) == list(sorted(schemas))
        assert (
            driver.features.current_catalog,
            driver.features.current_schema,
        ) in schemas

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas", catalog_filter="thiscatalogdoesnotexist"
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert schemas == []

        objects = (
            conn.adbc_get_objects(
                depth="db_schemas", db_schema_filter="thiscatalogdoesnotexist"
            )
            .read_all()
            .to_pylist()
        )
        schemas = [
            (obj["catalog_name"], schema["db_schema_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
        ]
        assert schemas == []

    def test_get_objects_table_not_exist(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
    ) -> None:
        # N.B. table tests are split up so we can more easily override/disable
        # parts of it
        table_name = "getobjectstest2"
        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

        objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        for catalog, schema, table in tables:
            assert table != ""
        assert list(sorted(set(tables))) == list(sorted(tables))
        table_id = (
            driver.features.current_catalog,
            driver.features.current_schema,
            table_name,
        )
        assert table_id not in tables

    def test_get_objects_table_present(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id in tables

    def test_get_objects_table_invalid_catalog(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="tables", catalog_filter="thiscatalogdoesnotexist"
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id not in tables

    def test_get_objects_table_invalid_schema(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="tables", db_schema_filter="thiscatalogdoesnotexist"
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id not in tables

    def test_get_objects_table_invalid_table(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="tables", table_name_filter="thiscatalogdoesnotexist"
            )
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id not in tables

    def test_get_objects_table_exact_table(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(depth="tables", table_name_filter=table_id[2])
            .read_all()
            .to_pylist()
        )
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id in tables

    def test_get_objects_column_not_exist(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        objects = conn.adbc_get_objects(depth="columns").read_all().to_pylist()
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        table_id = (
            driver.features.current_catalog,
            driver.features.current_schema,
            "getobjectstest2",
        )
        for catalog, schema, table, column in columns:
            assert (catalog, schema, table) != table_id

    def test_get_objects_column_present(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = conn.adbc_get_objects(depth="columns").read_all().to_pylist()
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") in columns

    def test_get_objects_column_filter_column_name(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(depth="columns", column_name_filter="ints")
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") not in columns

    def test_get_objects_column_filter_table_name(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(depth="columns", table_name_filter=table_id[-1])
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") in columns
        assert len(columns) == 2

    def test_get_objects_column_filter_catalog(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns", catalog_filter=driver.features.current_catalog
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") in columns

    def test_get_objects_column_filter_schema(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert (*table_id, "ints") in columns
        assert (*table_id, "strs") in columns

    def test_get_objects_column_filter_table(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_id[-1],
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            (
                obj["catalog_name"],
                schema["db_schema_name"],
                table["table_name"],
                column["column_name"],
            )
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]
        assert list(sorted(set(columns))) == list(sorted(columns))
        assert columns == [(*table_id, "ints"), (*table_id, "strs")]

    def test_get_objects_column_xdbc(
        self,
        conn: adbc_driver_manager.dbapi.Connection,
        driver: model.DriverQuirks,
        get_objects_table,
    ) -> None:
        table_id = get_objects_table
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_id[-1],
            )
            .read_all()
            .to_pylist()
        )
        columns = [
            {
                "catalog": obj["catalog_name"],
                "schema": schema["db_schema_name"],
                "table": table["table_name"],
                **column,
            }
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
            for column in table["table_columns"]
        ]

        compare.match_fields(
            columns[0],
            {
                "catalog": driver.features.current_catalog,
                "schema": driver.features.current_schema,
                "table": table_id[-1],
                "column_name": "ints",
                "ordinal_position": 1,
            },
        )
        compare.match_fields(
            columns[1],
            {
                "catalog": driver.features.current_catalog,
                "schema": driver.features.current_schema,
                "table": table_id[-1],
                "column_name": "strs",
                "ordinal_position": 2,
            },
        )

        for column in columns:
            for field in driver.features.supported_xdbc_fields:
                assert column[field] is not None

                if field == "xdbc_nullable":
                    assert column[field] == 1
                elif field == "xdbc_is_nullable":
                    assert column[field] == "YES"

    @pytest.fixture(scope="class")
    def get_objects_table(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ):
        with conn.cursor() as cursor:
            table_name = "getobjectstest"
            schema = pyarrow.schema(
                [
                    ("ints", pyarrow.int32()),
                    ("strs", pyarrow.string()),
                ]
            )
            data = pyarrow.Table.from_pydict(
                {
                    "ints": [1, None, 42],
                    "strs": [None, "foo", "spam"],
                },
                schema=schema,
            )
            table_id = (
                driver.features.current_catalog,
                driver.features.current_schema,
                table_name,
            )
            with conn.cursor() as cursor:
                try:
                    cursor.execute(driver.drop_table(table_name=table_name))
                except adbc_driver_manager.Error as e:
                    # Some databases have no way to do DROP IF EXISTS
                    if not driver.is_table_not_found(table_name=None, error=e):
                        raise

                cursor.adbc_ingest(table_name, data)

            yield table_id

            with conn.cursor() as cursor:
                try:
                    cursor.execute(driver.drop_table(table_name=table_name))
                except adbc_driver_manager.Error as e:
                    # Some databases have no way to do DROP IF EXISTS
                    if not driver.is_table_not_found(table_name=None, error=e):
                        raise

    @pytest.fixture(scope="class")
    def get_objects_constraints(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        table_names = (
            "constraint_check",
            "constraint_unique",
            "constraint_foreign",
            "constraint_foreign_multi",
            "constraint_primary",
            "constraint_primary_multi",
            "constraint_primary_multi2",
        )
        with conn.cursor() as cursor:
            for table in table_names:
                try:
                    stmt = driver.drop_table(table_name=table)
                except adbc_driver_manager.Error as e:
                    # Some databases have no way to do DROP IF EXISTS
                    if not driver.is_table_not_found(table_name=None, error=e):
                        raise

                with scoped_trace(stmt):
                    cursor.execute(stmt)

            for stmt in driver.sample_ddl_constraints:
                with scoped_trace(stmt):
                    cursor.execute(stmt)

    def get_constraints(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        table_filter: str,
    ) -> dict[str, list[dict]]:
        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_filter,
            )
            .read_all()
            .to_pylist()
        )
        tables = {
            table["table_name"]: table["table_constraints"]
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        }
        for table, constraints in tables.items():
            assert constraints is not None, table
        return tables

    def test_get_objects_constraints_check(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        get_objects_constraints: None,
    ) -> None:
        tables = self.get_constraints(driver, conn, "constraint_check")
        assert len(tables["constraint_check"]) == 2
        constraints = list(
            sorted(
                tables["constraint_check"],
                key=lambda x: len(x["constraint_column_names"]),
            )
        )
        compare.match_fields(
            constraints[0],
            {
                "constraint_type": "CHECK",
                "constraint_column_usage": None,
            },
        )
        assert (
            constraints[0]["constraint_column_names"] == ["a"]
            or constraints[0]["constraint_column_names"] == []
        )
        compare.match_fields(
            constraints[1],
            {
                "constraint_type": "CHECK",
                "constraint_column_usage": None,
            },
        )
        # Allow any subset of columns (MSSQL in particular seems to be odd
        # about this)
        assert (
            constraints[1]["constraint_column_names"] == ["a", "b"]
            or constraints[1]["constraint_column_names"] == ["a"]
            or constraints[1]["constraint_column_names"] == ["b"]
            or constraints[1]["constraint_column_names"] == []
        )

    def test_get_objects_constraints_foreign(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        get_objects_constraints: None,
    ) -> None:
        tables = self.get_constraints(driver, conn, "constraint_foreign%")

        assert len(tables["constraint_foreign"]) == 1, repr(tables)
        compare.match_fields(
            tables["constraint_foreign"][0],
            {
                "constraint_type": "FOREIGN KEY",
                "constraint_column_names": ["b"],
                "constraint_column_usage": [
                    {
                        "fk_catalog": driver.features.current_catalog,
                        "fk_db_schema": driver.features.current_schema,
                        "fk_table": "constraint_primary",
                        "fk_column_name": "a",
                    }
                ],
            },
        )

        # Some databases don't preserve the order of columns in a multi-column
        # foreign key
        assert len(tables["constraint_foreign_multi"]) == 1, repr(tables)
        constraint = tables["constraint_foreign_multi"][0]
        compare.match_fields(
            constraint,
            {"constraint_type": "FOREIGN KEY"},
        )
        cols = constraint["constraint_column_names"]
        if driver.features.quirk_get_objects_constraints_foreign_normalized:
            assert cols == ["b", "c"]
            assert constraint["constraint_column_usage"] == [
                {
                    "fk_catalog": driver.features.current_catalog,
                    "fk_db_schema": driver.features.current_schema,
                    "fk_table": "constraint_primary_multi2",
                    "fk_column_name": "b",
                },
                {
                    "fk_catalog": driver.features.current_catalog,
                    "fk_db_schema": driver.features.current_schema,
                    "fk_table": "constraint_primary_multi2",
                    "fk_column_name": "a",
                },
            ], repr(constraint)
        else:
            assert cols == ["c", "b"]
            assert constraint["constraint_column_usage"] == [
                {
                    "fk_catalog": driver.features.current_catalog,
                    "fk_db_schema": driver.features.current_schema,
                    "fk_table": "constraint_primary_multi2",
                    "fk_column_name": "a",
                },
                {
                    "fk_catalog": driver.features.current_catalog,
                    "fk_db_schema": driver.features.current_schema,
                    "fk_table": "constraint_primary_multi2",
                    "fk_column_name": "b",
                },
            ], repr(constraint)

    def test_get_objects_constraints_primary(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        get_objects_constraints: None,
    ) -> None:
        tables = self.get_constraints(driver, conn, "constraint_primary%")

        assert len(tables["constraint_primary"]) == 1
        compare.match_fields(
            tables["constraint_primary"][0],
            {
                "constraint_type": "PRIMARY KEY",
                "constraint_column_names": ["a"],
                "constraint_column_usage": None,
            },
        )

        assert len(tables["constraint_primary_multi"]) == 1
        constraint = tables["constraint_primary_multi"][0]
        compare.match_fields(
            constraint,
            {
                "constraint_type": "PRIMARY KEY",
                "constraint_column_usage": None,
            },
        )
        if driver.features.quirk_get_objects_constraints_primary_normalized:
            assert constraint["constraint_column_names"] == ["a", "b"]
        else:
            assert constraint["constraint_column_names"] == ["b", "a"]

        assert len(tables["constraint_primary_multi2"]) == 1
        constraint = tables["constraint_primary_multi2"][0]
        compare.match_fields(
            constraint,
            {
                "constraint_type": "PRIMARY KEY",
                "constraint_column_usage": None,
            },
        )
        assert constraint["constraint_column_names"] == ["a", "b"]

    def test_get_objects_constraints_unique(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        get_objects_constraints: None,
    ) -> None:
        tables = self.get_constraints(driver, conn, "constraint_unique%")

        assert len(tables["constraint_unique"]) == 2
        constraints = list(
            sorted(
                tables["constraint_unique"],
                key=lambda x: len(x["constraint_column_names"]),
            )
        )
        compare.match_fields(
            constraints[0],
            {
                "constraint_type": "UNIQUE",
                "constraint_column_names": ["a"],
                "constraint_column_usage": None,
            },
        )

        # Even if declared as UNIQUE(c, b), some databases return [b, c]
        compare.match_fields(
            constraints[1],
            {
                "constraint_type": "UNIQUE",
                "constraint_column_usage": None,
            },
        )
        if driver.features.quirk_get_objects_constraints_unique_normalized:
            assert constraints[1]["constraint_column_names"] == ["b", "c"]
        else:
            assert constraints[1]["constraint_column_names"] == ["c", "b"]

    def test_repl(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        import code

        code.interact(local={"conn": conn, "driver": driver})
