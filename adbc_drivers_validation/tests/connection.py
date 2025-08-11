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
Tests of connection-level features.

To use: import TestConnection and generate_tests, and from your own
pytest_generate_tests hook, call generate_tests.
"""

import typing

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import compare, model


def generate_tests(quirks: model.DriverQuirks, metafunc) -> None:
    """Parameterize the tests in this module for the given driver."""
    if metafunc.definition.name == "test_get_table_schema":
        combinations = []
        queries = model.query_set(quirks.queries_path)
        for query in queries.queries.values():
            marks = []
            if not isinstance(query.query, model.SchemaQuery):
                continue
            elif not quirks.features.connection_get_table_schema:
                marks.append(pytest.mark.skip(reason="not implemented"))
            marks.extend(query.pytest_marks)

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
    else:
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
        metafunc.parametrize(
            "driver",
            [pytest.param(quirks.name, id=quirks.name, marks=marks)],
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
        self, conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
    ) -> None:
        info = conn.adbc_get_info()
        assert info.get("driver_name") == driver.driver_name
        assert info.get("vendor_name") == driver.vendor_name
        assert info.get("driver_arrow_version").startswith("v")
        driver_version = info.get("driver_version")
        assert (
            driver_version.startswith("v")
            or driver_version == "unknown"
            or driver_version == "unknown-dev"
        )

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

    def test_get_objects_table(
        self, conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
    ) -> None:
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
            cursor.execute(driver.drop_table(table_name=table_name))

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
        assert table_id not in tables

        with conn.cursor() as cursor:
            cursor.adbc_ingest(table_name, data)

        objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
        tables = [
            (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
            for obj in objects
            for schema in obj["catalog_db_schemas"]
            for table in schema["db_schema_tables"]
        ]
        assert list(sorted(set(tables))) == list(sorted(tables))
        assert table_id in tables

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

        objects = (
            conn.adbc_get_objects(depth="tables", table_name_filter=table_name)
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

    def test_get_objects_column(
        self, conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
    ) -> None:
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
            cursor.execute(driver.drop_table(table_name=table_name))

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
        for catalog, schema, table, column in columns:
            assert (catalog, schema, table) != table_id

        with conn.cursor() as cursor:
            cursor.adbc_ingest(table_name, data)

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

        objects = (
            conn.adbc_get_objects(depth="columns", table_name_filter=table_name)
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

        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_name,
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
        self, conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
    ) -> None:
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
        with conn.cursor() as cursor:
            cursor.execute(driver.drop_table(table_name=table_name))
            cursor.adbc_ingest(table_name, data)

        objects = (
            conn.adbc_get_objects(
                depth="columns",
                catalog_filter=driver.features.current_catalog,
                db_schema_filter=driver.features.current_schema,
                table_name_filter=table_name,
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
                "table": table_name,
                "column_name": "ints",
                "ordinal_position": 1,
            },
        )
        compare.match_fields(
            columns[1],
            {
                "catalog": driver.features.current_catalog,
                "schema": driver.features.current_schema,
                "table": table_name,
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
                cursor.execute(driver.drop_table(table_name=table))

            for stmt in driver.sample_ddl_constraints:
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
                "constraint_column_names": [],
                "constraint_column_usage": None,
            },
        )
        compare.match_fields(
            constraints[1],
            {
                "constraint_type": "CHECK",
                "constraint_column_names": ["a"],
                "constraint_column_usage": None,
            },
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

    def test_get_table_schema(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: model.Query,
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
        compare.compare_schemas(expected_schema, schema)
