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


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_current_catalog(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
) -> None:
    assert conn.adbc_current_catalog == driver.features.current_catalog


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_current_db_schema(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
) -> None:
    assert conn.adbc_current_db_schema == driver.features.current_schema


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_get_info(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
) -> None:
    info = conn.adbc_get_info()
    assert info.get("driver_name") == driver.driver_name
    assert info.get("vendor_name") == driver.vendor_name


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_get_objects_catalog(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
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


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_get_objects_schema(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
) -> None:
    objects = conn.adbc_get_objects(depth="db_schemas").read_all().to_pylist()
    schemas = [
        (obj["catalog_name"], schema["db_schema_name"])
        for obj in objects
        for schema in obj["catalog_db_schemas"]
    ]
    assert list(sorted(set(schemas))) == list(sorted(schemas))
    assert (driver.features.current_catalog, driver.features.current_schema) in schemas

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
    assert (driver.features.current_catalog, driver.features.current_schema) in schemas

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
    assert (driver.features.current_catalog, driver.features.current_schema) in schemas

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
    assert (driver.features.current_catalog, driver.features.current_schema) in schemas

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


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_get_objects_table(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
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
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    objects = conn.adbc_get_objects(depth="tables").read_all().to_pylist()
    tables = [
        (obj["catalog_name"], schema["db_schema_name"], table["table_name"])
        for obj in objects
        for schema in obj["catalog_db_schemas"]
        for table in schema["db_schema_tables"]
    ]
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
        conn.adbc_get_objects(depth="tables", catalog_filter="thiscatalogdoesnotexist")
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


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_get_objects_column(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
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
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

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


@pytest.mark.parametrize("driver", model.drivers_to_test(), indirect=True)
def test_get_objects_column_xdbc(
    conn: adbc_driver_manager.dbapi.Connection, driver: model.DriverQuirks
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
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
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
