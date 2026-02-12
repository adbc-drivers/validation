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
Tests of bulk ingestion.

To use: import TestIngest and generate_tests, and from your own
pytest_generate_tests hook, call generate_tests.
"""

import itertools
import re
import time
import typing

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from adbc_drivers_validation import compare, model
from adbc_drivers_validation.model import Query
from adbc_drivers_validation.utils import setup_statement


def generate_tests(
    all_quirks: list[model.DriverQuirks],
    metafunc,
    *,
    ingest_mode_queries={"ingest/string"},
) -> None:
    """Parameterize the tests in this module for the given driver."""
    param_string = ""
    combinations = []

    for quirks in all_quirks:
        driver_param = f"{quirks.name}:{quirks.short_version}"
        enabled = {
            "test_not_null": quirks.features.statement_bulk_ingest,
            "test_temporary": quirks.features.statement_bulk_ingest_temporary,
            "test_schema": quirks.features.statement_bulk_ingest_schema,
            "test_catalog": quirks.features.statement_bulk_ingest_catalog,
        }.get(metafunc.definition.name, None)
        if enabled is not None:
            marks = []
            if not enabled:
                marks.append(pytest.mark.skip(reason="not implemented"))

            param_string = "driver"
            combinations.append(
                pytest.param(driver_param, id=driver_param, marks=marks)
            )
            continue

        param_string = "driver,query"
        for query in quirks.query_set.queries.values():
            marks = []
            marks.extend(query.pytest_marks)

            if not isinstance(query.query, model.IngestQuery):
                continue
            if not quirks.features.statement_bulk_ingest:
                marks.append(pytest.mark.skip(reason="not implemented"))

            if (
                metafunc.definition.name != "test_create"
                and query.name not in ingest_mode_queries
            ):
                # There's no need to test every case on every mode
                continue

            combinations.append(
                pytest.param(
                    driver_param, query, id=f"{driver_param}:{query.name}", marks=marks
                )
            )

    metafunc.parametrize(
        param_string,
        combinations,
        scope="module",
        indirect=["driver"],
    )


_SANITIZE_TABLE_NAME = re.compile(r"[^a-zA-Z0-9_]")


def make_table_name(prefix: str, query: Query) -> str:
    # Try to avoid table based rate limits by using a unique table per case.
    suffix = query.name.rsplit("/", 1)[-1]
    return _SANITIZE_TABLE_NAME.sub("_", f"{prefix}_{suffix}")


class TestIngest:
    def test_create(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_create", query)
        data = subquery.input()

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            with setup_statement(query, cursor):
                modified = cursor.adbc_ingest(table_name, data, mode="create")
                if driver.features.statement_rows_affected:
                    assert modified == len(data)
                else:
                    assert modified == -1

        fields = []
        for field in data.schema:
            fields.append(driver.quote_identifier(field.name))
        select = f"SELECT {', '.join(fields)} FROM {driver.quote_identifier(table_name)} ORDER BY {fields[0]} ASC"
        with conn.cursor() as cursor:
            with setup_statement(query, cursor):
                cursor.adbc_statement.set_sql_query(select)
                handle, _ = cursor.adbc_statement.execute_query()
                with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                    result = reader.read_all()

        # TODO: we should also inspect the type name and make sure it matches the
        # metadata
        expected = subquery.expected()
        compare.compare_tables(expected, result, query.metadata())

    def test_append(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_append", query)
        data = subquery.input()
        data2 = pyarrow.Table.from_pydict(
            {
                data.schema[0].name: list(range(len(data), len(data) * 2)),
                data.schema[1].name: data[1],
            },
            schema=data.schema,
        )

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            modified = cursor.adbc_ingest(table_name, data, mode="create")
            if driver.features.statement_rows_affected:
                assert modified == len(data)
            else:
                assert modified == -1

            modified = cursor.adbc_ingest(table_name, data2, mode="append")
            if driver.features.statement_rows_affected:
                assert modified == len(data2)
            else:
                assert modified == -1

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")
        select = f"SELECT {idx}, {value} FROM {driver.quote_identifier(table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        expected = subquery.expected()
        expected2 = pyarrow.Table.from_pydict(
            {
                expected.schema[0].name: list(range(len(expected), len(expected) * 2)),
                expected.schema[1].name: expected[1],
            },
            schema=expected.schema,
        )
        compare.compare_tables(
            pyarrow.concat_tables([expected, expected2]),
            result,
            query.metadata(),
        )

    def test_append_fail(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_append_fail", query)
        data = subquery.input()

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            with pytest.raises(adbc_driver_manager.dbapi.Error) as excinfo:
                cursor.adbc_ingest(table_name, data, mode="append")

        assert driver.is_table_not_found(table_name, excinfo.value)

    def test_createappend(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_createappend", query)
        data = subquery.input()
        data2 = pyarrow.Table.from_pydict(
            {
                data.schema[0].name: list(range(len(data), len(data) * 2)),
                data.schema[1].name: data[1],
            },
            schema=data.schema,
        )

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            modified = cursor.adbc_ingest(table_name, data, mode="create_append")
            if driver.features.statement_rows_affected:
                assert modified == len(data)
            else:
                assert modified == -1

            modified = cursor.adbc_ingest(table_name, data2, mode="create_append")
            if driver.features.statement_rows_affected:
                assert modified == len(data2)
            else:
                assert modified == -1

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")
        select = f"SELECT {idx}, {value} FROM {driver.quote_identifier(table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        # TODO: we should also inspect the type name and make sure it matches the
        # metadata
        expected = subquery.expected()
        expected2 = pyarrow.Table.from_pydict(
            {
                expected.schema[0].name: list(range(len(expected), len(expected) * 2)),
                expected.schema[1].name: expected[1],
            },
            schema=expected.schema,
        )
        compare.compare_tables(
            pyarrow.concat_tables([expected, expected2]),
            result,
            query.metadata(),
        )

    def test_replace(
        self, driver, conn: adbc_driver_manager.dbapi.Connection, query: Query
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_replace", query)
        data = subquery.input()
        data2 = data.slice(0, 1)

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            cursor.adbc_ingest(table_name, data, mode="replace")
            modified = cursor.adbc_ingest(table_name, data, mode="replace")
            if driver.features.statement_rows_affected:
                assert modified == len(data)
            else:
                assert modified == -1

            if driver.name == "bigquery":
                # BigQuery rate-limits metadata operations
                time.sleep(5)
            modified = cursor.adbc_ingest(table_name, data2, mode="replace")
            if driver.features.statement_rows_affected:
                assert modified == len(data2)
            else:
                assert modified == -1

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")
        select = f"SELECT {idx}, {value} FROM {driver.quote_identifier(table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        expected = subquery.expected().slice(0, 1)
        compare.compare_tables(expected, result, query.metadata())

    def test_replace_noop(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_replace_noop", query)
        data = subquery.input()

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            modified = cursor.adbc_ingest(table_name, data, mode="replace")
            if driver.features.statement_rows_affected:
                assert modified == len(data)
            else:
                assert modified == -1

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")
        select = f"SELECT {idx}, {value} FROM {driver.quote_identifier(table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        expected = subquery.expected()
        compare.compare_tables(expected, result, query.metadata())

    def test_not_null(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        # TODO: this should also be parametrized by a single query so that
        # drivers can use ingest_mode_queries to run this test with other options
        if "xdbc_nullable" not in driver.features.supported_xdbc_fields:
            pytest.skip(reason="not implemented")

        table_name = "test_ingest_not_null"
        data = pyarrow.Table.from_pydict(
            {
                "idx": [1, 2, 3],
                "value": ["foo", "bar", "baz"],
            },
            schema=pyarrow.schema(
                [
                    pyarrow.field("idx", pyarrow.int64()),
                    pyarrow.field("value", pyarrow.string(), nullable=False),
                ]
            ),
        )

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            cursor.adbc_ingest(table_name, data, mode="create")

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

        assert len(columns) == 2
        compare.match_fields(
            columns[0],
            {
                "catalog": driver.features.current_catalog,
                "schema": driver.features.current_schema,
                "table": table_name,
                "column_name": "idx",
                "ordinal_position": 1,
                "xdbc_nullable": 1,
                "xdbc_is_nullable": "YES",
            },
        )
        compare.match_fields(
            columns[1],
            {
                "catalog": driver.features.current_catalog,
                "schema": driver.features.current_schema,
                "table": table_name,
                "column_name": "value",
                "ordinal_position": 2,
                "xdbc_nullable": 0,
                "xdbc_is_nullable": "NO",
            },
        )

    def test_temporary(
        self,
        driver: model.DriverQuirks,
        conn_factory: typing.Callable[[], adbc_driver_manager.dbapi.Connection],
    ) -> None:
        data1 = pyarrow.Table.from_pydict(
            {
                "idx": [1, 2, 3],
                "value": ["foo", "bar", "baz"],
            }
        )
        data2 = pyarrow.Table.from_pydict(
            {
                "idx": [4, 5, 6],
                "value": ["qux", "quux", "spam"],
            }
        )
        table_name = "test_ingest_temporary"

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")

        with conn_factory() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute(driver.drop_table(table_name=table_name))
                except adbc_driver_manager.Error as e:
                    # Some databases have no way to do DROP IF EXISTS
                    if not driver.is_table_not_found(table_name=table_name, error=e):
                        raise
                cursor.adbc_ingest(table_name, data1, temporary=True)
                cursor.adbc_ingest(table_name, data2, temporary=False)

            with conn.cursor() as cursor:
                assert driver.features.current_schema is not None
                normal_table = driver.quote_identifier(
                    driver.features.current_schema, table_name
                )
                temp_table = driver.qualify_temp_table(cursor, table_name)
                select_normal = (
                    f"SELECT {idx}, {value} FROM {normal_table} ORDER BY {idx} ASC"
                )
                select_temporary = (
                    f"SELECT {idx}, {value} FROM {temp_table} ORDER BY {idx} ASC"
                )

                cursor.adbc_statement.set_sql_query(select_normal)
                handle, _ = cursor.adbc_statement.execute_query()
                with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                    result_normal = reader.read_all()

                cursor.adbc_statement.set_sql_query(select_temporary)
                handle, _ = cursor.adbc_statement.execute_query()
                with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                    result_temporary = reader.read_all()

        compare.compare_tables(data1, result_temporary)
        compare.compare_tables(data2, result_normal)

    def test_schema(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        data = pyarrow.Table.from_pydict(
            {
                "idx": [1, 2, 3],
                "value": ["foo", "bar", "baz"],
            }
        )
        table_name = "test_ingest_schema"
        schema_name = driver.features.secondary_schema
        assert schema_name is not None
        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    driver.drop_table(
                        table_name=table_name,
                        schema_name=schema_name,
                    )
                )
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            cursor.adbc_ingest(
                table_name,
                data,
                db_schema_name=schema_name,
            )

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")
        select = f"SELECT {idx}, {value} FROM {driver.quote_identifier(schema_name, table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        compare.compare_tables(data, result)

    def test_catalog(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
    ) -> None:
        data = pyarrow.Table.from_pydict(
            {
                "idx": [1, 2, 3],
                "value": ["foo", "bar", "baz"],
            }
        )
        table_name = "test_ingest_catalog"
        schema_name = driver.features.secondary_catalog_schema
        catalog_name = driver.features.secondary_catalog
        assert schema_name is not None
        assert catalog_name is not None
        with conn.cursor() as cursor:
            try:
                cursor.execute(
                    driver.drop_table(
                        table_name=table_name,
                        schema_name=schema_name,
                        catalog_name=catalog_name,
                    )
                )
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise
            cursor.adbc_ingest(
                table_name,
                data,
                db_schema_name=schema_name,
                catalog_name=catalog_name,
            )

        idx = driver.quote_identifier("idx")
        value = driver.quote_identifier("value")
        select = f"SELECT {idx}, {value} FROM {driver.quote_identifier(catalog_name, schema_name, table_name)} ORDER BY {idx} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        compare.compare_tables(data, result)

    def test_create_multiple_batches(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_multiple_batches", query)
        data = subquery.input()

        # Create multiple batches by duplicating the input
        num_batches = 3
        batches = []
        for i in range(num_batches):
            batch_data = pyarrow.Table.from_pydict(
                {
                    data.schema[0].name: list(
                        range(i * len(data), (i + 1) * len(data))
                    ),
                    data.schema[1].name: data[1],
                },
                schema=data.schema,
            )
            batches.extend(batch_data.to_batches())

        reader = pyarrow.RecordBatchReader.from_batches(data.schema, batches)

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            modified = cursor.adbc_ingest(table_name, reader, mode="create")
            # We expect num_batches * len(data) rows
            if driver.features.statement_rows_affected:
                assert modified == num_batches * len(data)
            else:
                assert modified == -1

        # Read back and verify
        fields = []
        for field in data.schema:
            fields.append(driver.quote_identifier(field.name))
        select = f"SELECT {', '.join(fields)} FROM {driver.quote_identifier(table_name)} ORDER BY {fields[0]} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        # Build expected result
        expected = subquery.expected()
        expected_tables = []
        for i in range(num_batches):
            expected_i = pyarrow.Table.from_pydict(
                {
                    expected.schema[0].name: list(
                        range(i * len(expected), (i + 1) * len(expected))
                    ),
                    expected.schema[1].name: expected[1],
                },
                schema=expected.schema,
            )
            expected_tables.append(expected_i)

        compare.compare_tables(
            pyarrow.concat_tables(expected_tables),
            result,
            query.metadata(),
        )

    def test_create_large_batch(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        query: Query,
    ) -> None:
        subquery = query.query
        assert isinstance(subquery, model.IngestQuery)

        table_name = make_table_name("test_ingest_large_batch", query)
        data = subquery.input()

        # Create a large batch by concatenating the input multiple times
        num_repeats = 1000
        large_data = pyarrow.Table.from_pydict(
            {
                data.schema[0].name: list(range(num_repeats * len(data[0]))),
                data.schema[1].name: list(
                    itertools.chain(*[data[1] for _ in range(num_repeats)])
                ),
            },
            schema=data.schema,
        )

        with conn.cursor() as cursor:
            try:
                cursor.execute(driver.drop_table(table_name=table_name))
            except adbc_driver_manager.Error as e:
                # Some databases have no way to do DROP IF EXISTS
                if not driver.is_table_not_found(table_name=table_name, error=e):
                    raise

            modified = cursor.adbc_ingest(table_name, large_data, mode="create")
            if driver.features.statement_rows_affected:
                assert modified == num_repeats * len(data)
            else:
                assert modified == -1

        # Read back and verify
        fields = []
        for field in data.schema:
            fields.append(driver.quote_identifier(field.name))
        select = f"SELECT {', '.join(fields)} FROM {driver.quote_identifier(table_name)} ORDER BY {fields[0]} ASC"
        with conn.cursor() as cursor:
            cursor.adbc_statement.set_sql_query(select)
            handle, _ = cursor.adbc_statement.execute_query()
            with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                result = reader.read_all()

        # Build expected result
        expected = subquery.expected()
        expected_tables = []
        for i in range(num_repeats):
            expected_i = pyarrow.Table.from_pydict(
                {
                    expected.schema[0].name: list(
                        range(i * len(expected), (i + 1) * len(expected))
                    ),
                    expected.schema[1].name: expected[1],
                },
                schema=expected.schema,
            )
            expected_tables.append(expected_i)

        compare.compare_tables(
            pyarrow.concat_tables(expected_tables),
            result,
            query.metadata(),
        )
