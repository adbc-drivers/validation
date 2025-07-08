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

import abc
import dataclasses
import functools
import itertools
import os
import platform
import re
import tomllib
import typing
from pathlib import Path

import adbc_driver_manager.dbapi
import pyarrow
import pytest

from . import arrowjson, utils

ROOT = Path(__file__).parent
DRIVER_EXTENSION = {"Windows": "dll", "Darwin": "dylib"}.get(platform.system(), "so")


@functools.cache
def query(path: Path) -> str:
    with path.open() as f:
        return f.read().strip()


@functools.cache
def query_meta(path: Path) -> dict[str, typing.Any]:
    with path.open("rb") as f:
        return tomllib.load(f)


@functools.cache
def query_schema(path: Path) -> pyarrow.Schema:
    with path.open("r") as f:
        return arrowjson.load_schema(f)


@functools.cache
def query_table(path: Path, schema: pyarrow.Schema) -> pyarrow.Table:
    with path.open("r") as f:
        return arrowjson.load_table(f, schema)


@dataclasses.dataclass
class FromEnv:
    env: str

    def get_or_raise(self) -> str:
        value = os.environ.get(self.env)
        if value is None:
            raise ValueError(f"Must set {self.env}")
        return value


@dataclasses.dataclass
class DriverSetup:
    database: dict[str, str | FromEnv]
    connection: dict[str, str | FromEnv]
    statement: dict[str, str | FromEnv]


@dataclasses.dataclass
class DriverFeatures:
    connection_get_table_schema: bool = False
    connection_transactions: bool = False
    statement_bulk_ingest: bool = False
    statement_bulk_ingest_catalog: bool = False
    statement_bulk_ingest_schema: bool = False
    statement_bulk_ingest_temporary: bool = False
    statement_execute_schema: bool = False
    statement_get_parameter_schema: bool = False
    statement_prepare: bool = True
    _current_catalog: str | FromEnv | None = None
    _current_schema: str | FromEnv | None = None
    supported_xdbc_fields: list[str] = dataclasses.field(default_factory=list)

    def __init__(
        self,
        *,
        connection_get_table_schema=False,
        connection_transactions=False,
        statement_bulk_ingest=False,
        statement_bulk_ingest_catalog=False,
        statement_bulk_ingest_schema=False,
        statement_bulk_ingest_temporary=False,
        statement_execute_schema=False,
        statement_get_parameter_schema=False,
        statement_prepare=True,
        current_catalog=None,
        current_schema=None,
        supported_xdbc_fields=None,
    ):
        self.connection_get_table_schema = connection_get_table_schema
        self.connection_transactions = connection_transactions
        self.statement_bulk_ingest = statement_bulk_ingest
        self.statement_bulk_ingest_catalog = statement_bulk_ingest_catalog
        self.statement_bulk_ingest_schema = statement_bulk_ingest_schema
        self.statement_bulk_ingest_temporary = statement_bulk_ingest_temporary
        self.statement_execute_schema = statement_execute_schema
        self.statement_get_parameter_schema = statement_get_parameter_schema
        self.statement_prepare = statement_prepare
        self._current_catalog = current_catalog
        self._current_schema = current_schema
        self.supported_xdbc_fields = supported_xdbc_fields or []

    @property
    def current_catalog(self) -> str | None:
        if isinstance(self._current_catalog, FromEnv):
            return self._current_catalog.get_or_raise()
        return self._current_catalog

    @property
    def current_schema(self) -> str | None:
        if isinstance(self._current_schema, FromEnv):
            return self._current_schema.get_or_raise()
        return self._current_schema


class DriverQuirks(abc.ABC):
    @property
    @abc.abstractmethod
    def queries_path(self) -> Path: ...

    def bind_parameter(self, index: int) -> str:
        """
        Return a bind parameter placeholder.

        Parameters
        ----------
        index : int
            The index of the parameter, starting from 1.
        """
        return "?"

    def drop_table(
        self, *, table_name: str, if_exists: bool = True, temporary: bool = False
    ) -> str:
        """
        Drop a table.

        Parameters
        ----------
        cursor : adbc_driver_manager.dbapi.Cursor
            The cursor to execute the query.
        table_name : str
            The name of the table to drop.
        if_exists : bool
            If True, do not raise an error if the table does not exist.
        temporary : bool
            If True, the table is a temporary table.
        """
        if temporary:
            raise NotImplementedError
        if if_exists:
            return f"DROP TABLE IF EXISTS {table_name}"
        else:
            return f"DROP TABLE {table_name}"

    @abc.abstractmethod
    def is_table_not_found(self, table_name: str, error: Exception) -> bool:
        """Check if the error indicates a table not found."""
        ...

    def qualify_temp_table(
        self, cursor: adbc_driver_manager.dbapi.Cursor, name: str
    ) -> str:
        """
        Return the fully escaped name of a temporary table.
        """
        raise NotImplementedError

    def split_statement(self, statement: str) -> list[str]:
        """
        Split a SQL statement into individual statements.

        Some vendors can't handle multiple queries in a single statement.
        """
        return [statement]


@dataclasses.dataclass
class IngestQuery:
    input_schema_path: Path
    input_path: Path
    expected_schema_path: Path | None = None
    expected_path: Path | None = None

    def input_schema(self) -> str:
        return query_schema(self.input_schema_path)

    def input(self) -> pyarrow.Table:
        return query_table(self.input_path, self.input_schema())

    def expected_schema(self) -> str:
        return query_schema(self.expected_schema_path or self.input_schema_path)

    def expected(self) -> pyarrow.Table:
        return query_table(
            self.expected_path or self.input_path, self.expected_schema()
        )


@dataclasses.dataclass
class SelectQuery:
    #: Query used to select data.
    query_path: Path
    #: Schema of the result set.
    expected_schema_path: Path
    #: Data of the result set.
    expected_path: Path
    #: Optional query used to set up the database.
    setup_query_path: Path | None = None
    #: Optional query used to insert data via bind parameters.
    bind_query_path: Path | None = None
    #: Schema of the bind parameters.
    bind_schema_path: Path | None = None
    #: Data for the bind parameters.
    bind_path: Path | None = None

    def setup_query(self) -> str | None:
        if not self.setup_query_path:
            return None
        return query(self.setup_query_path)

    def bind_query(self, driver: DriverQuirks) -> str:
        if not self.bind_query_path:
            return None

        raw_query = query(self.bind_query_path)
        param = re.compile(r"\$(\d+)")
        # Replace bind parameters with driver-specific syntax

        def repl(match: re.Match[str]) -> str:
            index = int(match.group(1))
            return driver.bind_parameter(index)

        return param.sub(repl, raw_query)

    def query(self) -> str:
        return query(self.query_path)

    def expected_schema(self) -> pyarrow.Schema:
        return query_schema(self.expected_schema_path)

    def expected_result(self) -> pyarrow.Table:
        return query_table(self.expected_path, self.expected_schema())

    def bind_schema(self) -> pyarrow.Schema:
        if not self.bind_schema_path:
            return None
        return query_schema(self.bind_schema_path)

    def bind_data(self) -> pyarrow.Table:
        if not self.bind_path:
            return None
        return query_table(self.bind_path, self.bind_schema())


@dataclasses.dataclass
class SchemaQuery:
    expected_schema_path: Path
    setup_query_path: Path

    def setup_query(self) -> str | None:
        if not self.setup_query_path:
            return None
        return query(self.setup_query_path)

    def expected_schema(self) -> pyarrow.Schema:
        return query_schema(self.expected_schema_path)


@dataclasses.dataclass
class Query:
    name: str
    query: IngestQuery | SchemaQuery | SelectQuery
    metadata_paths: list[Path] | None = None
    pytest_marks: list = dataclasses.field(default_factory=list)

    def metadata(self) -> dict[str, typing.Any]:
        md = {}
        for metadata_path in reversed(self.metadata_paths or []):
            m = query_meta(metadata_path)
            utils.merge_into(md, m)
        return md

    @classmethod
    def merge(
        cls,
        name: str,
        parent: typing.Self | None,
        *,
        metadata_path: Path | None = None,
        **kwargs,
    ) -> typing.Self:
        params = {}
        metadata_paths = []
        if metadata_path:
            metadata_paths.append(metadata_path)
        if parent:
            if parent.metadata_paths:
                metadata_paths.extend(parent.metadata_paths)
            match parent.query:
                case IngestQuery():
                    params = {
                        "input_schema_path": parent.query.input_schema_path,
                        "input_path": parent.query.input_path,
                    }
                    if parent.query.expected_schema_path:
                        params["expected_schema_path"] = (
                            parent.query.expected_schema_path
                        )
                    if parent.query.expected_path:
                        params["expected_path"] = parent.query.expected_path
                case SchemaQuery():
                    params = {
                        "expected_schema_path": parent.query.expected_schema_path,
                        "setup_query_path": parent.query.setup_query_path,
                    }
                case SelectQuery():
                    params = {
                        "query_path": parent.query.query_path,
                        "expected_schema_path": parent.query.expected_schema_path,
                        "expected_path": parent.query.expected_path,
                    }
                    if parent.query.setup_query_path:
                        params["setup_query_path"] = parent.query.setup_query_path
                    # TODO: we also want to test with ExecuteQuery so perhaps
                    # absent a specific bind query, we should bind the
                    # parameters to the regular query
                    if parent.query.bind_query_path:
                        params["bind_query_path"] = parent.query.bind_query_path
                        params["bind_schema_path"] = parent.query.bind_schema_path
                        params["bind_path"] = parent.query.bind_path

        for key, value in kwargs.items():
            if value is not None:
                params[key] = value

        if all(key in params for key in {"input_schema_path", "input_path"}) and all(
            key
            in {
                "input_schema_path",
                "input_path",
                "expected_schema_path",
                "expected_path",
            }
            for key in params
        ):
            query = IngestQuery(**params)
        elif params.keys() == {"expected_schema_path", "setup_query_path"}:
            query = SchemaQuery(**params)
        elif all(
            key in params
            for key in {
                "query_path",
                "expected_schema_path",
                "expected_path",
            }
        ) and all(
            key
            in {
                "query_path",
                "expected_schema_path",
                "expected_path",
                "setup_query_path",
                "bind_query_path",
                "bind_schema_path",
                "bind_path",
            }
            for key in params
        ):
            query = SelectQuery(**params)
        else:
            raise ValueError(f"{name}: unknown query type with files {params.keys()}")

        for key, value in params.items():
            if value is None:
                raise FileNotFoundError(f"{name}: missing {key}")

        return cls(
            name=name,
            query=query,
            metadata_paths=metadata_paths,
        )


@dataclasses.dataclass
class QuerySet:
    queries: dict[str, Query]

    @classmethod
    def load(self, root: Path, parent: typing.Self | None = None) -> typing.Self:
        def remove_all_suffixes(path: Path) -> Path:
            while path.suffix:
                path = path.with_suffix("")
            return path

        def case_name(path: Path) -> str:
            return str(remove_all_suffixes(path).relative_to(root))

        files = itertools.chain(
            root.rglob("*.sql"),
            root.rglob("*.json"),
            root.rglob("*.toml"),
        )
        files = sorted(files, key=lambda path: (case_name(path), path))
        files = itertools.groupby(files, key=case_name)

        queries = parent.queries.copy() if parent else {}
        for query_case, query_files in files:
            parent_query = None
            if parent:
                parent_query = parent.queries.get(query_case)

            params = {}
            metadata_path: Path | None = None
            for query_file in query_files:
                if query_file.suffixes == [".sql"]:
                    params["query_path"] = query_file
                elif query_file.suffixes == [".bind", ".sql"]:
                    params["bind_query_path"] = query_file
                elif query_file.suffixes == [".schema", ".json"]:
                    params["expected_schema_path"] = query_file
                elif query_file.suffixes == [".json"]:
                    params["expected_path"] = query_file
                elif query_file.suffixes == [".input", ".json"]:
                    params["input_path"] = query_file
                elif query_file.suffixes == [".input", ".schema", ".json"]:
                    params["input_schema_path"] = query_file
                elif query_file.suffixes == [".bind", ".json"]:
                    params["bind_path"] = query_file
                elif query_file.suffixes == [".bind", ".schema", ".json"]:
                    params["bind_schema_path"] = query_file
                elif query_file.suffixes == [".setup", ".sql"]:
                    params["setup_query_path"] = query_file
                elif query_file.suffixes == [".toml"]:
                    metadata_path = query_file
                else:
                    raise ValueError(
                        f"{query_case}: unexpected query file: {query_file}"
                    )
            query = Query.merge(
                name=query_case,
                parent=parent_query,
                metadata_path=metadata_path,
                **params,
            )

            if skip := query.metadata().get("skip"):
                query.pytest_marks.append(pytest.mark.skip(reason=skip))

            tags = query.metadata().get("tags", {})
            if broken_vendor := tags.get("broken-vendor", False):
                query.pytest_marks.append(pytest.mark.xfail(reason=broken_vendor))

            if query.metadata().get("hide"):
                # Some queries are entirely redundant (e.g. if a vendor simply
                # does not support a type, there's no need to test it or
                # report it)
                if query_case in queries:
                    del queries[query_case]
            else:
                queries[query_case] = query
        return QuerySet(queries=queries)


@functools.cache
def base_query_set() -> QuerySet:
    # Not a real driver, load the default/"generic" queries
    return QuerySet.load(ROOT / "queries")


@functools.cache
def query_set(path: Path) -> QuerySet:
    return QuerySet.load(path, parent=base_query_set())
