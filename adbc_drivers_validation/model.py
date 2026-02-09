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

from . import arrowjson, query_metadata, txtcase, utils

ROOT = Path(__file__).parent
DRIVER_EXTENSION = {"Windows": "dll", "Darwin": "dylib"}.get(platform.system(), "so")


@functools.cache
def query_txtcase(path: Path) -> txtcase.TxtCase | None:
    return txtcase.try_load(path)


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


def try_txtcase(path: Path, fallback, parts: list[str], schema=None):
    args = (schema,) if schema is not None else ()
    t = query_txtcase(path)
    if t is None:
        return fallback(path, *args)

    for part in parts:
        try:
            return t.get_part(part, *args)
        except KeyError:
            continue
    raise ValueError(f"{path} does not contain any of {parts}")


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
    connection_set_current_catalog: bool = False
    connection_set_current_schema: bool = False
    connection_transactions: bool = False
    get_objects_constraints_check: bool = False
    get_objects_constraints_foreign: bool = False
    get_objects_constraints_primary: bool = False
    get_objects_constraints_unique: bool = False
    statement_bind: bool = False
    statement_bulk_ingest: bool = False
    statement_bulk_ingest_catalog: bool = False
    statement_bulk_ingest_schema: bool = False
    statement_bulk_ingest_temporary: bool = False
    statement_execute_schema: bool = False
    statement_get_parameter_schema: bool = False
    statement_prepare: bool = False
    statement_rows_affected: bool = False
    statement_rows_affected_ddl: bool = False
    _current_catalog: str | FromEnv | None = None
    _current_schema: str | FromEnv | None = None
    _secondary_schema: str | FromEnv | None = None
    _secondary_catalog: str | FromEnv | None = None
    _secondary_catalog_schema: str | FromEnv | None = None
    supported_xdbc_fields: list[str] = dataclasses.field(default_factory=list)
    # Some vendors sort the columns, so declaring FOREIGN KEY(b, a) REFERENCES
    # foo(d, c) still gets returned in the order (a, c), (b, d)
    quirk_get_objects_constraints_foreign_normalized: bool = False
    quirk_get_objects_constraints_primary_normalized: bool = False
    quirk_get_objects_constraints_unique_normalized: bool = False

    def __init__(self, **kwargs) -> None:
        for key, value in kwargs.items():
            if key in {
                "current_catalog",
                "current_schema",
                "secondary_schema",
                "secondary_catalog",
                "secondary_catalog_schema",
            }:
                key = "_" + key
            setattr(self, key, value)

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

    @property
    def secondary_schema(self) -> str | None:
        if isinstance(self._secondary_schema, FromEnv):
            return self._secondary_schema.get_or_raise()
        return self._secondary_schema

    @property
    def secondary_catalog(self) -> str | None:
        if isinstance(self._secondary_catalog, FromEnv):
            return self._secondary_catalog.get_or_raise()
        return self._secondary_catalog

    @property
    def secondary_catalog_schema(self) -> str | None:
        if isinstance(self._secondary_catalog_schema, FromEnv):
            return self._secondary_catalog_schema.get_or_raise()
        return self._secondary_catalog_schema

    def with_values(self, **kwargs) -> typing.Self:
        params = {}
        for key, value in dataclasses.asdict(self).items():
            if key in {
                "_current_catalog",
                "_current_schema",
                "_secondary_schema",
                "_secondary_catalog",
                "_secondary_catalog_schema",
            }:
                params[key[1:]] = value
            else:
                params[key] = value
        params.update(kwargs)
        return self.__class__(**params)


class DriverQuirks(abc.ABC):
    name: str
    driver: str
    driver_name: str
    vendor_name: str
    vendor_version: str | re.Pattern[str]
    short_version: str
    features: DriverFeatures
    setup: DriverSetup

    @property
    @abc.abstractmethod
    def queries_paths(self) -> tuple[Path]: ...

    @property
    def query_set(self) -> "QuerySet":
        """
        The queries to be tested.

        A validation suite may override this to customize the queries being
        run programmatically (e.g. to generate new queries dynamically).
        """
        return query_set(self.queries_paths)

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
        self,
        *,
        table_name: str,
        schema_name: str | None = None,
        catalog_name: str | None = None,
        if_exists: bool = True,
        temporary: bool = False,
    ) -> str:
        """
        Drop a table.

        Parameters
        ----------
        cursor : adbc_driver_manager.dbapi.Cursor
            The cursor to execute the query.
        table_name : str
            The name of the table to drop.
        schema_name : str, optional
            The schema containing the table (if not given, assume current
            schema).
        catalog_name : str, optional
            The catalog containing the table (if not given, assume current
            catalog).
        if_exists : bool
            If True, do not raise an error if the table does not exist.
        temporary : bool
            If True, the table is a temporary table.
        """
        if temporary:
            raise NotImplementedError

        name = self.quote_identifier(
            *[part for part in (catalog_name, schema_name, table_name) if part]
        )

        if if_exists:
            return f"DROP TABLE IF EXISTS {name}"
        else:
            return f"DROP TABLE {name}"

    @abc.abstractmethod
    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        """
        Check if the error indicates a table not found.

        Parameters
        ----------
        table_name : str, optional
            The table that was expected to not be found.  Pass None to only
            check if the error was of this general category and not for a
            specific table.
        error : Exception
            The error to check.
        """
        ...

    def qualify_temp_table(
        self, cursor: adbc_driver_manager.dbapi.Cursor, name: str
    ) -> str:
        """
        Return the fully escaped name of a temporary table.
        """
        raise NotImplementedError

    def quote_identifier(self, *identifiers: str) -> str:
        return ".".join(self.quote_one_identifier(ident) for ident in identifiers)

    def quote_one_identifier(self, identifier: str) -> str:
        """Quote an identifier (e.g. table or column name)."""
        identifier = identifier.replace('"', '""')
        return f'"{identifier}"'

    @property
    def sample_ddl_constraints(self) -> list[str]:
        """CREATE TABLE statements with unique/foreign/primary keys."""
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

    def input_schema(self) -> pyarrow.Schema:
        return try_txtcase(self.input_schema_path, query_schema, ["input_schema"])

    def input(self) -> pyarrow.Table:
        return try_txtcase(self.input_path, query_table, ["input"], self.input_schema())

    def expected_schema(self) -> pyarrow.Schema:
        return try_txtcase(
            self.expected_schema_path or self.input_schema_path,
            query_schema,
            ["expected_schema", "input_schema"],
        )

    def expected(self) -> pyarrow.Table:
        return try_txtcase(
            self.expected_path or self.input_path,
            query_table,
            ["expected", "input"],
            self.expected_schema(),
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
        return try_txtcase(self.setup_query_path, query, ["setup_query"])

    def bind_query(self, driver: DriverQuirks) -> str | None:
        if not self.bind_query_path:
            return None

        raw_query = try_txtcase(self.bind_query_path, query, ["bind_query"])
        raw_query = raw_query.strip()

        param = re.compile(r"\$(\d+)")
        # Replace bind parameters with driver-specific syntax

        def repl(match: re.Match[str]) -> str:
            index = int(match.group(1))
            return driver.bind_parameter(index)

        return param.sub(repl, raw_query)

    def query(self) -> str:
        return try_txtcase(self.query_path, query, ["query"]).strip()

    def expected_schema(self) -> pyarrow.Schema:
        return try_txtcase(self.expected_schema_path, query_schema, ["expected_schema"])

    def expected_result(self) -> pyarrow.Table:
        return try_txtcase(
            self.expected_path, query_table, ["expected"], self.expected_schema()
        )

    def bind_schema(self) -> pyarrow.Schema:
        if not self.bind_schema_path:
            return None
        return try_txtcase(self.bind_schema_path, query_schema, ["bind_schema"])

    def bind_data(self) -> pyarrow.Table:
        if not self.bind_path:
            return None
        return try_txtcase(self.bind_path, query_table, ["bind"], self.bind_schema())


@dataclasses.dataclass
class Query:
    name: str
    query: IngestQuery | SelectQuery
    metadata_paths: list[Path | dict[str, typing.Any]] | None = None
    pytest_marks: list = dataclasses.field(default_factory=list)

    def metadata(self) -> query_metadata.QueryMetadata:
        md = {}
        for metadata_path in reversed(self.metadata_paths or []):
            if isinstance(metadata_path, dict):
                m = metadata_path
            else:
                m = try_txtcase(metadata_path, query_meta, ["metadata"])
            utils.merge_into(md, m)
        return query_metadata.QueryMetadata(**md)

    @property
    def arrow_type_name(self) -> str:
        """The human-friendly name of the Arrow type being tested."""
        show_type_parameters = self.metadata().tags.show_arrow_type_parameters
        if isinstance(self.query, IngestQuery):
            # first field of input schema is the row index
            field = self.query.input_schema()[1]
            return utils.arrow_type_name(
                field.type, field.metadata, show_type_parameters=show_type_parameters
            )
        elif isinstance(self.query, SelectQuery):
            if self.query.bind_schema() is not None:
                field = self.query.bind_schema()[-1]
                return utils.arrow_type_name(
                    field.type,
                    field.metadata,
                    show_type_parameters=show_type_parameters,
                )

            # Take the first field; some queries may select additional things
            # like nested types to test how a type behaves in different
            # contexts
            field = self.query.expected_schema()[0]
            return utils.arrow_type_name(
                field.type, field.metadata, show_type_parameters=show_type_parameters
            )
        raise ValueError(
            f"Unknown query type, cannot get Arrow type name for {self.name}"
        )

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
    def load(cls, root: Path, parent: typing.Self | None = None) -> typing.Self:
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
            root.rglob("*.txtcase"),
        )
        files = sorted(files, key=lambda path: (case_name(path), path))
        files = itertools.groupby(files, key=case_name)

        queries = parent.queries.copy() if parent else {}
        for query_case, query_files in files:
            parent_query = None
            if parent:
                parent_query = parent.queries.get(query_case)

            query_files = list(query_files)
            params = {}

            if len(query_files) == 1 and query_files[0].suffixes == [".txtcase"]:
                t = query_txtcase(query_files[0])
                if t is None:
                    raise ValueError(f"Could not load {query_files[0]}")
                for part in t.parts:
                    params[f"{part}_path"] = query_files[0]
            else:
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
                        params["metadata_path"] = query_file
                    elif query_file.suffixes == [".txtcase"]:
                        raise ValueError(
                            f"{query_case}: cannot mix .txtcase in with other files in the same directory: {query_file}"
                        )
                    else:
                        raise ValueError(
                            f"{query_case}: unexpected query file: {query_file}"
                        )
            query = Query.merge(
                name=query_case,
                parent=parent_query,
                **params,
            )

            md = query.metadata()
            if skip := md.skip:
                query.pytest_marks.append(pytest.mark.skip(reason=skip))

            if broken_driver := md.tags.broken_driver:
                query.pytest_marks.append(pytest.mark.xfail(reason=broken_driver))
            if broken_vendor := md.tags.broken_vendor:
                query.pytest_marks.append(pytest.mark.xfail(reason=broken_vendor))

            if md.hide:
                # Some queries are entirely redundant (e.g. if a vendor simply
                # does not support a type, there's no need to test it or
                # report it)
                if query_case in queries:
                    del queries[query_case]
            else:
                queries[query_case] = query
        return cls(queries=queries)


@functools.cache
def base_query_set() -> QuerySet:
    # Not a real driver, load the default/"generic" queries
    return QuerySet.load(ROOT / "queries")


@functools.cache
def query_set(paths: tuple[Path]) -> QuerySet:
    qs = base_query_set()
    for path in paths:
        qs = QuerySet.load(path, parent=qs)
    return qs
