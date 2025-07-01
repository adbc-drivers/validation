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
Generate user documentation based on validation suite results.
"""

import argparse
import collections
import dataclasses
import html
import json
import typing
import xml.etree.ElementTree
from pathlib import Path

import duckdb
import jinja2
import pyarrow

from . import model


@dataclasses.dataclass
class CustomFeature:
    """A custom feature that a driver supports."""

    #: The name to display.
    name: str
    #: A short description.
    description: str
    #: Whether the test(s) for this feature passed.
    supported: bool


@dataclasses.dataclass
class CustomFeatures:
    """A set of custom features that a driver supports."""

    #: A mapping from categories to a list of subfeatures.
    groups: dict[str, list[CustomFeature]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(list)
    )


@dataclasses.dataclass
class DriverTypeTable:
    """A table of features supported by a driver."""

    features: model.DriverFeatures

    custom_features: CustomFeatures = dataclasses.field(default_factory=CustomFeatures)

    type_select: list[tuple[str, str]] = dataclasses.field(default_factory=list)
    type_bind: list[tuple[str, str]] = dataclasses.field(default_factory=list)
    type_ingest: list[tuple[str, str]] = dataclasses.field(default_factory=list)

    get_objects: dict[str, bool] = dataclasses.field(default_factory=dict)
    get_table_schema: bool = False
    ingest: dict[str, bool] = dataclasses.field(default_factory=dict)

    footnotes: dict[str, dict[int, str]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(dict)
    )

    def add_footnote(self, scope: str, contents: str) -> str:
        counter = len(self.footnotes[scope]) + 1
        self.footnotes[scope][counter] = contents
        return f" [^{counter}]"

    def add_table_entry(
        self,
        category: typing.Literal["select", "bind", "ingest"],
        lhs: str,
        rhs: str,
        test_case: dict[str, typing.Any],
    ) -> None:
        caveats = []
        table_entry = rhs
        passed = test_case["test_results"].count("passed")

        partial_support = False
        for raw_meta in test_case["tags"]:
            meta = json.loads(raw_meta)
            partial_support = partial_support or meta.get("partial-support", False)
            caveats.extend(meta.get("caveats", []))

            if caveat := meta.get("broken-vendor"):
                caveats.append(caveat)

        if passed == 0:
            table_entry = "❌"
        elif partial_support or passed < len(test_case["test_results"]):
            table_entry += " ⚠️"
            for query_name, result in zip(
                test_case["query_names"], test_case["test_results"]
            ):
                if result == "passed":
                    continue
                query_kind = query_name.split("/")[1]
                caveats.append(f"{query_kind} is not supported for {rhs}")

        for caveat in caveats:
            table_entry += self.add_footnote("types", caveat)

        getattr(self, f"type_{category}").append((lhs, table_entry))


def arrow_type_name(arrow_type, show_type_parameters=False):
    # Special handling (sometimes we want params, sometimes not)
    if show_type_parameters:
        return str(arrow_type)
    elif isinstance(arrow_type, pyarrow.Decimal128Type):
        return "decimal128"
    elif isinstance(arrow_type, pyarrow.ListType):
        return "list"
    elif isinstance(arrow_type, pyarrow.StructType):
        return "struct"
    return str(arrow_type)


def render_to(sink: Path, template, kwargs) -> None:
    sink.parent.mkdir(parents=True, exist_ok=True)
    rendered = template.render(**kwargs)
    # Eliminate trailing whitespace from lines
    rendered = "\n".join(line.rstrip() for line in rendered.split("\n"))

    with sink.open("w") as f:
        f.write(rendered)
        f.write("\n")
    print("Generated", sink)


def load_testcases() -> None:
    """Load test case data into DuckDB."""
    report = xml.etree.ElementTree.parse(model.ROOT / "validation-report.xml").getroot()
    drivers = set(model.drivers_to_test())

    testcases = []
    for testcase in report.findall(".//testsuite[@name='validation']/testcase"):
        module = testcase.get("classname")
        if module in {"tests.test_arrowjson"}:
            continue
        name = testcase.get("name")
        if "[" in name:
            name, _, _ = name.partition("[")
        failure = testcase.find("failure")
        skipped = testcase.find("skipped")

        properties = {}
        for prop in testcase.findall(".//properties/property"):
            properties[prop.get("name")] = prop.get("value")

        driver = properties["driver"]

        if driver not in drivers:
            continue

        query_name = properties.get("query")
        if query_name is None:
            query = None
            metadata = {}
            tags = {}
        else:
            query = model.query_set(driver).queries[query_name]
            metadata = query.metadata()
            tags = metadata.get("tags", {})

        if failure is not None:
            test_result = "failed"
        elif skipped is not None:
            if skipped.get("type") == "pytest.xfail":
                test_result = "xfail"
            else:
                test_result = "skipped"
        else:
            test_result = "passed"

        testcases.append(
            {
                "test": name,
                "test_result": test_result,
                "driver": driver,
                "query_name": query_name,
                "tags": json.dumps(tags),
                "properties": json.dumps(properties),
            }
        )

    schema = pyarrow.schema(
        [
            pyarrow.field("test", pyarrow.string()),
            pyarrow.field("test_result", pyarrow.string()),
            pyarrow.field("driver", pyarrow.string()),
            pyarrow.field("query_name", pyarrow.string()),
            # Actually JSON, but there's no standard extension type since PMC members rejected it
            pyarrow.field("tags", pyarrow.string()),
            pyarrow.field("properties", pyarrow.string()),
        ]
    )
    duckdb.register(
        "testcases_raw", pyarrow.Table.from_pylist(testcases, schema=schema)
    )
    duckdb.sql(
        """
        CREATE TABLE testcases AS
        SELECT
          test,
          test_result,
          driver,
          query_name,
          CAST(tags AS JSON) AS tags,
          CAST(properties AS JSON) AS properties,
        FROM testcases_raw
        """
    )


def render(drivers: dict[str, DriverTypeTable], output_directory: Path) -> None:
    env = jinja2.Environment(
        loader=jinja2.PackageLoader("validation"),
        autoescape=jinja2.select_autoescape(),
    )
    features_template = env.get_template("features.qmd")
    types_template = env.get_template("types.qmd")
    for driver in sorted(drivers):
        output = output_directory / f"types/_{driver}.md"
        render_to(output, types_template, dataclasses.asdict(drivers[driver]))

        output = output_directory / f"features/_{driver}.md"
        render_to(output, features_template, dataclasses.asdict(drivers[driver]))


def generate_includes() -> None:
    drivers = {
        driver: DriverTypeTable(features=model.driver_manifest(driver).features)
        for driver in model.drivers_to_test()
    }

    # Select type support
    type_tests = (
        duckdb.sql("""
        FROM testcases
        SELECT
          driver,
          tags->>'sql-type-name' AS sql_type,
          ARRAY_AGG(test_result ORDER BY query_name ASC) AS test_results,
          ARRAY_AGG(query_name ORDER BY query_name ASC) AS query_names,
          ARRAY_AGG(tags ORDER BY query_name ASC) as tags,
        WHERE
          test = 'test_query'
          AND query_name NOT LIKE 'type/bind/%'
          AND (tags->>'sql-type-name') IS NOT NULL
        GROUP BY driver, tags->>'sql-type-name'
        ORDER BY driver, tags->>'sql-type-name'
        """)
        .arrow()
        .to_pylist()
    )
    for test_case in type_tests:
        query_set = model.query_set(test_case["driver"]).queries
        arrow_type_names = set()
        for query_name in test_case["query_names"]:
            query = query_set[query_name]
            show_type_parameters = (
                query.metadata()
                .get("tags", {})
                .get("show-arrow-type-parameters", False)
            )
            for field in query.query.expected_schema():
                arrow_type_names.add(
                    arrow_type_name(
                        field.type, show_type_parameters=show_type_parameters
                    )
                )
        if len(arrow_type_names) != 1:
            raise NotImplementedError(
                f"Can't handle a driver being inconsistent in its arrow type for a SQL type: {arrow_type_names}"
            )
        sql_type = html.escape(test_case["sql_type"])
        arrow_type = html.escape(next(iter(arrow_type_names)))
        drivers[test_case["driver"]].add_table_entry(
            "select", sql_type, arrow_type, test_case
        )

    # Bind type support
    type_tests = (
        duckdb.sql("""
        FROM testcases
        SELECT
          driver,
          tags->>'sql-type-name' AS sql_type,
          ARRAY_AGG(test_result ORDER BY query_name ASC) AS test_results,
          ARRAY_AGG(query_name ORDER BY query_name ASC) AS query_names,
          ARRAY_AGG(tags ORDER BY query_name ASC) as tags,
        WHERE
          test = 'test_query'
          AND query_name LIKE 'type/bind/%'
          AND (tags->>'sql-type-name') IS NOT NULL
        GROUP BY driver, tags->>'sql-type-name'
        ORDER BY driver, tags->>'sql-type-name'
        """)
        .arrow()
        .to_pylist()
    )
    for test_case in type_tests:
        query_set = model.query_set(test_case["driver"]).queries
        arrow_type_names = {
            arrow_type_name(field.type)
            for query_name in test_case["query_names"]
            for field in query_set[query_name].query.bind_schema()
        }
        if len(arrow_type_names) != 1:
            raise NotImplementedError(
                f"Can't handle a driver being inconsistent in its arrow type for a SQL type: {arrow_type_names}"
            )
        sql_type = html.escape(test_case["sql_type"])
        arrow_type = html.escape(next(iter(arrow_type_names)))
        drivers[test_case["driver"]].add_table_entry(
            "bind", arrow_type, sql_type, test_case
        )

    # Ingest type support
    type_tests = (
        duckdb.sql("""
        FROM testcases
        SELECT
          driver,
          tags->>'sql-type-name' AS sql_type,
          test_result,
          query_name,
          tags
        WHERE
          test = 'test_ingest_create'
          AND (tags->>'sql-type-name') IS NOT NULL
          AND test_result != 'skipped'
        ORDER BY driver, query_name
        """)
        .arrow()
        .to_pylist()
    )
    for test_case in type_tests:
        query_set = model.query_set(test_case["driver"]).queries
        arrow_type = html.escape(
            arrow_type_name(
                query_set[test_case["query_name"]].query.input_schema()[1].type
            )
        )
        sql_type = html.escape(test_case["sql_type"])
        drivers[test_case["driver"]].add_table_entry(
            "ingest",
            arrow_type,
            sql_type,
            {
                "test_results": [test_case["test_result"]],
                "query_names": [test_case["query_name"]],
                "tags": [test_case["tags"]],
            },
        )

    # GetObjects
    get_objects = (
        duckdb.sql("""
    WITH get_objects_cases AS (
      FROM testcases
      SELECT
        driver,
        regexp_extract(test, 'test_get_objects_([a-z]+)', 1) AS test,
        test_result,
      WHERE test LIKE 'test_get_objects_%'
    )
    FROM get_objects_cases
    SELECT driver, test, BOOL_AND(test_result = 'passed') AS supported
    GROUP BY driver, test
    """)
        .arrow()
        .to_pylist()
    )
    for test_case in get_objects:
        drivers[test_case["driver"]].get_objects[test_case["test"]] = test_case[
            "supported"
        ]

    # Get table schema
    get_table_schema = (
        duckdb.sql("""
    FROM testcases
    SELECT driver, CAST(COUNTIF(test_result = 'passed') AS BIGINT) AS supported_cases, COUNT() AS total_cases
    WHERE test = 'test_get_table_schema'
    GROUP BY driver, test
    """)
        .arrow()
        .to_pylist()
    )
    for test_case in get_table_schema:
        drivers[test_case["driver"]].get_table_schema = (
            test_case["supported_cases"] == test_case["total_cases"]
        )

    # Ingest modes
    ingest_types = (
        duckdb.sql("""
    WITH ingest_cases AS (
      FROM testcases
      SELECT
        driver,
        regexp_extract(test, 'test_ingest_([a-z]+)', 1) AS test,
        test_result,
      WHERE test LIKE 'test_ingest_%'
    )
    FROM ingest_cases
    SELECT driver, test, BOOL_OR(test_result = 'passed') AS supported
    GROUP BY driver, test
    """)
        .arrow()
        .to_pylist()
    )
    for test_case in ingest_types:
        drivers[test_case["driver"]].ingest[test_case["test"]] = test_case["supported"]

    # Custom features
    custom_features = (
        duckdb.sql("""
        FROM testcases
        SELECT
          driver,
          properties->>'feature:group' AS feature_group,
          properties->>'feature:name' AS feature_name,
          ANY_VALUE(properties->>'doc') AS description,
          ANY_VALUE(test_result) = 'passed' AS supported
        WHERE
          (properties->>'feature:group' IS NOT NULL) AND
          (properties->>'feature:name' IS NOT NULL)
        GROUP BY
          driver,
          properties->>'feature:group',
          properties->>'feature:name'
        ORDER BY
          driver,
          properties->>'feature:group',
          properties->>'feature:name'
        """)
        .arrow()
        .to_pylist()
    )
    for test_case in custom_features:
        custom_features = drivers[test_case["driver"]].custom_features
        custom_features.groups[test_case["feature_group"]].append(
            CustomFeature(
                name=test_case["feature_name"],
                description=test_case["description"],
                supported=test_case["supported"],
            )
        )

    return drivers


def main():
    parser = argparse.ArgumentParser(
        description="Generate documentation based on validation suite"
    )
    parser.add_argument(
        "-o", "--output", help="Output directory", type=Path, required=True
    )
    args = parser.parse_args()

    load_testcases()
    drivers = generate_includes()
    render(drivers, args.output)


if __name__ == "__main__":
    main()
