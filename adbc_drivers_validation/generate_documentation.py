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
Generate user documentation based on validation suite results.
"""

import collections
import dataclasses
import html
import json
import typing
import xml.etree.ElementTree
from pathlib import Path

import bidict
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

    quirks: model.DriverQuirks
    features: model.DriverFeatures

    custom_features: CustomFeatures = dataclasses.field(default_factory=CustomFeatures)

    type_select: list[tuple[str, str]] = dataclasses.field(default_factory=list)
    type_bind: list[tuple[str, str]] = dataclasses.field(default_factory=list)
    type_ingest: list[tuple[str, str]] = dataclasses.field(default_factory=list)

    get_objects: dict[str, bool] = dataclasses.field(default_factory=dict)
    get_table_schema: bool = False
    ingest: dict[str, bool] = dataclasses.field(default_factory=dict)
    vendor_version: str = "unknown"

    def pprint(self) -> str:
        # Slightly more friendly representation for debugging
        lines = []

        lines.append("Features")
        lines.append("~~~~~~~~")

        for field in dataclasses.fields(self.features):
            if field.name.startswith("_"):
                continue

            value = getattr(self.features, field.name)
            if isinstance(value, bool):
                value = "✅" if value else "❌"
            lines.append(f"- {field.name}: {value}")

        for group, features in self.custom_features.groups.items():
            lines.append(f"- {group}:")
            for feature in features:
                status = "✅" if feature.supported else "❌"
                lines.append(f"  - {feature.name}: {status} {feature.description}")

        lines.append("")
        lines.append("GetObjects")
        lines.append("~~~~~~~~~~")
        for name, supported in self.get_objects.items():
            status = "✅" if supported else "❌"
            lines.append(f"- {name}: {status}")

        status = "✅" if self.get_table_schema else "❌"
        lines.append("")
        lines.append(f"GetTableSchema: {status}")

        lines.append("")
        lines.append("Ingest Modes")
        lines.append("~~~~~~~~~~~~")
        for name, supported in self.ingest.items():
            status = "✅" if supported else "❌"
            lines.append(f"- {name}: {status}")

        def render_type_table(category: str, entries: list[tuple[str, str]]) -> None:
            if not entries:
                return
            lines.append("")
            lines.append(f"{category.capitalize()} Types")
            lines.append("~" * (len(category) + 6))
            max_lhs = max(len(lhs) for lhs, _ in entries)
            for lhs, rhs in entries:
                lines.append(f"- {lhs.ljust(max_lhs)} → {rhs}")

        render_type_table("select", self.type_select)
        render_type_table("bind", self.type_bind)
        render_type_table("ingest", self.type_ingest)

        return "\n".join(lines)


@dataclasses.dataclass
class ValidationReport:
    driver: str
    versions: dict[str, DriverTypeTable]
    driver_version: str = "unknown"
    footnotes: dict[str, bidict.bidict[int, str]] = dataclasses.field(
        default_factory=lambda: collections.defaultdict(bidict.bidict)
    )

    def pprint(self) -> str:
        # Slightly more friendly representation for debugging
        lines = []

        lines.append(f"Driver Version: {self.driver_version}")
        lines.append("")

        lines.append("Versions")
        lines.append("========")

        for version, table in self.versions.items():
            lines.append("")
            lines.append(version)
            lines.append("-" * len(version))
            lines.append(table.pprint())

        lines.append("")
        lines.append("Footnotes")
        lines.append("=========")
        for idx, footnote in self.footnotes["types"].items():
            lines.append(f"[^{idx}]: {footnote}")

        return "\n".join(lines)

    def add_footnote(self, scope: str, contents: str) -> str:
        if scope not in {"types"}:
            raise ValueError(f"Invalid footnote scope: {scope}")

        if contents in self.footnotes[scope].inverse:
            counter = self.footnotes[scope].inverse[contents]
            return f" [^{counter}]"
        counter = len(self.footnotes[scope]) + 1
        self.footnotes[scope][counter] = contents
        return f" [^{counter}]"

    def add_table_entry(
        self,
        vendor_version: str,
        category: typing.Literal["select", "bind", "ingest"],
        lhs: str,
        rhs: str,
        test_case: dict[str, typing.Any],
        *,
        extra_caveats: list[str] | None = None,
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
        elif (
            partial_support or passed < len(test_case["test_results"]) or extra_caveats
        ):
            table_entry += " ⚠️"
            for query_name, result in zip(
                test_case["query_names"], test_case["test_results"]
            ):
                if result == "passed":
                    continue
                query_kind = query_name.split("/")[1]
                caveats.append(f"{query_kind} is not supported for {rhs}")

        caveats.extend(extra_caveats or [])

        for caveat in caveats:
            fn = self.add_footnote("types", caveat)
            if fn not in table_entry:
                table_entry += fn

        getattr(self.versions[vendor_version], f"type_{category}").append(
            (lhs, table_entry)
        )


def arrow_type_name(arrow_type, metadata=None, show_type_parameters=False):
    # Special handling (sometimes we want params, sometimes not)
    if metadata and (ext := metadata.get(b"ARROW:extension:name")):
        return f"extension<{ext.decode('utf-8')}>"
    if show_type_parameters:
        return str(arrow_type)
    elif isinstance(arrow_type, pyarrow.Decimal128Type):
        return "decimal128"
    elif isinstance(arrow_type, pyarrow.FixedSizeBinaryType):
        return "fixed_size_binary"
    elif isinstance(arrow_type, pyarrow.FixedSizeListType):
        return "fixed_size_binary"
    elif isinstance(arrow_type, pyarrow.ListType):
        return "list"
    elif isinstance(arrow_type, pyarrow.StructType):
        return "struct"
    return str(arrow_type)


def render_to(sink: Path, template, kwargs) -> None:
    rendered = render_part(template, kwargs)
    sink.parent.mkdir(parents=True, exist_ok=True)
    with sink.open("w") as f:
        f.write(rendered)
        f.write("\n")
    print("Generated", sink)
    return rendered


def render_part(template, kwargs) -> str:
    rendered = template.render(**kwargs)
    # Eliminate trailing whitespace from lines
    lines = [line.rstrip() for line in rendered.splitlines()]
    # Remove empty lines
    while lines and not lines[-1]:
        lines = lines[:-1]
    while lines and not lines[0]:
        lines = lines[1:]
    rendered = "\n".join(lines)
    return rendered


def load_testcases(
    quirks: model.DriverQuirks, results_path: Path, query_set: model.QuerySet
) -> None:
    """Load test case data into DuckDB."""
    report = xml.etree.ElementTree.parse(results_path).getroot()
    driver_name = f"{quirks.name}:{quirks.short_version}"

    testcases = []
    for testcase in report.findall(".//testsuite[@name='validation']/testcase"):
        module = testcase.get("classname")
        name = testcase.get("name")
        if "[" in name:
            name, _, _ = name.partition("[")
        failure = testcase.find("failure")
        skipped = testcase.find("skipped")

        properties = {}
        for prop in testcase.findall(".//properties/property"):
            properties[prop.get("name")] = prop.get("value")

        driver = properties["driver"]
        if driver != driver_name:
            continue
        driver = quirks.name
        version = quirks.short_version

        query_name = properties.get("query")
        if query_name is None:
            query = None
            metadata = {}
            tags = {}
        else:
            query = query_set.queries[query_name]
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
                "test_module": module,
                "test_name": name,
                "test_result": test_result,
                "driver": driver,
                "vendor_version": version,
                "query_name": query_name,
                "tags": json.dumps(tags),
                "properties": json.dumps(properties),
            }
        )

    schema = pyarrow.schema(
        [
            pyarrow.field("test_module", pyarrow.string()),
            pyarrow.field("test_name", pyarrow.string()),
            pyarrow.field("test_result", pyarrow.string()),
            pyarrow.field("driver", pyarrow.string()),
            pyarrow.field("vendor_version", pyarrow.string()),
            pyarrow.field("query_name", pyarrow.string()),
            # Actually JSON
            pyarrow.field("tags", pyarrow.string()),
            pyarrow.field("properties", pyarrow.string()),
        ]
    )
    duckdb.register(
        "testcases_raw", pyarrow.Table.from_pylist(testcases, schema=schema)
    )
    duckdb.sql(
        """
        CREATE TABLE IF NOT EXISTS testcases (
              test_module STRING,
              test_name STRING,
              test_result STRING,
              driver STRING,
              vendor_version STRING,
              query_name STRING,
              tags JSON,
              properties JSON,
        );
        INSERT INTO testcases
        SELECT
          test_module,
          test_name,
          test_result,
          driver,
          vendor_version,
          query_name,
          CAST(tags AS JSON) AS tags,
          CAST(properties AS JSON) AS properties,
        FROM testcases_raw
        """
    )


def render(
    report: ValidationReport,
    driver_template_path: Path,
    output_directory: Path,
) -> None:
    env = jinja2.Environment(
        loader=jinja2.PackageLoader("adbc_drivers_validation"),
        autoescape=jinja2.select_autoescape(),
    )
    with driver_template_path.open("r") as source:
        driver_template = env.from_string(source.read())

    driver = report.driver
    default_vendor_version = max(report.versions.keys())
    default_version_info = report.versions[default_vendor_version]

    template_vars = {
        **dataclasses.asdict(report.versions[default_vendor_version]),
        "driver": report.driver,
    }
    types = render_part(env.get_template("types.md"), template_vars)
    features = render_part(env.get_template("features.md"), template_vars)
    footnotes = render_part(
        env.get_template("footnotes.md"),
        {**template_vars, "footnotes": report.footnotes},
    )

    # Assemble the version header/warnings/etc
    is_prerelease = (
        not report.driver_version.startswith("v")
        or report.driver_version.endswith("-dirty")
        or "dev" in report.driver_version
    )

    if is_prerelease:
        ref = f"driver-{driver}-prerelease"
        heading = f"{{badge-primary}}`Driver Version|{report.driver_version}`"
    else:
        ref = f"driver-{driver}-{report.driver_version}"
        heading = f"[{{badge-primary}}`Driver Version|{report.driver_version}`]({ref})"

    # TODO: Improve this display for drivers tested with many versions. We
    # probably want to show one badge with a range rather than a badge for every
    # version
    for version in sorted(report.versions):
        heading += f" {{badge-success}}`Tested With|{default_version_info.quirks.vendor_name} {version}`"

    compatibility_info = f"This driver was tested on the following versions of {default_version_info.quirks.vendor_name}:\n"
    for version in sorted(report.versions):
        compatibility_info += f"\n- {report.versions[version].vendor_version}"

    if is_prerelease:
        heading += (
            "\n\n:::{warning}\nThis is documentation for a prerelease version.\n:::"
        )

    render_to(
        output_directory / f"{driver}.md",
        driver_template,
        {
            **template_vars,
            "types": types,
            "features": features,
            "footnotes": footnotes,
            "cross_reference": f"({ref})=",
            "heading": heading,
            "version": report.driver_version,
            "compatibility_info": compatibility_info,
        },
    )


def generate_includes(
    all_quirks: list[model.DriverQuirks], query_sets: dict[str, model.QuerySet]
) -> dict[str, DriverTypeTable]:
    # Handle different versions of one vendor
    report = ValidationReport(
        driver=all_quirks[0].name,
        versions={
            quirks.short_version: DriverTypeTable(
                quirks=quirks, features=quirks.features
            )
            for quirks in all_quirks
        },
    )

    # Version
    version = (
        duckdb.sql("""
    FROM testcases
    SELECT
      properties->>'driver_version' AS driver_version,
      properties->>'short_version' AS short_version,
      properties->>'vendor_version' AS vendor_version,
    WHERE test_name = 'test_get_info'
    """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    driver_version = list(set(v["driver_version"] for v in version))
    if len(driver_version) != 1:
        raise ValueError(f"Expected one driver version, got {driver_version}")
    report.driver_version = driver_version[0]
    for v in version:
        short_version = v["short_version"]
        vendor_version = v["vendor_version"]
        report.versions[short_version].vendor_version = vendor_version

    # Select type support
    type_tests = (
        duckdb.sql("""
        FROM testcases
        SELECT
          vendor_version,
          tags->>'sql-type-name' AS sql_type,
          ARRAY_AGG(test_result ORDER BY query_name ASC) AS test_results,
          ARRAY_AGG(query_name ORDER BY query_name ASC) AS query_names,
          ARRAY_AGG(tags ORDER BY query_name ASC) as tags,
        WHERE
          test_name = 'test_query'
          AND query_name NOT LIKE 'type/bind/%'
          AND (tags->>'sql-type-name') IS NOT NULL
        GROUP BY vendor_version, tags->>'sql-type-name'
        ORDER BY vendor_version, tags->>'sql-type-name'
        """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    for test_case in type_tests:
        arrow_type_names = set()
        for query_name in test_case["query_names"]:
            query = query_sets[test_case["vendor_version"]].queries[query_name]
            show_type_parameters = (
                query.metadata()
                .get("tags", {})
                .get("show-arrow-type-parameters", False)
            )
            # Take the first field; some queries may select additional things
            # like nested types to test how a type behaves in different
            # contexts
            field = query.query.expected_schema()[0]
            arrow_type_names.add(
                arrow_type_name(
                    field.type,
                    field.metadata,
                    show_type_parameters=show_type_parameters,
                )
            )
        extra_caveats = []
        if len(arrow_type_names) != 1:
            arrow_type = ", ".join(sorted(arrow_type_names))
            extra_caveats.append(
                "Return type is inconsistent depending on how the query was written"
            )
        else:
            arrow_type = next(iter(arrow_type_names))

        arrow_type = html.escape(arrow_type)
        sql_type = html.escape(test_case["sql_type"])
        report.add_table_entry(
            test_case["vendor_version"],
            "select",
            sql_type,
            arrow_type,
            test_case,
            extra_caveats=extra_caveats,
        )

    # Bind type support
    type_tests = (
        duckdb.sql("""
        FROM testcases
        SELECT
          vendor_version,
          tags->>'sql-type-name' AS sql_type,
          ARRAY_AGG(test_result ORDER BY query_name ASC) AS test_results,
          ARRAY_AGG(query_name ORDER BY query_name ASC) AS query_names,
          ARRAY_AGG(tags ORDER BY query_name ASC) as tags,
        WHERE
          test_name = 'test_query'
          AND query_name LIKE 'type/bind/%'
          AND (tags->>'sql-type-name') IS NOT NULL
        GROUP BY vendor_version, tags->>'sql-type-name'
        ORDER BY vendor_version, tags->>'sql-type-name'
        """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    for test_case in type_tests:
        query_set = query_sets[test_case["vendor_version"]]
        arrow_type_names = set()
        for query_name in test_case["query_names"]:
            bind_schema = query_set.queries[query_name].query.bind_schema()
            field = bind_schema[-1]
            arrow_type_names.add(arrow_type_name(field.type, field.metadata))
        sql_type = html.escape(test_case["sql_type"])
        for arrow_type in arrow_type_names:
            arrow_type = html.escape(arrow_type)
            report.add_table_entry(
                test_case["vendor_version"],
                "bind",
                arrow_type,
                sql_type,
                test_case,
            )

    # Ingest type support
    type_tests = (
        duckdb.sql("""
        FROM testcases
        SELECT
          vendor_version,
          tags->>'sql-type-name' AS sql_type,
          test_result,
          query_name,
          tags
        WHERE
          test_module LIKE '%TestIngest'
          AND test_name = 'test_create'
          AND (tags->>'sql-type-name') IS NOT NULL
        ORDER BY vendor_version, query_name
        """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    for test_case in type_tests:
        query_set = query_sets[test_case["vendor_version"]]
        arrow_type = html.escape(
            arrow_type_name(
                query_set.queries[test_case["query_name"]].query.input_schema()[1].type
            )
        )
        sql_type = html.escape(test_case["sql_type"])
        report.add_table_entry(
            test_case["vendor_version"],
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
        vendor_version,
        regexp_extract(test_name, 'test_get_objects_([a-z]+)', 1) AS test_name,
        test_result,
      WHERE test_name LIKE 'test_get_objects_%'
    )
    FROM get_objects_cases
    SELECT vendor_version, test_name, BOOL_AND(test_result = 'passed') AS supported
    GROUP BY vendor_version, test_name
    """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    for test_case in get_objects:
        report.versions[test_case["vendor_version"]].get_objects[
            test_case["test_name"]
        ] = test_case["supported"]

    # Get table schema
    get_table_schema = (
        duckdb.sql("""
    FROM testcases
    SELECT vendor_version, CAST(COUNTIF(test_result = 'passed') AS BIGINT) AS supported_cases, COUNT() AS total_cases
    WHERE test_name = 'test_get_table_schema' AND test_result != 'skipped'
    GROUP BY vendor_version, test_name
    """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    for test_case in get_table_schema:
        report.versions[test_case["vendor_version"]].get_table_schema = (
            test_case["supported_cases"] == test_case["total_cases"]
        )

    # Ingest modes
    ingest_types = (
        duckdb.sql("""
    WITH ingest_cases AS (
      FROM testcases
      SELECT
        vendor_version,
        test_name,
        test_result,
      WHERE test_module LIKE '%TestIngest'
    )
    FROM ingest_cases
    SELECT vendor_version, test_name, BOOL_OR(test_result = 'passed') AS supported
    GROUP BY vendor_version, test_name
    """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    for test_case in ingest_types:
        ingest = report.versions[test_case["vendor_version"]].ingest
        name = test_case["test_name"][5:]  # Strip 'test_' prefix
        ingest[name] = test_case["supported"]

    # Custom features
    custom_features = (
        duckdb.sql("""
        FROM testcases
        SELECT
          vendor_version,
          properties->>'feature:group' AS feature_group,
          properties->>'feature:name' AS feature_name,
          ANY_VALUE(properties->>'doc') AS description,
          ANY_VALUE(test_result) = 'passed' AS supported
        WHERE
          (properties->>'feature:group' IS NOT NULL) AND
          (properties->>'feature:name' IS NOT NULL)
        GROUP BY
          vendor_version,
          properties->>'feature:group',
          properties->>'feature:name'
        ORDER BY
          vendor_version,
          properties->>'feature:group',
          properties->>'feature:name'
        """)
        .arrow()
        .read_all()
        .to_pylist()
    )
    for test_case in custom_features:
        custom_features = report.versions[test_case["vendor_version"]].custom_features
        custom_features.groups[test_case["feature_group"]].append(
            CustomFeature(
                name=test_case["feature_name"],
                description=test_case["description"],
                supported=test_case["supported"],
            )
        )

    return report


def generate(
    all_quirks: list[model.DriverQuirks],
    test_results: Path,
    driver_template: Path,
    output: Path,
) -> None:
    if len({quirks.name for quirks in all_quirks}) != 1:
        raise ValueError("All quirks must be for the same driver")
    if len({quirks.short_version for quirks in all_quirks}) != len(all_quirks):
        raise ValueError("All quirks must be for the different versions")

    query_sets = {}
    for quirks in all_quirks:
        query_set = model.query_set(quirks.queries_paths)
        load_testcases(quirks, test_results, query_set)
        query_sets[quirks.short_version] = query_set
    report = generate_includes(all_quirks, query_sets)
    print(report.pprint())
    render(report, driver_template, output)
