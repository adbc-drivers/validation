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

"""Pydantic models for query metadata."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ReversibleOption(BaseModel):
    """An option value that can be applied and reverted."""

    model_config = ConfigDict(extra="forbid")

    apply: str = Field(description="Value to apply for this option.")
    revert: str | None = Field(description="Value to revert to after the query.")


class ConnectionOptions(BaseModel):
    """Connection options for setup."""

    model_config = ConfigDict(extra="forbid")

    options: dict[str, ReversibleOption] = Field(
        default_factory=dict,
        description="Connection options to apply. Values can be strings or dicts with 'apply' and 'revert' keys.",
    )

    @field_validator("options", mode="before")
    @classmethod
    def parse_options(cls, v):
        """Parse string values into ReversibleOption objects."""
        if not isinstance(v, dict):
            return v

        result = {}
        for key, value in v.items():
            if isinstance(value, str):
                result[key] = {"apply": value, "revert": None}
            else:
                result[key] = value
        return result


class StatementOptions(BaseModel):
    """Statement options for setup."""

    model_config = ConfigDict(extra="forbid")

    options: dict[str, ReversibleOption] = Field(
        default_factory=dict,
        description="Statement options to apply. Values can be strings or dicts with 'apply' and 'revert' keys.",
    )

    @field_validator("options", mode="before")
    @classmethod
    def parse_options(cls, v):
        """Parse string values into ReversibleOption objects."""
        if not isinstance(v, dict):
            return v

        result = {}
        for key, value in v.items():
            if isinstance(value, str):
                result[key] = {"apply": value, "revert": None}
            else:
                result[key] = value
        return result


class SetupMetadata(BaseModel):
    """Setup metadata for a query."""

    model_config = ConfigDict(extra="forbid")

    drop: str | None = Field(
        default=None,
        description="Name of the table to drop before running the query.",
    )
    connection: ConnectionOptions | None = Field(
        default=None,
        description="Connection options to apply for this query.",
    )
    statement: StatementOptions | None = Field(
        default=None,
        description="Statement options to apply for this query.",
    )


class TagsMetadata(BaseModel):
    """Tags metadata for a query."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    sql_type_name: str | None = Field(
        default=None,
        alias="sql-type-name",
        description="The name of the SQL type being tested (for documentation).",
    )
    caveats: list[str] = Field(
        default_factory=list,
        description="List of footnotes to add to the user documentation.",
    )
    partial_support: bool = Field(
        default=False,
        alias="partial-support",
        description="Indicate that something is only partially supported.",
    )
    broken_driver: str | None = Field(
        default=None,
        alias="broken-driver",
        description="Indicate that the driver is broken for this query.",
    )
    broken_vendor: str | None = Field(
        default=None,
        alias="broken-vendor",
        description="Indicate that the vendor is broken for this query.",
    )
    show_arrow_type_parameters: bool = Field(
        default=False,
        alias="show-arrow-type-parameters",
        description="Show Arrow type parameters in documentation.",
    )
    variant: str | None = Field(
        default=None,
        description="A variant name to distinguish this query from others with the same SQL type (for documentation).",
    )


class QueryMetadata(BaseModel):
    """Metadata for a query test case.

    This metadata is typically stored in a .toml file alongside the query files,
    or in a .txtcase file under the '// part: metadata' section.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    hide: bool = Field(
        default=False,
        description="If true, don't run this query (removes entry from documentation).",
    )
    skip: str | None = Field(
        default=None,
        description="If present, skip the query with the given reason (shows as unsupported in documentation).",
    )
    sort_keys: list[tuple[str, Literal["ascending", "descending"]]] | None = Field(
        default=None,
        alias="sort-keys",
        description="Sort the result set by these columns before comparison.",
    )
    setup: SetupMetadata = Field(
        default_factory=SetupMetadata,
        description="Setup metadata for the query.",
    )
    connection: ConnectionOptions | None = Field(
        default=None,
        description="(Deprecated) Connection options. Use setup.connection instead.",
    )
    statement: StatementOptions | None = Field(
        default=None,
        description="(Deprecated) Statement options. Use setup.statement instead.",
    )
    tags: TagsMetadata = Field(
        default_factory=TagsMetadata,
        description="Tags for documentation generation.",
    )
