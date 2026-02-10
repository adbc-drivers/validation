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

"""Tests for the metadata Pydantic models."""

import tomllib
from pathlib import Path

import pytest

import adbc_drivers_validation.txtcase
from adbc_drivers_validation.query_metadata import (
    ConnectionOptions,
    QueryMetadata,
    SetupMetadata,
    StatementOptions,
    TagsMetadata,
)

# Get the root directory for test data
ROOT = Path(__file__).parent.parent


def test_connection_options_parse():
    opts = ConnectionOptions.model_validate(
        {
            "options": {
                "simple": "value",
                "complex": {"apply": "on", "revert": "off"},
            },
        }
    )
    assert opts.options["simple"].apply == "value"
    assert opts.options["simple"].revert is None
    assert opts.options["complex"].apply == "on"
    assert opts.options["complex"].revert == "off"


def test_statement_options_parse():
    opts = StatementOptions.model_validate(
        {
            "options": {
                "simple": "value",
                "complex": {"apply": "on", "revert": "off"},
            },
        }
    )
    assert opts.options["simple"].apply == "value"
    assert opts.options["simple"].revert is None
    assert opts.options["complex"].apply == "on"
    assert opts.options["complex"].revert == "off"


class TestTagsMetadata:
    """Test the TagsMetadata model."""

    def test_empty_tags(self):
        """Test creating empty tags."""
        tags = TagsMetadata()
        assert tags.sql_type_name is None
        assert tags.caveats == []
        assert tags.partial_support is False
        assert tags.broken_driver is None
        assert tags.show_arrow_type_parameters is False

    def test_tags_with_sql_type_name(self):
        """Test tags with sql-type-name."""
        tags = TagsMetadata.model_validate({"sql-type-name": "VARCHAR"})
        assert tags.sql_type_name == "VARCHAR"

    def test_tags_with_caveats(self):
        """Test tags with caveats."""
        tags = TagsMetadata(caveats=["Note 1", "Note 2"])
        assert tags.caveats == ["Note 1", "Note 2"]

    def test_tags_with_partial_support(self):
        """Test tags with partial-support."""
        tags = TagsMetadata.model_validate({"partial-support": True})
        assert tags.partial_support is True


class TestSetupMetadata:
    """Test the SetupMetadata model."""

    def test_empty_setup(self):
        """Test creating empty setup."""
        setup = SetupMetadata()
        assert setup.drop is None
        assert setup.connection is None
        assert setup.statement is None

    def test_setup_with_drop(self):
        """Test setup with drop table."""
        setup = SetupMetadata(drop="test_table")
        assert setup.drop == "test_table"

    def test_setup_with_connection(self):
        """Test setup with connection options."""
        setup = SetupMetadata(
            connection=ConnectionOptions.model_validate({"options": {"key": "value"}})
        )
        assert setup.connection is not None
        assert setup.connection.options["key"].apply == "value"
        assert setup.connection.options["key"].revert is None


class TestQueryMetadata:
    """Test the QueryMetadata model."""

    def test_empty_metadata(self):
        """Test creating empty metadata."""
        metadata = QueryMetadata()
        assert metadata.hide is False
        assert metadata.skip is None
        assert metadata.sort_keys is None
        assert metadata.setup is not None
        assert metadata.connection is None
        assert metadata.statement is None
        assert isinstance(metadata.tags, TagsMetadata)

    def test_metadata_with_hide(self):
        """Test metadata with hide flag."""
        metadata = QueryMetadata(hide=True)
        assert metadata.hide is True

    def test_metadata_with_skip(self):
        """Test metadata with skip reason."""
        metadata = QueryMetadata(skip="Not supported")
        assert metadata.skip == "Not supported"

    def test_metadata_with_sort_keys(self):
        """Test metadata with sort-keys."""
        metadata = QueryMetadata.model_validate(
            {"sort-keys": [("col1", "ascending"), ("col2", "descending")]}
        )
        assert metadata.sort_keys == [("col1", "ascending"), ("col2", "descending")]

    def test_metadata_with_setup(self):
        """Test metadata with setup section."""
        metadata = QueryMetadata(setup=SetupMetadata(drop="test_table"))
        assert metadata.setup is not None
        assert metadata.setup.drop == "test_table"

    def test_metadata_with_tags(self):
        """Test metadata with tags."""
        metadata = QueryMetadata(
            tags=TagsMetadata.model_validate({"sql-type-name": "INTEGER"})
        )
        assert metadata.tags.sql_type_name == "INTEGER"


_root = Path(__file__).parent.parent / "adbc_drivers_validation/queries"


@pytest.mark.parametrize(
    "path",
    [
        pytest.param(path, id=str(path.relative_to(_root)))
        for path in _root.rglob("*.toml")
    ],
)
def test_load_all_toml(path: Path):
    with path.open("rb") as f:
        data = tomllib.load(f)
    QueryMetadata.model_validate(data)


@pytest.mark.parametrize(
    "path",
    [
        pytest.param(path, id=str(path.relative_to(_root)))
        for path in _root.rglob("*.txtcase")
    ],
)
def test_load_all_txtcase(path: Path):
    t = adbc_drivers_validation.txtcase.load(path)
    try:
        data = t.get_part("metadata")
    except KeyError:
        return
    QueryMetadata.model_validate(data)
