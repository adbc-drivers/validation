# Copyright (c) 2026 ADBC Drivers Contributors
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

import json
from pathlib import Path

import pytest

from adbc_drivers_validation import model
from adbc_drivers_validation.generate_documentation import (
    DriverTypeTable,
    TypeTableEntry,
    ValidationReport,
    VendorVersion,
    render,
)


def test_type_table_entry() -> None:
    # fmt:off
    entry1 = TypeTableEntry("1", ("a",), "passed", (" [f1]",), None)
    entry2 = TypeTableEntry("1", ("b", "c"), "passed", (" [f2]",), None)
    entry3 = TypeTableEntry("1", ("d",), "partial", (" [f3]", " [f4]",), None)
    entry4 = TypeTableEntry("1", ("e",), "failed", (" [f5]",), None)
    entry5 = TypeTableEntry("1", ("b",), "passed", (), None)
    entry6 = TypeTableEntry("1", ("b", "d"), "partial", (" [f6]",), None)
    entry7 = TypeTableEntry("1", ("a", "b"), "failed", (" [f1]",), None)
    entry8 = TypeTableEntry("2", ("a",), "passed", (" [f1]",), None)
    entry9 = TypeTableEntry("1", ("b",), "passed", (" [f2]",), "Variant")
    # fmt:on

    assert entry1.render_rhs() == "a [f1]"
    assert entry2.render_rhs() == "b, c [f2]"
    assert entry3.render_rhs() == "d ⚠️ [f3] [f4]"
    assert entry4.render_rhs() == "❌ [f5]"

    assert entry1.merge(entry5).render_rhs() == "a, b [f1]"
    assert entry5.merge(entry1).render_rhs() == "a, b [f1]"
    assert entry1.merge(entry3).render_rhs() == "a, d ⚠️ [f1] [f3] [f4]"
    assert entry3.merge(entry1).render_rhs() == "a, d ⚠️ [f1] [f3] [f4]"
    assert entry1.merge(entry4).render_rhs() == "a ⚠️ [f1] [f5]"
    assert entry4.merge(entry1).render_rhs() == "a ⚠️ [f1] [f5]"

    assert entry2.merge(entry6).render_rhs() == "b, c, d ⚠️ [f2] [f6]"
    assert entry6.merge(entry2).render_rhs() == "b, c, d ⚠️ [f2] [f6]"
    assert entry2.merge(entry4).render_rhs() == "b, c ⚠️ [f2] [f5]"
    assert entry4.merge(entry2).render_rhs() == "b, c ⚠️ [f2] [f5]"

    assert entry4.merge(entry7).render_rhs() == "❌ [f1] [f5]"
    assert entry7.merge(entry4).render_rhs() == "❌ [f1] [f5]"

    with pytest.raises(ValueError, match="1 and 2"):
        entry1.merge(entry8)

    with pytest.raises(ValueError, match="None and Variant"):
        entry1.merge(entry9)


class _NoBindNoIngestQuirks(model.DriverQuirks):
    name = "nobind"
    driver = "adbc_driver_nobind"
    driver_name = "No Bind Driver"
    vendor_name = "No Bind"
    vendor_version = "1"
    short_version = "1"
    features = model.DriverFeatures()
    setup = model.DriverSetup()

    @property
    def queries_paths(self) -> tuple[Path]:
        return (Path(__file__).parent,)

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        return False


def test_render_without_bind_or_ingest_results(tmp_path: Path) -> None:
    quirks = _NoBindNoIngestQuirks()
    report = ValidationReport(
        driver="nobind",
        versions={
            VendorVersion("nobind", "1"): DriverTypeTable(
                quirks=quirks,
                features=quirks.features,
                type_select=[TypeTableEntry("INT", ("int32",), "passed", (), None)],
            )
        },
    )
    template = tmp_path / "nobind.md"
    template.write_text("{{ types|safe }}\n")

    render(report, [("nobind", "No Bind")], template, tmp_path)

    rendered = (tmp_path / "nobind.md").read_text()
    assert "Arrow to Database" in rendered
    assert "int32" in rendered


def test_broken_vendor_footnotes() -> None:
    quirks = _NoBindNoIngestQuirks()
    version = VendorVersion("nobind", "1")
    table = DriverTypeTable(quirks=quirks, features=quirks.features)
    report = ValidationReport(driver="nobind", versions={version: table})

    report.add_table_entry(
        "nobind",
        "1",
        "select",
        "INT",
        "int32",
        {
            "test_results": ["passed", "passed"],
            "metadata": [
                json.dumps({"tags": {"broken-vendor": "legacy reason"}}),
                json.dumps(
                    {
                        "tags": {
                            "broken-vendor": {
                                "reason": "schema issue",
                                "tests": ["test_get_table_schema"],
                            }
                        }
                    }
                ),
            ],
        },
    )

    assert list(report.footnotes.values()) == ["legacy reason", "schema issue"]
