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

from pathlib import Path

import pytest
from pydantic import ValidationError

from adbc_drivers_validation.model import FromEnv, Query, SelectQuery


def test_broken_vendor_marks() -> None:
    path = Path("unused.txtcase")
    query = Query(
        "type/select/example",
        SelectQuery(path, path, path),
        [{"tags": {"broken-vendor": "vendor issue"}}],
    )
    assert query.pytest_marks[0].mark.kwargs == {"reason": "vendor issue"}
    assert query.pytest_marks_for_test("test_query")[0].mark.name == "xfail"

    query.metadata_paths = [
        {
            "tags": {
                "broken-vendor": {
                    "reason": "schema issue",
                    "tests": ["test_get_table_schema", "test_create"],
                }
            }
        }
    ]
    assert query.pytest_marks == []
    assert query.pytest_marks_for_test("test_query") == []
    assert query.pytest_marks_for_test("TestQuery.test_get_table_schema") == []
    for name in ("test_get_table_schema", "test_create"):
        (mark,) = query.pytest_marks_for_test(name)
        assert mark.mark.name == "xfail"
        assert mark.mark.kwargs == {"reason": "schema issue"}

    query.metadata_paths = [
        {"tags": {"broken-vendor": {"reason": "schema issue", "tests": []}}}
    ]
    with pytest.raises(ValidationError, match="at least 1 item"):
        query.metadata()


def test_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FOO", "bar")
    assert FromEnv("FOO").get() == "bar"
    assert FromEnv("FOO").get_or_raise() == "bar"
    assert FromEnv("FOO", template="foo{}").get_or_raise() == "foobar"
    with pytest.raises(ValueError):
        FromEnv("BAR").get_or_raise()

    assert FromEnv("BAR").get() is None
    assert FromEnv("BAR", optional=True).get() is None
    assert FromEnv("BAR", optional=True).get_or_raise() is None
