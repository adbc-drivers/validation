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

import pytest

from adbc_drivers_validation.generate_documentation import TypeTableEntry


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
