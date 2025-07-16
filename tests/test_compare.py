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

import pyarrow
import pytest

import adbc_drivers_validation.compare as compare


def test_compare_fields():
    f1 = pyarrow.field("a", pyarrow.int32(), nullable=True)
    f2 = pyarrow.field("b", pyarrow.int32(), nullable=True)
    f3 = pyarrow.field("a", pyarrow.int64(), nullable=True)
    f4 = pyarrow.field("a", pyarrow.int32(), nullable=False)
    f5 = pyarrow.field(
        "a",
        pyarrow.int32(),
        nullable=True,
        metadata={"key": "value"},
    )
    f6 = pyarrow.field(
        "a",
        pyarrow.int32(),
        nullable=True,
        metadata={"ARROW:extension:metadata": "value"},
    )

    compare.compare_fields(f1, f1)
    compare.compare_fields(f2, f2)
    compare.compare_fields(f3, f3)
    compare.compare_fields(f4, f4)
    compare.compare_fields(f5, f5)
    compare.compare_fields(f6, f6)
    compare.compare_fields(f1, f5)
    compare.compare_fields(f5, f1)

    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f2)
    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f3)
    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f4)
    with pytest.raises(AssertionError):
        compare.compare_fields(f1, f6)
