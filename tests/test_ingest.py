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

import pyarrow
import pytest

from adbc_drivers_validation.tests.ingest import _make_long_values


@pytest.mark.parametrize(
    "value_type,value_class",
    [
        pytest.param(pyarrow.string(), str, id="string"),
        pytest.param(pyarrow.large_string(), str, id="large_string"),
        pytest.param(pyarrow.string_view(), str, id="string_view"),
        pytest.param(pyarrow.binary(), bytes, id="binary"),
        pytest.param(pyarrow.large_binary(), bytes, id="large_binary"),
        pytest.param(pyarrow.binary_view(), bytes, id="binary_view"),
    ],
)
def test_make_long_values(value_type: pyarrow.DataType, value_class: type) -> None:
    sizes = [1, 257, 4096]
    values = _make_long_values(value_type, sizes=sizes)
    assert [len(value) for value in values] == sizes
    assert all(isinstance(value, value_class) for value in values)
    # Values must be deterministic so that expected results stay stable.
    assert _make_long_values(value_type, sizes=sizes) == values


def test_make_long_values_unsupported_type() -> None:
    with pytest.raises(TypeError, match="got int64"):
        _make_long_values(pyarrow.int64())
