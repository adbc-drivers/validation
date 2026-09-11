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

_EXPECTED_STRING = "0123456789abcdef" * 16 + "0"
_EXPECTED_BINARY = bytes(range(256)) + b"\x00"


@pytest.mark.parametrize(
    "value_type,expected",
    [
        pytest.param(pyarrow.string(), _EXPECTED_STRING, id="string"),
        pytest.param(pyarrow.large_string(), _EXPECTED_STRING, id="large_string"),
        pytest.param(pyarrow.string_view(), _EXPECTED_STRING, id="string_view"),
        pytest.param(pyarrow.binary(), _EXPECTED_BINARY, id="binary"),
        pytest.param(pyarrow.large_binary(), _EXPECTED_BINARY, id="large_binary"),
        pytest.param(pyarrow.binary_view(), _EXPECTED_BINARY, id="binary_view"),
    ],
)
def test_make_long_values(value_type: pyarrow.DataType, expected: str | bytes) -> None:
    assert _make_long_values(value_type, sizes=[257]) == [expected]


def test_make_long_values_unsupported_type() -> None:
    with pytest.raises(TypeError, match="got int64"):
        _make_long_values(pyarrow.int64())
