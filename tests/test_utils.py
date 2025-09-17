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

import traceback

import pytest

import adbc_drivers_validation.utils as utils


def test_scoped_trace() -> None:
    with pytest.raises(ValueError) as excinfo:
        with utils.scoped_trace("additional context"):
            raise ValueError("original error")

    assert "additional context" in excinfo.value.__notes__
    assert "original error" in str(excinfo.value)
    tb = "".join(traceback.format_exception(excinfo.value))
    assert "additional context" in tb
