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

from adbc_drivers_validation.model import FromEnv


def test_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FOO", "bar")
    assert FromEnv("FOO").get_or_raise() == "bar"
    assert FromEnv("FOO", template="foo{}").get_or_raise() == "foobar"
    with pytest.raises(ValueError):
        FromEnv("BAR").get_or_raise()
