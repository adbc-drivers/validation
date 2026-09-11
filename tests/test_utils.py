# Copyright (c) 2025-2026 ADBC Drivers Contributors
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

import time
import traceback

import adbc_driver_manager
import pyarrow
import pytest

import adbc_drivers_validation.utils as utils


def test_retry_adbc_operation(monkeypatch: pytest.MonkeyPatch) -> None:
    class RetryOnceDriver:
        def __init__(self) -> None:
            self.errors: list[Exception] = []

        def is_retryable(self, error: Exception) -> bool:
            self.errors.append(error)
            return len(self.errors) == 1

    attempts = 0
    error = adbc_driver_manager.Error(
        "retry requested",
        status_code=adbc_driver_manager.AdbcStatusCode.IO,
    )

    def operation() -> int:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise error
        return 42

    delays: list[int] = []
    monkeypatch.setattr(time, "sleep", delays.append)
    driver = RetryOnceDriver()

    assert utils.retry_adbc_operation(operation, driver.is_retryable) == 42
    assert driver.errors == [error]
    assert delays == [4]


def test_scoped_trace() -> None:
    with pytest.raises(ValueError) as excinfo:
        with utils.scoped_trace("additional context"):
            raise ValueError("original error")

    assert "additional context" in excinfo.value.__notes__
    assert "original error" in str(excinfo.value)
    tb = "".join(traceback.format_exception(excinfo.value))
    assert "additional context" in tb


def test_merge_into() -> None:
    target = {}
    values = {"a": 1, "b": {"c": 2}}
    utils.merge_into(target, values)
    assert target == {"a": 1, "b": {"c": 2}}

    target = {"b": {"d": 3}}
    values = {"a": 1, "b": {"c": 2}}
    utils.merge_into(target, values)
    assert target == {"a": 1, "b": {"c": 2, "d": 3}}

    target = {"a": [1]}
    values = {"a": [2, 3]}
    utils.merge_into(target, values)
    assert target == {"a": [2, 3]}

    target = {"a": [1]}
    values = {"a": {"b": 2}}
    utils.merge_into(target, values)
    assert target == {"a": {"b": 2}}


@pytest.fixture(params=["binary_view", "string_view"])
def take_table(request: pytest.FixtureRequest) -> pyarrow.Table:
    if request.param == "binary_view":
        ty = pyarrow.binary_view()
        schema = pyarrow.schema([("idx", pyarrow.int64()), ("col", ty)])
        return pyarrow.table(
            {
                "idx": [0, 1, 2],
                "col": [b"foo", b"bar", b"baz"],
            },
            schema=schema,
        )
    elif request.param == "string_view":
        ty = pyarrow.string_view()
        schema = pyarrow.schema([("idx", pyarrow.int64()), ("col", ty)])
        return pyarrow.table(
            {
                "idx": [0, 1, 2],
                "col": ["foo", "bar", "baz"],
            },
            schema=schema,
        )
    else:
        raise ValueError(f"Unknown parameter: {request.param}")


def test_take(take_table: pyarrow.Table) -> None:
    table = utils.take(take_table, pyarrow.array([0, 1, 2]))
    assert table.equals(take_table)


@pytest.mark.xfail(raises=pyarrow.ArrowNotImplementedError)
def test_pyarrow_take(take_table: pyarrow.Table) -> None:
    table = take_table.take([0, 1, 2])
    assert table.equals(take_table)


def test_sort_by(take_table: pyarrow.Table) -> None:
    table = utils.sort_by(take_table, [("idx", "ascending")])
    assert table.equals(take_table)

    table = utils.sort_by(take_table, [("idx", "descending")])
    assert not table.equals(take_table)
