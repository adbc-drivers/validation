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

import typing
from unittest.mock import MagicMock

import adbc_driver_manager
import pyarrow
import pytest

from adbc_drivers_validation import model
from adbc_drivers_validation.tests import query as query_tests


@pytest.fixture
def driver() -> MagicMock:
    driver = MagicMock(spec=model.DriverQuirks)
    driver.name = "test"
    driver.short_version = "1"
    driver.features = model.DriverFeatures(statement_bind=True)
    driver.query_set = model.base_query_set()
    driver.bind_parameter.return_value = "?"
    return driver


@pytest.mark.parametrize("variant", ["stream", "dictionary"])
@pytest.mark.parametrize("custom_queries", [False, True])
@pytest.mark.parametrize("bind_supported", [False, True])
def test_generate_bind_variants(
    driver: MagicMock,
    variant: str,
    custom_queries: bool,
    bind_supported: bool,
) -> None:
    driver.features.statement_bind = bind_supported
    metafunc = MagicMock(spec=pytest.Metafunc)
    metafunc.definition = MagicMock()
    metafunc.definition.name = f"test_query_bind_{variant}"
    metafunc.definition.iter_markers.return_value = []
    options = {}
    expected = {"type/bind/string", "type/bind/large_string"}
    if custom_queries:
        expected = {"type/bind/int32"}
        options[f"bind_{variant}_queries"] = expected

    query_tests.generate_tests([driver], metafunc, **options)

    metafunc.parametrize.assert_called_once()
    params = metafunc.parametrize.call_args.args[1]
    assert {param.values[1].name for param in params} == expected
    for param in params:
        skip_reasons = [mark.kwargs["reason"] for mark in param.marks]
        assert skip_reasons == ([] if bind_supported else ["bind not supported"])


@pytest.mark.parametrize("mode", ["insert", "select"])
@pytest.mark.parametrize("fixture_setup", [False, True])
@pytest.mark.parametrize(
    "batch_size,empty_batches,batch_lengths",
    [
        (None, False, [5]),
        (2, False, [2, 2, 1]),
        (2, True, [0, 2, 0, 2, 0, 1, 0]),
    ],
)
def test_bind_stream(
    driver: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
    mode: typing.Literal["insert", "select"],
    fixture_setup: bool,
    batch_size: int | None,
    empty_batches: bool,
    batch_lengths: list[int],
) -> None:
    driver.features.statement_bind_test_mode = mode
    driver.features.select_fixture_setup = fixture_setup
    query = driver.query_set.queries["type/bind/string"]
    conn = MagicMock()
    statement = conn.cursor.return_value.__enter__.return_value.adbc_statement
    setup = MagicMock()
    monkeypatch.setattr(query_tests, "_setup_query", setup)

    def bind_stream(stream: pyarrow.RecordBatchReader) -> None:
        batches = list(stream)
        assert [batch.num_rows for batch in batches] == batch_lengths
        data = pyarrow.Table.from_batches(batches, schema=stream.schema)
        handle = adbc_driver_manager.ArrowArrayStreamHandle()
        data.to_reader()._export_to_c(handle.address)
        statement.execute_query.return_value = handle, -1

    statement.bind_stream.side_effect = bind_stream

    query_tests.TestQuery().test_query_bind_stream(
        driver, conn, query, batch_size, empty_batches
    )

    statement.bind_stream.assert_called_once()
    statement.bind.assert_not_called()
    statement.execute_query.assert_called_once()
    assert statement.execute_update.call_count == (1 if mode == "insert" else 0)
    assert setup.call_count == (1 if mode == "insert" and fixture_setup else 0)
