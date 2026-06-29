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

import base64
import uuid

import pytest

import adbc_drivers_validation.variant as variant


@pytest.mark.parametrize(
    "value",
    [
        variant.VariantNull(),
        variant.VariantBool(True),
        variant.VariantBool(False),
        variant.VariantInt8(1),
        variant.VariantInt8(-128),
        variant.VariantInt8(127),
        variant.VariantInt16(-32768),
        variant.VariantInt16(32767),
        variant.VariantInt32(-2147483648),
        variant.VariantInt32(2147483647),
        variant.VariantInt64(-9223372036854775808),
        variant.VariantInt64(9223372036854775807),
        variant.VariantDecimal4(1, 123456789),
        variant.VariantDecimal8(2, 123456789012345678),
        variant.VariantDecimal16(3, 123456789012345678901234567),
        variant.VariantFloat(1.5),
        variant.VariantDouble(1.23),
        variant.VariantDate(10),
        variant.VariantTimestampMicros(123456, utc=False),
        variant.VariantTimestampMicros(123456, utc=True),
        variant.VariantTimestampNanos(123456789, utc=False),
        variant.VariantTimestampNanos(123456789, utc=True),
        variant.VariantTimeMicros(86399999999),
        variant.VariantBinary(b"\x00\x01\x02"),
        variant.VariantString("Hello, world!"),
        variant.VariantString("Hello, world!", force_long=True),
        variant.VariantUUID(uuid.UUID("12345678-1234-5678-1234-567812345678").bytes),
        variant.VariantArray(()),
        variant.VariantArray(
            (variant.VariantInt8(1), variant.VariantInt8(2), variant.VariantInt8(3))
        ),
        variant.VariantObject(()),
        variant.VariantObject(
            (
                ("a", variant.VariantInt8(1)),
                (
                    "b",
                    variant.VariantArray(
                        (
                            variant.VariantString("x"),
                            variant.VariantBool(True),
                            variant.VariantNull(),
                        )
                    ),
                ),
            ),
        ),
    ],
)
def test_round_trip(value: variant.VariantValue) -> None:
    metadata, encoded_value = value.to_variant_bytes()
    assert variant.parse_variant(metadata, encoded_value) == value


def test_object_primitive() -> None:
    # From apache/parquet-testing:
    # https://github.com/apache/parquet-testing/tree/master/variant
    #
    # Source fixture files:
    # - variant/object_primitive.metadata
    # - variant/object_primitive.value
    #
    # Expected logical value source:
    # - variant/data_dictionary.json key "object_primitive"
    metadata = base64.b64decode(
        "AQcACRUnOkZQX2ludF9maWVsZGRvdWJsZV9maWVsZGJvb2xlYW5fdHJ1ZV9maWVs"
        "ZGJvb2xlYW5fZmFsc2VfZmllbGRzdHJpbmdfZmllbGRudWxsX2ZpZWxkdGltZX"
        "N0YW1wX2ZpZWxk"
    )
    value = base64.b64decode(
        "AgcDAgEABQQGCQgCABkKGjEMASAIFc1bBwQIOUFwYWNoZSBQYXJxdWV0AFkyMD"
        "I1LTA0LTE2VDEyOjM0OjU2Ljc4"
    )

    assert variant.parse_variant(metadata, value) == variant.VariantObject(
        (
            ("boolean_false_field", variant.VariantBool(False)),
            ("boolean_true_field", variant.VariantBool(True)),
            ("double_field", variant.VariantDecimal4(8, 123456789)),
            ("int_field", variant.VariantInt8(1)),
            ("null_field", variant.VariantNull()),
            ("string_field", variant.VariantString("Apache Parquet")),
            ("timestamp_field", variant.VariantString("2025-04-16T12:34:56.78")),
        )
    )


@pytest.mark.parametrize(
    "encoded",
    [
        pytest.param(
            b"\x03\x01\x00\x01\x0c\x01",
            id="element_reads_past_declared_payload",
        ),
        pytest.param(
            b"\x03\x02\x00\x02\x01\x0c\x01\x0c\x02",
            id="non_monotonic_offsets",
        ),
    ],
)
def test_array_rejects_invalid_offsets(encoded: bytes) -> None:
    with pytest.raises(ValueError, match="Invalid Variant array"):
        variant.parse_variant(b"\x01\x00\x00", encoded)


@pytest.mark.parametrize(
    "encoded",
    [
        pytest.param(
            b"\x02\x01\x00\x00\x01\x0c\x01",
            id="field_reads_past_declared_payload",
        ),
        pytest.param(
            b"\x02\x02\x00\x01\x00\x02\x01\x0c\x01\x0c\x02",
            id="field_start_past_declared_payload",
        ),
    ],
)
def test_object_rejects_invalid_offsets(encoded: bytes) -> None:
    metadata, _ = variant.VariantObject(
        (("a", variant.VariantNull()), ("b", variant.VariantNull()))
    ).to_variant_bytes()

    with pytest.raises(ValueError, match="Invalid Variant object"):
        variant.parse_variant(metadata, encoded)
