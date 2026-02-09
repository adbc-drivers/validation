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

"""File format containing a validation case in one file."""

import re
import tomllib
import typing
from pathlib import Path
from typing import Self

import pyarrow

from . import arrowjson


class TxtCase:
    """A validation case stored in a single text file."""

    def __init__(self, path: Path, parts: dict[str, str]) -> None:
        self._path = path
        self._parts = parts

    @classmethod
    def load(cls, path: str | Path) -> Self:
        with Path(path).open("r") as source:
            return cls.loadf(path, source)

    @classmethod
    def loads(cls, path: str | Path, content: str) -> Self:
        import io

        with io.StringIO(content) as source:
            return cls.loadf(path, source)

    @classmethod
    def loadf(cls, path: str | Path, source) -> Self:
        part_re = re.compile(r"^//\s*part:\s*(\w+)\s*[\s-]*$")

        parts = {}
        current_part = None
        current_part_content = []
        for i, line in enumerate(source):
            if (m := part_re.match(line)) is not None:
                if current_part is not None:
                    parts[current_part] = "".join(current_part_content)
                    current_part_content = []
                current_part = m.group(1)
            elif line.startswith("//"):
                # txtcase comment (differentiate from SQL, TOML comment)
                continue
            else:
                if current_part is None:
                    if not line.strip():
                        continue
                    raise ValueError(f"{path}:L{i + 1}: content outside of part")
                current_part_content.append(line)

        if current_part is not None:
            parts[current_part] = "".join(current_part_content)
            current_part_content = []

        return cls(Path(path), parts)

    @property
    def parts(self) -> list[str]:
        return list(self._parts.keys())

    def get_part(self, part: str, schema: pyarrow.Schema | None = None) -> typing.Any:
        value = self._parts.get(part)
        if value is None:
            raise KeyError(f"'{part}' not in {self._path}")

        if part in {"bind", "expected", "input"}:
            if schema is None:
                raise ValueError(f"'{part}' requires a schema")
            return arrowjson.loads_table(value, schema)

        if schema is not None:
            raise ValueError(f"'{part}' does not require a schema")

        if part == "metadata":
            return tomllib.loads(value)
        if part in {"bind_schema", "expected_schema", "input_schema"}:
            return arrowjson.loads_schema(value)
        elif part in {"bind_query", "query", "setup_query"}:
            return value

        raise NotImplementedError(f"Part '{part}' not implemented in {self._path}")


def load(path: str | Path) -> TxtCase:
    """Load a TxtCase from a file."""
    return TxtCase.load(path)


def try_load(path: str | Path) -> TxtCase | None:
    p = Path(path)
    if p.suffixes != [".txtcase"]:
        return None
    return TxtCase.load(path)
