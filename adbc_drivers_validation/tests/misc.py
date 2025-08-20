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

import subprocess
import typing
from pathlib import Path

from adbc_drivers_validation import model


def generate_tests(quirks: list[model.DriverQuirks], metafunc) -> None:
    """Parameterize the tests in this module for the given driver."""
    combinations = []
    for driver in quirks:
        combinations.append(driver.name)

    metafunc.parametrize(
        "driver",
        combinations,
        scope="module",
        indirect=["driver"],
    )


# TODO(lidavidm): once repos are OSS, we can just pull down adbc-drivers/dev
# to do this, but in the meantime, managing more deploy keys is getting
# ridiculous
def detect_version(driver: str, cwd: Path | None = None) -> str:
    tags = (
        subprocess.check_output(
            [
                "git",
                "tag",
                "-l",
                "--no-column",
                "--no-format",
                "--no-color",
                "--sort",
                "-v:refname",
                f"{driver}/v*",
            ],
            cwd=cwd,
            text=True,
        )
        .strip()
        .splitlines()
    )
    if not tags:
        version = "unknown"
    else:
        tag = tags[0]
        _, _, version = tag.partition("/")
        # If we are not on the tag, append the commit count and hash
        count = int(
            subprocess.check_output(
                ["git", "rev-list", f"{tag}..HEAD", "--count"], cwd=cwd, text=True
            ).strip()
        )
        if count > 0:
            rev = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], cwd=cwd, text=True
            ).strip()
            version += f"-dev.{count}.{rev}"

    # Append -dirty if there are uncommitted changes
    dirty = (
        subprocess.check_output(["git", "status", "--porcelain"], cwd=cwd, text=True)
        .strip()
        .splitlines()
    )
    # Ignore untracked files
    if any(not line.startswith("?? ") for line in dirty):
        version += "-dirty"

    return version


class TestMisc:
    def test_metadata(
        self,
        driver: model.DriverQuirks,
        record_property: typing.Callable[[str, typing.Any], None],
    ) -> None:
        record_property("driver_version", detect_version(driver.name))
