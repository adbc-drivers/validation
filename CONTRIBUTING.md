<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# How to Contribute

All contributors are expected to follow the [Code of
Conduct](https://github.com/adbc-drivers/validation?tab=coc-ov-file#readme).

## Reporting Issues and Making Feature Requests

Please file issues, questions, and feature requests on the GitHub issue
tracker: https://github.com/adbc-drivers/validation/issues

Potential security vulnerabilities should be reported to
[security@adbc-drivers.org](mailto:security@adbc-drivers.org) instead.  See
the [Security Policy](https://github.com/adbc-drivers/validation?tab=security-ov-file#readme).

## Setting Up a Developer Environment

1. Install [Python](https://www.python.org/).
1. Create a virtual environment, if needed.
1. Install the project: `pip install --editable .`.

## Opening a Pull Request

Before opening a pull request:

- Review your changes and make sure no stray files, etc. are included.
- Ensure the Apache license header is at the top of all files.
- Check if there is an existing issue.  If not, please file one, unless the
  change is trivial.
- Assign the issue to yourself by commenting just the word `take`.
- Run the static checks by installing [pre-commit](https://pre-commit.com/),
  then running `pre-commit run --all-files` from inside the repository.  Make
  sure all your changes are staged/committed (unstaged changes will be
  ignored).

When writing the pull request description:

- Ensure the title follows [Conventional
  Commits](https://www.conventionalcommits.org/en/v1.0.0/) format.  No
  component is necessary.  Example titles:

  - `feat: test ingestion with target schema`
  - `fix: don't assume SQLSTATE values`
  - `feat!: test multiple types in one query`

  Ensure that breaking changes are appropriately flagged with a `!` as seen
  above.
- Make sure the bottom of the description has `Closes #NNN`, `Fixes #NNN`, or
  similar, so that the issue will be linked to your pull request.
