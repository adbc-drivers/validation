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

# Driver Validation Suite

A reusable, pluggable test suite for ADBC drivers written with Python and
[pytest](https://docs.pytest.org/en/stable/).  The test suite exercises
various ADBC features and different data types, generating a table of
supported (and unsupported) features.  It can accommodate database-specific
syntax and quirks.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Installation & Usage

There are no real docs yet.  It is recommended to look at an existing driver
to see how to set it up.

### Adding New Tests

The validation suite essentially runs a bunch of SQL queries against each
driver.  The queries are tagged with various bits of metadata that are used to
generate documentation.

The queries live under `queries/base`.  They are somewhat subcategorized, but
this is arbitrary and not important to the test runner.  A single query is all
the files in the same directory with the same filename (less extensions).  The
type of query, and hence what kind of test results, depends on which files are
present.

All queries have an optional `query.toml` file defining various metadata:

- `hide` (`bool`) - if `true`, don't run this query (for this driver)
- `skip` (`str`) - if present, skip the query with the given reason (for this
  driver).  `hide` will effectively remove an entry from the generated
  documentation, while `skip` will instead show that the entry is unsupported.
- `sort-keys` (`list[tuple[str, 'ascending' | 'descending']]`) - if present,
  sort the result set by these columns before comparison
- `tags` table:
  - `caveats` (`list[str]`) - if present, a list of footnotes to add to the
    user documentation (e.g. to explain why something is only partially
    supported)
  - `partial-support` (`bool`) - if present, indicate in the user
    documentation that something is only partially supported
  - `sql-type-name` (`str`) - the name of the SQL type being tested (to
    display in the user documentation)

A `SELECT` query just tests running a query and checking the result.  It
consists of:

- `query.sql` - the query to run
- `query.schema.json` - the schema of the result set
- `query.json` - the data of the result set (in JSON Lines)
- `query.setup.sql` (optional) - a query to run before the main query (e.g. to
  create any tables required)
- `query.bind.json` (optional) - data to insert via executing a query with
  bind parameters (in JSON Lines)
- `query.bind.schema.json` (optional) - the schema of the bind data
- `query.bind.sql` (optional) - the query that is executed for bind data.  It
  should always use `$1` style placeholders, which will be replaced at runtime
  with database-specific placeholders

A schema query tests getting the schema of the result set of a query without
actually running it.  It consists of:

- `query.sql`
- `query.schema.json`

An ingest query tests using bulk ingestion to load Arrow data, then querying
the result table.  It consists of:

- `query.input.schema.json` - the schema of the data to insert
- `query.input.json` - the data to insert
- `query.schema.json` (optional) - the schema of the result set (by default it
  is assumed to be the same as the input)
- `query.json` (optional) - the data of the result set (in JSON Lines) (by
  default it is assumed to be the same as the input)

#### Overriding Tests with Driver-Specific Tests

Often a driver needs specific changes to a test case, e.g. because it picks a
different return type.  Also, drivers may support extra features that need
specific test cases, or may not support a feature and need to skip a test
case.  These can be added under a directory specified by the driver quirks.
Instead of duplicating the full test case, as long as a file is present at the
same relative path, it will be used to override that specific part of that
test case for that driver.

#### `.txtcase` Test Format

Instead of creating separate files, a single file with the extension
`.txtcase` can be created instead.  This file uses `//` as comment syntax.
Each file above can be placed in the `.txtcase` file with a `// part: query`
comment indicating which file it is meant to represent.  The comment should
use the following text based on the original file extension:

| file | `// part: ` |
|-|-|
| `.toml` | `metadata` |
| `.setup.sql` | `setup_query` |
| `.bind.sql` | `bind_query` |
| `.bind.schema.json` | `bind_schema` |
| `.bind.json` | `bind` |
| `.sql` | `query` |
| `.schema.json` | `expected_schema` |
| `.json` | `expected` |

Overriding files works normally.  You cannot mix `.txtcase` with other files
for the same query inside the same directory, however, as the framework will
error during test discovery (it would potentially be ambiguous which overload
to use).
