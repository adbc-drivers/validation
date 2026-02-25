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

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## General Notes

- Use `gh` CLI to read GitHub issues.
- Research whether the database system in question supports the type or not. If it does not, then the best course of action is to create a txtcase file with `hide = true`. Otherwise you may want to use either `broken-driver` or `broken-vendor`.
- See `query_metadata.py` for the format of metadata.
- Don't cram multiple shell commands in one attempt as it forces me to manually review every time.
- Include license headers in txtcase files.
- When generating query metadata, make sure to define sql-type-name. Again, research what the right name for the particular vendor is.

## Architecture Overview

### Project Structure

- `adbc_drivers_validation/`: Main package source code
  - `model.py`: Core data models for queries, drivers, and test case configuration
  - `quirks.py`: Driver-specific quirks and SQL dialect handling
  - `compare.py`: Comparison utilities for test results
  - `arrowjson.py`: Utilities for working with Arrow schemas and tables in JSON format
  - `tests/`: Test framework modules
    - `conftest.py`: Pytest configuration and common fixtures
    - `query.py`: Tests for running SQL queries
    - `ingest.py`: Tests for bulk data ingestion
    - `statement.py`: Tests for statement handling
    - `connection.py`: Tests for connection handling
  - `queries/`: Contains test case definitions
    - `type/literal/`: Tests for literal values
    - `type/select/`: Tests for selecting values from tables
    - `type/bind/`: Tests for binding parameters
    - `ingest/`: Tests for bulk data ingestion
  - `templates/`: Templates for documentation generation

### Key Concepts

**Query Test Cases**: Each test case consists of multiple files with the same base name:

- `.sql`: The query to execute
- `.schema.json`: The expected schema of the result
- `.json`: The expected data result
- `.setup.sql`: Optional setup SQL to run before the main query
- `.bind.sql`: Optional query to execute with parameters
- `.bind.schema.json`: Schema of the bind parameters
- `.bind.json`: Data for the bind parameters
- `.toml`: Metadata about the test case (tags, skip conditions, etc.)

Alternatively, these files can be consolidated into a single `.txtcase` file.  Inside the file, a `// part: ` comment is needed before the file contents to identify the data.  For example, instead of having a `int64.toml`, `int64.sql`, `int64.json`, and `int64.schema.json`, you would have a `int64.txtcase` file:

```
// part: metadata
toml contents

// part: query
SELECT CAST(1 AS BIGINT)

// part: expected_schema
schema JSON

// part: expected
{"res": 1}
```

This is how the extension corresponds to the part comment:

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

Read README.md to learn more.
