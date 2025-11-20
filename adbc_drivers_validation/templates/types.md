{#
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
#}

#### {{quirks.vendor_name}} to Arrow

:::{list-table}
:header-rows: 1
:width: 100%
:widths: 1 3

* - SQL Type
  - Arrow Type

{%- for sql_type, arrow_type in type_select|sort %}
* - {{ sql_type }}
  - {{ arrow_type }}
{%- endfor -%}
{{ "" }}
:::

#### Arrow to {{quirks.vendor_name}}

:::{list-table}
:header-rows: 1
:width: 100%
:widths: 1 1 1

* - Arrow Type
  - {{quirks.vendor_name}} Type<br/>Bind
  - {{quirks.vendor_name}} Type<br/>Ingest

{%- for arrow_type, sql_type_bind, sql_type_ingest in type_bind_ingest|sort %}
* - {{ arrow_type }}
  - {{ sql_type_bind }}
  - {{ sql_type_ingest }}
{%- endfor -%}
{{ "" }}
:::
