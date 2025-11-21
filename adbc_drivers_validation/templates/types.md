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

<table class="docutils data align-default" style="width: 100%;">
  <tr>
    <th rowspan="2" style="text-align: center; vertical-align: middle;">Arrrow Type</th>
    <th colspan="2" style="text-align: center;">{{quirks.vendor_name}} Type</th>
  </tr>
  <tr>
    <th style="text-align: center;">Bind</th>
    <th style="text-align: center;">Ingest</th>
  </tr>
{%- for arrow_type, sql_type_bind, sql_type_ingest in type_bind_ingest|sort %}
<tr>
  <td>{{ arrow_type }}</td>
{%- if sql_type_bind == sql_type_ingest %}
<td colspan="2" style="text-align: center;">{{ sql_type_bind }}</td>
{% else %}
<td style="text-align: center;">{{ sql_type_bind }}</td><td style="text-align: center;">{{ sql_type_ingest }}</td>
{% endif %}
</tr>
{%- endfor %}
</table>
