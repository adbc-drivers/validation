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

* - {{quirks.vendor_name}} Type
  - Arrow Type

{%- for entry in type_select|sort(attribute="lhs") %}

* - {{ entry.lhs }}
  - {{ entry.render_rhs() }}
{%- endfor -%}
{{ "" }}
:::

#### Arrow to {{quirks.vendor_name}}

{# Note: The blank lines below make footnotes work #}
{# See https://github.com/adbc-drivers/validation/issues/114 #}

<table class="docutils data align-default" style="width: 100%;">
<thead>
<tr>
<th rowspan="2" style="text-align: center; vertical-align: middle;">Arrow Type</th>
<th colspan="{{type_bind_ingest_columns|length}}" style="text-align: center;">{{quirks.vendor_name}} Type</th>
</tr>
<tr>
{% for col in type_bind_ingest_columns %}
<th style="text-align: center;">{{col}}</th>
{% endfor %}
</tr>
</thead>
<tbody>
{% for row in type_bind_ingest %}
<tr>
{% for span, cell in row %}
{% if loop.index == 0 %}
<td>

{{cell}}

</td>
{% elif span == 1 %}
<td style="text-align: center;">

{{cell}}

</td>
{% else %}
<td colspan="{{span}}" style="text-align: center;">

{{cell}}

</td>
{% endif %}
{% endfor %}
</tr>
{% endfor %}
</tbody>
</table>
