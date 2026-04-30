{#
  Copyright (c) 2025-2026 ADBC Drivers Contributors

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

{# Note: The blank lines below make footnotes work #}
{# See https://github.com/adbc-drivers/validation/issues/114 #}

#### Database to Arrow

<table class="docutils data align-default" style="width: 100%;">
<thead>
<tr>
<th style="text-align: left; vertical-align: middle;">Database Type</th>
{% for col in type_select_columns %}
<th style="text-align: center;">{{vendor_friendly_name[col]}}</th>
{% endfor %}
</tr>
</thead>
<tbody>
{% for row in type_select %}
<tr>
{% for span, cell in row %}
{% if loop.index == 1 %}
<td style="text-align: left;">

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

#### Arrow to Database

<table class="docutils data align-default" style="width: 100%;">
<thead>
<tr>
<th rowspan="3" style="text-align: left; vertical-align: middle;">Arrow Type</th>
{% for v in type_bind_ingest_vendors %}
<th colspan="{{type_bind_ingest_columns[v]|length}}" style="text-align: center;">{{vendor_friendly_name[v]}} Type</th>
{% endfor %}
</tr>
<tr>
{% for v in type_bind_ingest_vendors %}
{% for col in type_bind_ingest_columns[v] %}
<th style="text-align: center;">{{col}}</th>
{% endfor %}
{% endfor %}
</tr>
</thead>
<tbody>
{% for row in type_bind_ingest %}
<tr>
{% for span, cell in row %}
{% if loop.index == 1 %}
<td style="text-align: left;">

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
