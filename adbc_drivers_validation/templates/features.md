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
<table class="docutils data align-default" style="width: 100%">
  <colgroup>
    <col span="1" style="width: 25%;">
    <col span="1" style="width: 25%;">
{% for vendor in vendors %}
    <col span="1" style="width: {{ 50 / len(vendors) }}%;">
{% endfor %}
  </colgroup>
  <thead>
    <tr>
      <th colspan="2">Feature</th>
{% for vendor in vendors %}
      <th style="text-align: center;">{{ vendor }}</th>
{% endfor %}
    </tr>
  </thead>
  <tbody>
{% for feature in features %}
{% for subfeature in feature["subfeatures"] %}
    <tr>
{% if loop.index == 1 and len(feature["subfeatures"]) > 1 %}
      <td rowspan="{{ len(feature["subfeatures"]) }}">{{ feature["feature"] }}</td>
{% elif loop.index == 1 and subfeature["subfeature"] == "" %}
      <td colspan="2">{{ feature["feature"] }}</td>
{% elif loop.index == 1 %}
      <td>{{ feature["feature"] }}</td>
{% endif %}
{% if subfeature["subfeature"] != "" %}
      <td>{{ subfeature["subfeature"]}}</td>
{% endif %}
{% for span in subfeature["support"] %}
      <td colspan="{{ span[0] }}" style="text-align: center;">{{ span[1] }}</td>
{% endfor %}
    </tr>
{% endfor %}
{% endfor %}
  </tbody>
</table>
