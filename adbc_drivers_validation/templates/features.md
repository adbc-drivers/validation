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
<table class="docutils data align-default" style="width: 100%">
  <colgroup>
    <col span="1" style="width: 25%;">
    <col span="1" style="width: 25%;">
    <col span="1" style="width: 50%;">
  </colgroup>
  <thead>
    <tr>
      <th>Feature</th>
      <th colspan="2">Support</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="8">Bulk Ingestion</td>
      <td>Create</td>
      <td>{% if ingest["create"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Append</td>
      <td>{% if ingest["append"] and ingest["append_fail"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Create/Append</td>
      <td>{% if ingest["createappend"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Replace</td>
      <td>{% if ingest["replace"] and ingest["replace_noop"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Temporary Table</td>
      <td>{% if ingest["temporary"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Specify target catalog</td>
      <td>{% if ingest["catalog"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Specify target schema</td>
      <td>{% if ingest["schema"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Non-nullable fields are marked NOT NULL</td>
      <td>{% if ingest["not_null"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td rowspan="4">Catalog (GetObjects)</td>
      <td>depth=catalogs</td>
      <td>{% if get_objects["catalog"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>depth=db_schemas</td>
      <td>{% if get_objects["schema"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>depth=tables</td>
      <td>{% if get_objects["table"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>depth=columns (all)</td>
      <td>{% if get_objects["column"] %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Get Parameter Schema</td>
      <td colspan="2">{% if features.statement_get_parameter_schema %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Get Table Schema</td>
      <td colspan="2">{% if get_table_schema %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Prepared Statements</td>
      <td colspan="2">{% if features.statement_prepare %}✅{% else %}❌{% endif %}</td>
    </tr>
    <tr>
      <td>Transactions</td>
      <td colspan="2">{% if features.connection_transactions %}✅{% else %}❌{% endif %}</td>
    </tr>
  </tbody>
</table>

{% if custom_features.groups %}
<table class="docutils data align-default" style="width: 100%">
  <colgroup>
    <col span="1" style="width: 25%;">
    <col span="1" style="width: 25%;">
    <col span="1" style="width: 10%;">
    <col span="1" style="width: 40%;">
  </colgroup>
  <thead>
    <tr>
      <th>Feature</th>
      <th>Name</th>
      <th>Support</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    {%- for group, features in custom_features.groups.items() %}
    {%- for feature in features %}
    <tr>
    {%- if loop.first %}
      <td rowspan="{{ features|length }}">{{ group }}</td>
    {%- endif %}
      <td>{{ feature.name }}</td>
      <td>{% if feature.supported %}✅{% else %}❌{% endif %}</td>
      <td>{{ feature.description }}</td>
    </tr>
    {%- endfor %}
    {%- endfor %}
  </tbody>
</table>
{% endif %}
