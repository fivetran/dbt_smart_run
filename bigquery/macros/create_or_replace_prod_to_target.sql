{% macro create_or_replace_prod_to_target(relation_name, prod_schema, prod_materialized, metadata_only) %}
  {% set target_schema = generate_schema_name(target.schema) %}
  {% if metadata_only %}
    {% set sql %}
      drop {{ prod_materialized }} if exists `{{ target.database }}.{{ target_schema }}.{{ relation_name }}`;
      create {{ prod_materialized }} `{{ target.database }}.{{ target_schema }}.{{ relation_name }}` like
      `{{ target.database }}.{{ prod_schema }}.{{ relation_name }}`;
    {%- endset %}
    {{ dbt_utils.log_info("Copying object metadata only" ~ relation_name ~ " from configured schema " ~ prod_schema ~ " into schema " ~ target_schema) }}
  {% else %}
    {% set sql %}
      drop {{ prod_materialized }} if exists `{{ target.database }}.{{ target_schema }}.{{ relation_name }}`;
      create {{ prod_materialized }} `{{ target.database }}.{{ target_schema }}.{{ relation_name }}` as
      select * from `{{ target.database }}.{{ prod_schema }}.{{ relation_name }}`;
    {%- endset %}
    {{ dbt_utils.log_info("Copying object " ~ relation_name ~ " from configured schema " ~ prod_schema ~ " into schema " ~ target_schema) }}
  {% endif %}

  {% do run_query(sql) %}
  {#
    {{ dbt_utils.log_info("Copied object " ~ relation_name ~ " into target schema " ~ target.schema) }}
  #}
{% endmacro %}