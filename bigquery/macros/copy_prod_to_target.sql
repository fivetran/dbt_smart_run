{% macro copy_prod_to_target(relation_name, prod_schema, metadata_only) %}
  {% set target_schema = generate_schema_name(target.schema) %}

  {% set drop_sql %}
    drop table if exists `{{ target.database }}.{{ target_schema }}.{{ relation_name }}`;
  {%- endset %}

  {% do run_query(drop_sql) %}

  {% if metadata_only %}
    {{ dbt_utils.log_info("Copying object metadata only " ~ relation_name ~ " into target schema " ~ target_schema) }}
    {% set copy_sql %}
      create table `{{ target.database }}.{{ target_schema }}.{{ relation_name }}` like
      `{{ target.database }}.{{ prod_schema }}.{{ relation_name }}`;
    {%- endset %}
  
  {% else %}
    {{ dbt_utils.log_info("Cloning object " ~ relation_name ~ " from configured schema " ~ prod_schema ~ " into schema " ~ target_schema) }}
    {% set copy_sql %}
      create table `{{ target.database }}.{{ target_schema }}.{{ relation_name }}`
      clone `{{ target.database }}.{{ prod_schema }}.{{ relation_name }}`;
    {%- endset %}
  
  {% endif %}
  
  {% do run_query(copy_sql) %}

{% endmacro %}