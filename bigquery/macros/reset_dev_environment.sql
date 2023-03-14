{#- based on  https://discourse.getdbt.com/t/creating-a-dev-environment-quickly-on-snowflake/1151 -#}

{% macro reset_models_in_dev(models="", metadata_only=false) %}
  {#- This macro rebuilds parents of provided model within your current development environment via select * from prod. If no models are specified, all models in project will be rebuilt.
    To run it:
      $ dbt run-operation reset_models_in_dev --args "{'models': 'model_a model_b'}"
    
    To specify metadata_only, use the following argument: 
      $ dbt run-operation reset_models_in_dev --args "{'models': 'model_a model_b', 'metadata_only': true}"
  -#}

  {% set model_list = models.split(' ') %}
  {% set target_schema = generate_schema_name(target.schema) %}

  {% if target.name == 'dev' %}
    
    {% if model_list|length < 1 or (model_list|length == 1 and model_list[0] == '') %}
      {{ dbt_utils.log_info('No models have been specified. All models in project will be rebuilt.')}}
      {% set model_list = [] %}
      {% set model_nodes = graph.nodes.values() | selectattr("resource_type", "==", 'model') | selectattr("package_name", "==", 'prj_production') | list %}
      {% for model_node in model_nodes %}
        {% if model_node["name"] not in model_list %}
          {% do model_list.append(model_node["name"]) %}
        {% endif %}
      {% endfor %}

      {{ reset_dev_for_list_of_models(model_list, metadata_only) }}
    
    {% else %}
      {{ dbt_utils.log_info(model_list|length ~ " models provided") }}
      {{ dbt_utils.log_info("Building list of parent models to copy") }}
      {% set parent_model_list = [] %}
      
      {% for model_name in model_list %}
        {% if model_name == '' %}
          {{ dbt_utils.log_info("Skipping blank model_name (did you have extra spaces in your list of models?)") }}
        {% else %}
          {# see https://docs.getdbt.com/reference/dbt-jinja-functions/graph#accessing-models #}
          {% set child_nodes = graph.nodes.values() | selectattr("resource_type", "==", 'model') | selectattr("name", "==", model_name) | list %}
          
          {% if child_nodes|length < 1 or child_nodes[0] == '' %}
            {{ dbt_utils.log_info("Model `" ~ model_name ~ "` not found. Check your spelling and try again; if this model is from a package, try running `dbt deps`") }}
          
          {% else %}
            {% for child_node in child_nodes %}

              {{ dbt_utils.log_info("Fetching parents of " ~ child_node.unique_id) }}
              {% set parent_node_names = child_node.refs %}
              {% for parent_node_name in parent_node_names %}

                {% if parent_node_name[0] in parent_model_list %}
                  {# why [0]?  - dbt structures refs as a list of lists, with the inner list only containing one element #}
                  {{ dbt_utils.log_info("  Skipping duplicate parent model: " ~ parent_node_name[0]) }}
                {% else %}
                  {% do parent_model_list.append(parent_node_name[0]) %}
                  {{ dbt_utils.log_info("  Added to list: " ~ parent_node_name[0]) }}
                {% endif %}
              {% endfor %}
            {% endfor %}
          {% endif %}
        {% endif %}
      {% endfor %}

      {% if parent_model_list|length > 0 %}
        {{ dbt_utils.log_info("Begin looping through parent models") }}
        {{ reset_dev_for_list_of_models(parent_model_list, metadata_only) }}
      {% endif %}

    {% endif %}

  {% else %}
    {{ dbt_utils.log_info("Negatory: your current target is " ~ target.name ~ ". This macro only works for a dev target.", info=True) }}
  {% endif %}

{% endmacro %}


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
