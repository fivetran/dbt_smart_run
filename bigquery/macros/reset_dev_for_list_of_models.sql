{% macro reset_dev_for_list_of_models(model_list, metadata_only=false) %}
  {#- This macro takes in a list of model names and rebuilds these models within your current development environment via select * from prod.
    To run it:
      $ dbt run-operation reset_dev_for_list_of_models --args "{'model_list': ['model_a', 'model_b']}"
  -#}
  
  {% if is_valid_model_list(model_list) %}
    {{ dbt_utils.log_info('Model list provided is valid. Dev environment for all models provided will be reset.')}}
    {{ dbt_utils.log_info('--- Model(s) to be copied are: ' ~ model_list)}}
  
    {% for model_name in model_list %}
      {#
        {{ dbt_utils.log_info("Fetching context for " ~ model_name) }}
      #}
      
      {% set node = (graph.nodes.values() | selectattr("name", "equalto", model_name) | list)[0] %}
      
        {#- Controlling for new models that don't exist in prod and therefore can't be copied.
            See https://discourse.getdbt.com/t/writing-packages-when-a-source-table-may-or-may-not-exist/1487 -#}
        {%- set prod_relation = adapter.get_relation(
          database=target.database,
          schema=node.config.schema,
          identifier=node.name) -%}

        {%- set relation_exists_in_prod = prod_relation is not none -%}

        {% if relation_exists_in_prod %}
          {% if node.config.materialized == 'view' %}
            {{ create_or_replace_prod_to_target(node.name, node.config.schema, node.config.materialized, false) }}
          {% else %}
            {{ copy_prod_to_target(node.name, node.config.schema, metadata_only) }}
          {% endif %}
        {% else %}
          {{ dbt_utils.log_info("Skipping " ~ model_name ~ " because it does not yet exist in prod") }}
        {% endif %}
      
    {% endfor %}

  {% else %}
    {{ dbt_utils.log_info('Model list provided is not valid. No models will be copied.')}}
  {% endif %}

{% endmacro %}