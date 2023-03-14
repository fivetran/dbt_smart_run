{% macro is_valid_model_list(model_list = []) %}
  {#- This macro takes in a list of model names and checks to see if all of the models provided exist.
      If all models exist, macro will return true.
      If any model does not exist, error messages will be logged and macro will return false.
  -#}
  {% set has_invalid_model = [] %}

  {% for model_name in model_list %}
  
    {% set node_list = graph.nodes.values() | selectattr("name", "equalto", model_name) | list %}
  
    {% if node_list == [] %}
      {{ dbt_utils.log_info(model_name ~ ' is not a valid model.') }}
      {% do has_invalid_model.append(1) %}
    {% endif %}
  
  {% endfor %}

  {% if has_invalid_model %}
    {{ return(false)}}
  {% else %}
    {{ return(true)}}
  {% endif %}

{% endmacro %}