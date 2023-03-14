{% macro get_all_upstream_reference_models(model_name) %}
  {# This macro takes a name of a model, and returns a complete list of all upstream models. #}
  
  {% set upstream_models = [model_name] %} {# temporarily add model to upstream_model list #}

  {% if is_valid_model_list([model_name])%}
    {# Note on for loop below:
      - initially, upstream_models list is only set to the original model provided. 
      - As the code progresses, upstream_models is modified to include all refs of that model.
      - During subsequent loops, the refs that were added, will be accessed.
    #}
    {% for model_name in upstream_models %}    
      {% set node_list = graph.nodes.values() | selectattr("name", "==", model_name) | list %}
      {% set node = node_list[0] %}

      {% for ref in node.refs %}
        {% if ref[0] not in upstream_models %}
          {%- do upstream_models.append(ref[0]) -%}
        {% endif %}
      {% endfor %}
    {% endfor %}

    {{ return(upstream_models[1:]) }}

  {% else %}
    {{ dbt_utils.log_info("Data quality rejection: Model `" ~ model_name ~ "` not found.") }}
    {{ dbt_utils.log_info("--- Check your spelling and try again; if this model is from a package, try running `dbt deps`") }}
  {% endif %}

{% endmacro %}