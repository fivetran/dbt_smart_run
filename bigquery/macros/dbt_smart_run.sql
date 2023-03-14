{% macro generate_copy_and_run_dictionary(targets, models_updated) %}
  {# This macro is a helper macro, designed to work in pair with dbt_smart_run. It takes in a list of target model 
      names and a list of model names that have been updated. The output is a mapping dictonary which outlines which 
      models need to be ran and with need to be copied in order for your target model to be refreshed in a way that 
      takes into account all changes made to the updated models.
    
    Code implementation: use a breadth-first search used to traverse the lineage graph, starting with the target model(s). 
    Jinja makes this a bit cumbersome to write, but the basics of what is going on is:

    for each model in models_to_check:
      if model has any upstream models in our change list OR is in change_list OR is target:
        add model to run_list
        add parent models to models_to_check
      else
        add to copy_list
  #}

  {% set models_updated = models_updated %}
  {% set run_list = [] %}
  {% set copy_list = [] %}
  {% set models_to_check = [] %}
  {% set checked_models = [] %}

  {% for target in targets %}
    {% do models_to_check.append(target) %}
  {% endfor %}

  {% for _ in range(1, graph.nodes|count) %} {# necessary to run this way due to jinjas lack of while loop  #}

    {# final step to execute when there are no additional models to check #}
    {% if (models_to_check | length == 0) %}      
    
      {{ return({"run_list": run_list, "copy_list": copy_list}) }}
    
    {% endif %} 

    {% set model_to_check_node_list = graph.nodes.values() | selectattr("name", "==", models_to_check[0]) | list %}
      
    {% set model_to_check_node = model_to_check_node_list[0] %}

    {% do models_to_check.pop(0) %}

    {% if model_to_check_node.name not in checked_models %}
          
      {% set upstream_models = get_all_upstream_reference_models(model_to_check_node.name) %}
      {% set add_to_run_list = [] %} {# necessary to run this way due to scope of jinja variables, ideally this would simply be a boolean flag. #}
      
      {# if the model is in target list, run that model #}
      {% if model_to_check_node.name in targets %}
        {% do add_to_run_list.append(1) %} 
      {% endif %}

      {# this checks if the model itself has been changed. If so, we need to run that model #}
      {% if model_to_check_node.name in models_updated %}
        {% do add_to_run_list.append(1) %} 
      {% endif %}

      {# this checks if the model has any upstream models in our models_updated list. If so, we need to run that model #}
      {% for model in models_updated %}
        {% if model in upstream_models %}
          {% do add_to_run_list.append(1) %}
        {% endif %}
      {% endfor %}

      {% if add_to_run_list %}
        {% do run_list.append(model_to_check_node.name) %}
        
        {# we need to add models parents to models_to_check so that they are evaluated in future loops #}
        {% for parent_model in model_to_check_node.refs %}
          {% if parent_model[0] not in models_to_check and parent_model[0] not in checked_models %}
            {%- do models_to_check.append(parent_model[0]) -%}
          {% endif %}
        {% endfor %}

      {% else %}
        {% do copy_list.append(model_to_check_node.name) %}
      {% endif %}
      
      {% do checked_models.append(model_to_check_node.name) %}
    
    {% endif %}

  {% endfor %}

{% endmacro %}

{% macro dbt_smart_run(targets, models_updated, skip_copy=false) %}
  {# This macro takes in a list of target model names and a list of model names that have been updated. It runs through
      a series of data quality checks and executes the command necessary to copy models necessary and generates the run
      command which the end user will need to run independently.
  #}

  {% if is_valid_model_list(targets) %}
    {{ dbt_utils.log_info('Data quality confirmation: Target model(s) provided are valid.')}}

    {% set smart_run_mapping = generate_copy_and_run_dictionary(targets, models_updated) %}
    
    {% if skip_copy == false %}
      {% if is_valid_model_list(models_updated) %}
        {{ dbt_utils.log_info('Data quality confirmation: Updated model(s) provided are valid.')}}

        {% if smart_run_mapping['copy_list'] %}
          {% set models_to_copy = smart_run_mapping['copy_list'] %}
          {% do reset_dev_for_list_of_models(models_to_copy)%}
        {% endif %}
      
      {% else %}
        {{ dbt_utils.log_info('Data quality rejection: Modified model(s) are not valid. This list was automatically generated based off of your commit history')}}
        {{ dbt_utils.log_info('--- Models that have been logged as changed are: ' ~ models_updated)}}
      {% endif %}
    {% endif %}

    {% if smart_run_mapping['run_list'] %}
      {{ dbt_utils.log_info('*** dbt_smart_run will now execute the following command: ***')}}
      {# this line of code is pulled and executed in dbt_smart_run.py #}
      {{ dbt_utils.log_info('$ dbt run -m '~ smart_run_mapping['run_list']|join(" "))}}

    {% endif %}

  {% else %}
    {{ dbt_utils.log_info('Data quality rejection: Target model(s) provided are not valid. Please correct model names provided and re-run operation.')}}
    {{ dbt_utils.log_info('--- Target model(s) provided are: ' ~ targets)}}
  {% endif %}

{% endmacro %}