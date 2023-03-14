-- this macro lets you specify a target schema/dataset
-- if your profiles.yml 'target' is set to 'prod' it will write to the specified schema/dataset in the 'config' input above your model
-- if your profiles.yml 'target' is set to 'dev' (when you're using dbt locally) it will always write to your default dev schema/dataset

{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ generate_schema_name_for_env(custom_schema_name, node) }}
{%- endmacro %}