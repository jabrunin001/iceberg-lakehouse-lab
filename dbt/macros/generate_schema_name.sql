{# Use the model's +schema as the literal namespace (silver, gold) instead of dbt's
   default <target_schema>_<custom_schema> concatenation (silver_silver, silver_gold).
   This keeps tables in clean demo.silver.* / demo.gold.* Iceberg namespaces. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
