-- ============================================================
-- generate_schema_name.sql
-- Purpose: Override dbt's default schema naming behaviour
-- Without this: dbt creates RAW_STAGING instead of STAGING
-- With this: dbt creates exactly STAGING and MARTS as defined
-- ============================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}