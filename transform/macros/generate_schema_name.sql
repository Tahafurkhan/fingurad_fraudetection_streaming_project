{#
    Override dbt's default schema naming.

    The default `generate_schema_name` concatenates the target schema with any
    custom schema, so a model configured with `+schema: marts` against a target
    whose schema is already `marts` lands in `marts_marts`. That default exists
    to keep developers from colliding in a shared warehouse, which is a real
    concern -- but here environment separation comes from the catalog and from
    Asset Bundle targets, not from schema prefixes.

    This version uses the custom schema verbatim when one is set, and the
    target schema otherwise, so tables land exactly where the config says.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
