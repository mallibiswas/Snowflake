{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {%- if custom_alias_name is none and target.name == 'ZENPROD_DBT' and '__' in node.name -%}

        {{ node.name.split('__', 1)[1] | upper }}

    {%- elif custom_alias_name is none -%}

        {{ node.name | upper }}

    {%- else -%}

        {{ custom_alias_name | trim | upper }}

    {%- endif -%}

{%- endmacro %}