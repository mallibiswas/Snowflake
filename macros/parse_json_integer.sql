-- This handles differences between stored mongo json files.
--      Production returns: "11111111" or {"$numberLong":"11111111"}
--      Sandbox returns: {"$numberLong":"11111111"}

{% macro parse_json_integer(column) %}
{% if target.name == 'ZENPROD_DBT' %}IFNULL(TRY_TO_NUMBER({{ column }}::STRING), {{ column }}['$numberLong']::number){% else %}{{ column }}['$numberLong']::integer{% endif %}
{% endmacro %}