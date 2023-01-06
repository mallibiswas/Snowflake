-- This handles differences between stored mongo json files.
--      Production returns: "2017-03-16T06:39:14.489Z"
--      Sandbox returns: {"$date":"2017-03-16T06:39:14.489Z"}

{% macro parse_json_date(column) %}
{% if target.name == 'ZENPROD_DBT' %}IFNULL(TRY_TO_TIMESTAMP({{ column }}::STRING), {{ column }}['$date']::timestamp){% else %}{{ column }}['$date']::timestamp{% endif %}
{% endmacro %}