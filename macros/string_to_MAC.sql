{% macro string_to_MAC(column) %}
    CASE
        WHEN {{ column }} IS NULL THEN '00:00:00:00:00:00'
        WHEN LENGTH({{ column }}) = 17 THEN {{ column }}
        ELSE left(regexp_replace({{ column }},'(.{1,2})','\\1:'),17)
    END
{% endmacro %}
