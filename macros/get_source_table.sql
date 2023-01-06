{% macro seed_or_ref(real_table, seed_table) %}

{% if target.name in var("test_env") %}
    {{ seed_table }}
{% else %}
    {{ real_table }}
{% endif %}

{% endmacro %}
