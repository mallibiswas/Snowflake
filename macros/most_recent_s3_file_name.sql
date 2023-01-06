-- This macro retrieves the most recent file in a specific stage according to the supplied pattern
-- The recency is based on alphabetically ordering the files.

{% macro most_recent_s3_file_name(stage_schema, stage_name, pattern=".*") %}

-- use the right stage_catalog
{% if database == 'ZENSAND_DBT' %}
    {% set stage_catalog = 'ZENSAND' %}
{% else %}
    {% set stage_catalog = database %}
{% endif %}

{% set query %}
    -- Because we can't select the files in any efficient matter but list, we first do that.
    LIST @{{ stage_catalog }}.{{ stage_schema }}.{{ stage_name }} PATTERN = '{{ pattern }}';
    -- Using the last_query_id we get the results from the previous executed LIST statement
    with staged_files as (
        SELECT *
        FROM table(result_scan(last_query_id()))
    ), stage_info as (
        SELECT
               CASE RIGHT(STAGE_URL, 1) = '/'
                   WHEN TRUE THEN STAGE_URL
                   ELSE STAGE_URL || '/'
               END AS STAGE_URL
        FROM {{ stage_catalog }}.INFORMATION_SCHEMA.STAGES
        WHERE STAGE_CATALOG = '{{ stage_catalog }}'
            AND STAGE_SCHEMA = '{{ stage_schema }}'
            AND STAGE_NAME = '{{ stage_name }}'
    )
    select REPLACE(MAX(staged_files.$1), stage_info.STAGE_URL, '@{{ stage_catalog }}.{{ stage_schema }}.{{ stage_name }}/')
    from staged_files
        LEFT JOIN stage_info ON TRUE
    GROUP BY stage_info.STAGE_URL;
{% endset %}

{% if execute %}
  {{log(query)}}
  {% set result = run_query(query) %}
  {% if result.rows|length == 0 %}
    {{ exceptions.raise_compiler_error('No file available for Stage ' ~ stage_schema ~ '.' ~ stage_name ~ ' for pattern: "' ~ pattern ~'"') }}
    {{ return('/* Stage Not Found */') }}
  {% else %}
    {{ return(result[0][0]) }}
  {% endif %}
{% else %}
  {{ return("TABLE(SELECT 1 as a)") }}
{% endif %}

                 
{% endmacro %} 


