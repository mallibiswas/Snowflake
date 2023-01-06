{% macro refresh_zenalytics_view(this) %}
    CREATE OR REPLACE VIEW {{ 'ZENALYTICS' if target.database == 'ZENPROD' else 'ZENDEV' }}.{{ this.schema }}.{{ this.name }} AS
    SELECT *
    FROM {{ this }}
{% endmacro %}