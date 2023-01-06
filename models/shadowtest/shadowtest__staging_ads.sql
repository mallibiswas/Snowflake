
{% set db = "ZENPROD"-%}
{% set schema = "ADS"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LIVERAMP_AD_INSIGHT_METRICS"),
    b_relation=ref('stg_ads__liveramp_ad_insight_metrics'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AD_INSIGHT_METRICS"),
    b_relation=ref('stg_ads__ad_insight_metrics'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="FACEBOOK_AD_INSIGHT_METRICS"),
    b_relation=ref('stg_ads__facebook_ad_insight_metrics'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AUDIENCE_SEGMENTS"),
    b_relation=ref('stg_ads__audience_segments'),
    exclude_columns=["ASOF_DATE"]
) }}