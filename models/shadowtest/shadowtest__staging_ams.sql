
{% set db = "ZENALYTICS"-%}
{% set schema = "AMS"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SUBSCRIPTIONS_V2_POST_MIGRATION"),
    b_relation=ref('stg_ams__subscriptions_v2_post_migration'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SUBSCRIPTIONS_V2_PRE_MIGRATION"),
    b_relation=ref('stg_ams__subscriptions_v2_pre_migration'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SUBSCRIPTIONS_V2_SNAPSHOT"),
    b_relation=ref('stg_ams__subscriptions_v2_snapshot'),
    exclude_columns=["ASOF_DATE"]
) }}
