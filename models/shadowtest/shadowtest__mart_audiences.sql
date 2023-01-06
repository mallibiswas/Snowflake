
{% set db = "ZENALYTICS"-%}
{% set schema = "AUDIENCES"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AUDIENCE"),
    b_relation=ref('mart_audiences__audience'),
    exclude_columns=["ASOF_DATE"]
) }}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="USER_PROFILE"),
    b_relation=ref('mart_audiences__user_profile'),
    exclude_columns=["ASOF_DATE"]
) }}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="USER_SIGHTINGS"),
    b_relation=ref('mart_audiences__user_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}