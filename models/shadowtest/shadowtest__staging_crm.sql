
{% set db = "ZENALYTICS"-%}
{% set schema = "CRM"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema='AUDIENCES', identifier="USER_PROFILE"),
    b_relation=ref('stg_crm__user_profile'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ANALYTICS_CUSTOMER"),
    b_relation=ref('stg_crm__analytics_customer'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="BUSINESSPROFILE_HIERARCHY"),
    b_relation=ref('stg_crm__businessprofile_hierarchy'),
    exclude_columns=["ASOF_DATE"]
) }}
