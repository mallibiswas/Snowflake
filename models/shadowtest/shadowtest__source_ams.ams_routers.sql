
{% set db = "ZENPROD"-%}
{% set schema = "AMS_ROUTERS"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LOGICAL_ROUTER"),
    b_relation=ref('src_ams_routers__logical_router'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LOGICAL_ROUTER_ASSIGNMENT"),
    b_relation=ref('src_ams_routers__logical_router_assignment'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="NETWORK_ASSIGNMENT"),
    b_relation=ref('src_ams_routers__network_assignment'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ROUTER"),
    b_relation=ref('src_ams_routers__router'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ROUTER_ASSIGNMENT"),
    b_relation=ref('src_ams_routers__router_assignment'),
    exclude_columns=["ASOF_DATE"]
) }}
