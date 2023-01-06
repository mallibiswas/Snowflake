
{% set db = "ZENPROD"-%}
{% set schema = "AMS_PRODUCTLICENSER"-%}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="BUSINESS_ENTITY"),
    b_relation=ref('src_ams_productlicenser__business_entity'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="FEATURE"),
    b_relation=ref('src_ams_productlicenser__feature'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LICENSE"),
    b_relation=ref('src_ams_productlicenser__license'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LICENSE_ASSIGNMENT"),
    b_relation=ref('src_ams_productlicenser__license_assignment'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PACKAGE"),
    b_relation=ref('src_ams_productlicenser__package'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PACKAGE_FEATURE_LINK"),
    b_relation=ref('src_ams_productlicenser__package_feature_link'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PACKAGE_QUOTA_LINK"),
    b_relation=ref('src_ams_productlicenser__package_quota_link'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PRODUCT"),
    b_relation=ref('src_ams_productlicenser__product'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="QUOTA"),
    b_relation=ref('src_ams_productlicenser__quota'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="QUOTA_TYPE"),
    b_relation=ref('src_ams_productlicenser__quota_type'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="UNLIMITED_QUOTA_ACCOUNTS"),
    b_relation=ref('src_ams_productlicenser__unlimited_quota_accounts'),
    exclude_columns=["ASOF_DATE"]
) }}
