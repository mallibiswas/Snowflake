
{% set db = "ZENPROD"-%}
{% set schema = "PRIVACY"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PRIVACY_DELETES"),
    b_relation=ref('stg_privacy__privacy_deletes'),
    exclude_columns=["ASOF_DATE"]
) }}