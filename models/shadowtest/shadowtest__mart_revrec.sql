
{% set db = "ZENALYTICS"-%}
{% set schema = "REVREC"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_BUDGET_MONTHLY_FACT"),
    b_relation=ref('mart_revrec__ads_budget_monthly_fact'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_SPEND_MONTHLY_FACT"),
    b_relation=ref('mart_revrec__ads_spend_monthly_fact'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="D_ACCOUNT_SERVICE_DATES"),
    b_relation=ref('mart_revrec__d_account_service_dates'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="D_DATE"),
    b_relation=ref('mart_revrec__d_date'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="D_INVOICE_ADJUSTMENTS"),
    b_relation=ref('mart_revrec__d_invoice_adjustments'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="D_SERVICE_DATES_ADJUSTMENTS"),
    b_relation=ref('mart_revrec__d_service_dates_adjustments'),
    exclude_columns=["ASOF_DATE"]
) }}
