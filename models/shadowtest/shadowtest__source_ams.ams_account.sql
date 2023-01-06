
{% set db = "ZENPROD"-%}
{% set schema = "AMS_ACCOUNTS"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ACCOUNT"),
    b_relation=ref('src_ams_account__account'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ACCOUNT_PAYMENT_INFOS"),
    b_relation=ref('src_ams_account__account_payment_infos'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_BILLING_API_REQUESTS"),
    b_relation=ref('src_ams_account__ads_billing_api_requests'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_BILLING_LOG"),
    b_relation=ref('src_ams_account__ads_billing_log'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_BILLING_LOG_CREDIT_APPLIED_CENTS"),
    b_relation=ref('src_ams_account__ads_billing_log_credit_applied_cents'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_BILLING_LOG_SPEND"),
    b_relation=ref('src_ams_account__ads_billing_log_spend'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_BILLING_LOG_STATUS"),
    b_relation=ref('src_ams_account__ads_billing_log_status'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_BILLING_LOG_VALUES"),
    b_relation=ref('src_ams_account__ads_billing_log_values'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ADS_CAMPAIGN_DAILY_SPEND"),
    b_relation=ref('src_ams_account__ads_campaign_daily_spend'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ASSET"),
    b_relation=ref('src_ams_account__asset'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="CAMPAIGN"),
    b_relation=ref('src_ams_account__campaign'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="CHARGE"),
    b_relation=ref('src_ams_account__charge'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="CHARGE_LOG"),
    b_relation=ref('src_ams_account__charge_log'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ORDERS"),
    b_relation=ref('src_ams_account__orders'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ORDER_ITEM"),
    b_relation=ref('src_ams_account__order_item'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PAYMENT_INFO"),
    b_relation=ref('src_ams_account__payment_info'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="QUOTE_LINE_ITEM_INFO"),
    b_relation=ref('src_ams_account__quote_line_item_info'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="RECURLY_PROVIDER"),
    b_relation=ref('src_ams_account__recurly_provider'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="RECURLY_SUBSCRIPTION"),
    b_relation=ref('src_ams_account__recurly_subscription'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="RECURLY_SUBSCRIPTION_SNAPSHOT"),
    b_relation=ref('src_ams_account__recurly_subscription_snapshot'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SALESFORCE_ACCOUNT"),
    b_relation=ref('src_ams_account__salesforce_account'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SALESFORCE_ASSET"),
    b_relation=ref('src_ams_account__salesforce_asset'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SALESFORCE_ORDER"),
    b_relation=ref('src_ams_account__salesforce_order'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SALESFORCE_ORDER_ITEM"),
    b_relation=ref('src_ams_account__salesforce_order_item'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SALESFORCE_QUOTE"),
    b_relation=ref('src_ams_account__salesforce_quote'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SALESFORCE_QUOTE_LINE_ITEM"),
    b_relation=ref('src_ams_account__salesforce_quote_line_item'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SUBSCRIPTION"),
    b_relation=ref('src_ams_account__subscription'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SUBSCRIPTION_LOG"),
    b_relation=ref('src_ams_account__subscription_log'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SUBSCRIPTION_SNAPSHOT"),
    b_relation=ref('src_ams_account__subscription_snapshot'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="VIEW_ADS_BILLING_LOG"),
    b_relation=ref('src_ams_account__view_ads_billing_log'),
    exclude_columns=["ASOF_DATE"]
) }}
