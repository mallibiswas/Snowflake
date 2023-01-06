
{% set db = "ZENPROD"-%}
{% set schema = "CRM"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ANALYTICS_AGGREGATESTATS"),
    b_relation=ref('src_crm__analytics_aggregatestats'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ANALYTICS_COLLECTIONSTATS"),
    b_relation=ref('src_crm__analytics_collectionstats'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ANALYTICS_CUSTOMER"),
    b_relation=ref('src_crm__analytics_customer'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ANALYTICS_MESSAGELOGSTATS"),
    b_relation=ref('src_crm__analytics_messagelogstats'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ANALYTICS_TRAFFIC"),
    b_relation=ref('src_crm__analytics_traffic'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="AUTH_USER"),
    b_relation=ref('src_crm__auth_user'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="GDPR_CONTACT_BLACKLIST"),
    b_relation=ref('src_crm__gdpr_contact_blacklist'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MOBILE_DEVICEREGISTRATIONLOG"),
    b_relation=ref('src_crm__mobile_deviceregistrationlog'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MOBILE_ENGAGEMENTLOG"),
    b_relation=ref('src_crm__mobile_engagementlog'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MOBILE_NOTIFICATIONDEVICE"),
    b_relation=ref('src_crm__mobile_notificationdevice'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MOBILE_NOTIFICATIONDEVICELOGIN"),
    b_relation=ref('src_crm__mobile_notificationdevicelogin'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MODELS_ACCOUNTTUTORIALCOMPLETION"),
    b_relation=ref('src_crm__models_accounttutorialcompletion'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MODELS_BILLINGANDSUBSCRIPTIONPREFS"),
    b_relation=ref('src_crm__models_billingandsubscriptionprefs'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MODELS_BUSINESSBRANDING"),
    b_relation=ref('src_crm__models_businessbranding'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MODELS_MESSAGESTATS"),
    b_relation=ref('src_crm__models_messagestats'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MODELS_ONBOARDINGWIZARDPAGECOMPLETIONS"),
    b_relation=ref('src_crm__models_onboardingwizardpagecompletions'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="MODELS_USERTUTORIALCOMPLETION"),
    b_relation=ref('src_crm__models_usertutorialcompletion'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_ACCESSDEVICE"),
    b_relation=ref('src_crm__portal_accessdevice'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_ACCESSDEVICEOWNERSHIP"),
    b_relation=ref('src_crm__portal_accessdeviceownership'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_BUSINESSOWNERSHIP"),
    b_relation=ref('src_crm__portal_businessownership'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_BUSINESSPROFILE"),
    b_relation=ref('src_crm__portal_businessprofile'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_BUSINESSRELATIONSHIP"),
    b_relation=ref('src_crm__portal_businessrelationship'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_PORTALTERMSPRIVACYCONSENT"),
    b_relation=ref('src_crm__portal_portaltermsprivacyconsent'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_PRODUCT"),
    b_relation=ref('src_crm__portal_product'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_PRODUCTVERSION"),
    b_relation=ref('src_crm__portal_productversion'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_ROUTER"),
    b_relation=ref('src_crm__portal_router'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_ROUTERTYPE"),
    b_relation=ref('src_crm__portal_routertype'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_TOSCONSENT"),
    b_relation=ref('src_crm__portal_tosconsent'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_USERPROFILE"),
    b_relation=ref('src_crm__portal_userprofile'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="REPMANAGEMENT_BUSINESSRATING"),
    b_relation=ref('src_crm__repmanagement_businessrating'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="REPMANAGEMENT_SETTINGS"),
    b_relation=ref('src_crm__repmanagement_settings'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_ACTIVITYLOG"),
    b_relation=ref('src_crm__smbsite_activitylog'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_BLACKLISTEDEMAIL"),
    b_relation=ref('src_crm__smbsite_blacklistedemail'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_CLICKEVENT"),
    b_relation=ref('src_crm__smbsite_clickevent'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_CLICKLOG"),
    b_relation=ref('src_crm__smbsite_clicklog'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_DEFAULTTRIGGER"),
    b_relation=ref('src_crm__smbsite_defaulttrigger'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_EMAILBLAST"),
    b_relation=ref('src_crm__smbsite_emailblast'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_EMAILIMPORT"),
    b_relation=ref('src_crm__smbsite_emailimport'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_EMAILTEMPLATE"),
    b_relation=ref('src_crm__smbsite_emailtemplate'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_EMAILWHITELIST"),
    b_relation=ref('src_crm__smbsite_emailwhitelist'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_GATEKEEPER"),
    b_relation=ref('src_crm__smbsite_gatekeeper'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_GATEKEEPERENTRY"),
    b_relation=ref('src_crm__smbsite_gatekeeperentry'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_MAC_TO_CONTACT"),
    b_relation=ref('src_crm__smbsite_mac_to_contact'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_MERCHANTNOTIFICATIONSETTING"),
    b_relation=ref('src_crm__smbsite_merchantnotificationsetting'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_MERGEDBLAST"),
    b_relation=ref('src_crm__smbsite_mergedblast'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_MESSAGE"),
    b_relation=ref('src_crm__smbsite_message'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_MOBILEAPPDOWNLOAD"),
    b_relation=ref('src_crm__smbsite_mobileappdownload'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_OFFER"),
    b_relation=ref('src_crm__smbsite_offer'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_OFFERCODE"),
    b_relation=ref('src_crm__smbsite_offercode'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_OFFERLOG"),
    b_relation=ref('src_crm__smbsite_offerlog'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_OFFERLOGDETAIL"),
    b_relation=ref('src_crm__smbsite_offerlogdetail'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_TARGET"),
    b_relation=ref('src_crm__smbsite_target'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="SMBSITE_TRIGGER"),
    b_relation=ref('src_crm__smbsite_trigger'),
    exclude_columns=["ASOF_DATE"]
) }}
