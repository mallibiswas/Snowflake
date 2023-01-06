
{% set db = "ZENPROD"-%}
{% set schema = "PRESENCE"-%}

{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database='ZENALYTICS', schema='ADS', identifier="AUDIENCE_VISIT_AGG"),
    b_relation=ref('stg_presence__audience_visit_agg'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="ENRICHED_SIGHTINGS"),
    b_relation=ref('stg_presence__enriched_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database='ZENALYTICS', schema=schema, identifier="FINISHED_SIGHTINGS"),
    b_relation=ref('stg_presence__finished_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="LOCATION_BLIPS"),
    b_relation=ref('stg_presence__location_blips'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="PORTAL_BLIPS"),
    b_relation=ref('stg_presence__portal_blips'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database='ZENALYTICS', schema=schema, identifier="RECLASSIFIED_FINISHED_SIGHTINGS_VW"),
    b_relation=ref('stg_presence__reclassified_finished_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database='ZENALYTICS', schema=schema, identifier="RECLASSIFIED_PRESENCE_SAMPLING_STATS"),
    b_relation=ref('stg_presence__reclassified_sampling_stats'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database='ZENALYTICS', schema=schema, identifier="RECLASSIFIED_PRESENCE_SAMPLING_STATS_DEMOGRAPHICS"),
    b_relation=ref('stg_presence__reclassified_sampling_stats_demographics'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database='ZENALYTICS', schema=schema, identifier="PRESENCE_SAMPLING_STATS"),
    b_relation=ref('stg_presence__sampling_stats'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="WIFI_CONSENTED_SIGHTINGS"),
    b_relation=ref('stg_presence__wifi_consented_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="WIFI_ENRICHED_SIGHTINGS"),
    b_relation=ref('stg_presence__wifi_enriched_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}
{{ audit_helper.compare_relations(
    a_relation=adapter.get_relation(database=db, schema=schema, identifier="WIFI_FINISHED_SIGHTINGS"),
    b_relation=ref('stg_presence__wifi_finished_sightings'),
    exclude_columns=["ASOF_DATE"]
) }}