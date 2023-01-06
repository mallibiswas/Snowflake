SELECT DISTINCT PARENT_ID,
                PARENT_NAME,
                nvl(ZEN_TYPE, 'Unknown') AS INDUSTRY,
                BPH.BUSINESS_ID,
                BUSINESS_NAME
FROM {{ ref('stg_crm__businessprofile_hierarchy') }} bph
    LEFT JOIN {{ ref('stg_business_features__types') }} bft ON bph.business_id = bft.business_id
WHERE bph.business_id IN (SELECT business_id from {{ ref('mart_presence_metrics__sightings_report_summary') }})