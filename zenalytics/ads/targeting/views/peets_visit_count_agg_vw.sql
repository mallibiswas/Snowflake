CREATE OR REPLACE VIEW ZENALYTICS.ADS.PEETS_VISIT_COUNT_AGG_VW 
COMMENT='Aggregate visits by (location_id, contact_info)' as 
SELECT location_id, contact_info, count(*) as visit_count
FROM ZENALYTICS.ADS.PEETS_ENRICHED_SIGHTINGS_VW
GROUP BY (location_id, contact_info)

