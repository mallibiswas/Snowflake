CREATE OR REPLACE VIEW ZENALYTICS.ADS.AUDIENCE_VISIT_AGG_VW comment='Aggregate enriched visits (network and regular) by (location_id, contact_info)' as
SELECT business_id as location_id, contact_info as email, count(*) as visit_count, min(TO_TIMESTAMP(start_time / 1000)) as first_seen, max(TO_TIMESTAMP(end_time / 1000)) as last_seen
FROM ZENALYTICS.PRESENCE.ENRICHED_SIGHTINGS
WHERE status = 'FINISHED' and contact_method = 'email' AND contact_info IS NOT NULL
GROUP BY (business_id, contact_info)
