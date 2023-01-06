-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.AUDIENCE_SEGMENTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.AUDIENCE_SEGMENTS as
          WITH merged_sightings_ as (
            SELECT lower(location_id) as location_id, contact_info as email, start_time, end_time
            FROM ZENPROD.PRESENCE.WIFI_CONSENTED_SIGHTINGS
            WHERE classification = 'Classification_WALKIN' and contact_method = 'CONTACT_METHOD_EMAIL' AND is_employee = 'false' AND contact_info IS NOT NULL
            AND end_time > TO_TIMESTAMP(1571097600000 / 1000)
              
            UNION ALL

            SELECT lower(business_id) as location_id, contact_info as email, TO_TIMESTAMP(start_time / 1000) as start_time, TO_TIMESTAMP(end_time / 1000) as end_time
            FROM ZENPROD.PRESENCE.ENRICHED_SIGHTINGS
            WHERE status = 'FINISHED' and contact_method = 'email' AND contact_info IS NOT NULL AND is_walk_in = 'true'
            AND end_time <= 1571097600000
          )
          SELECT location_id, email, count(*) as visit_count, min(start_time) as first_seen, max(end_time) as last_seen
          FROM merged_sightings_
          GROUP BY (location_id, email);`
     }).execute();
$$;

-- Create task to call the procedure (every 4 hours)
create task ZENPROD.ADS.AUDIENCE_SEGMENTS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 */4 * * * UTC'
as 
    CALL ZENPROD.ADS.AUDIENCE_SEGMENTS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.AUDIENCE_SEGMENTS_TASK resume;
