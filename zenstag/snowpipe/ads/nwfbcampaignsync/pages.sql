-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.PAGES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.PAGES as
          select
            $1 as page_id,
            $2 as name,
            $3 as street,
            $4 as city,
            $5 as state,
            $6 as zip,
            $7 as country,
            $8::float as latitude,
            $9::float as longitude,
            $10::boolean as is_always_open,
            $11::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/pages_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.PAGES_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.PAGES_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.PAGES_TASK resume;
