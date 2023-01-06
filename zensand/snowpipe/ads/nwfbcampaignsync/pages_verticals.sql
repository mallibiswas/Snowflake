-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.ADS.PAGES_VERTICALS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.ADS.PAGES_VERTICALS as
          select
            $1 as page_vertical_id,
            $2::integer as page_id,
            $3::integer as vertical_id,
            $4 as vertical_name,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENSAND.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/pages_verticals_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.ADS.PAGES_VERTICALS_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.ADS.PAGES_VERTICALS_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.ADS.PAGES_VERTICALS_TASK resume;
