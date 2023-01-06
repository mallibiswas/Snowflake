-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.PAGES_INSTAGRAM_ACCOUNTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.PAGES_INSTAGRAM_ACCOUNTS as
          select
            $1 as pages_instagram_accounts_id,
            $2::integer as page_id,
            $3::integer as instagram_id,
            $4 as instagram_name,
            $5::timestamp as last_synced,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwfbcampaignsync/${FILE_DATE}/pages_instagram_accounts_v3.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.PAGES_INSTAGRAM_ACCOUNTS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.PAGES_INSTAGRAM_ACCOUNTS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.PAGES_INSTAGRAM_ACCOUNTS_TASK resume;
