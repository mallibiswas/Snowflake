-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.ADS.LR_ADVERTISERS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.ADS.LR_ADVERTISERS as
          select
            $1 as id,
            $2 as advertiser_id,
            $3 as name,
            $4::timestamp as created,
            current_timestamp() as asof_date
          FROM @ZENSTAG.ADS.ARCHIVER_ADS_S3_STAGE/nwlrcampaignsync/${FILE_DATE}/advertisers.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.ADS.LR_ADVERTISERS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.ADS.LR_ADVERTISERS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.ADS.LR_ADVERTISERS_TASK resume;
