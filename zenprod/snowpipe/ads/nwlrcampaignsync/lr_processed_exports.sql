-- Procedure to complely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.ADS.LR_PROCESSED_EXPORTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.ADS.LR_PROCESSED_EXPORTS as
          select
            $1 as message_id,
            $2 as url,
            $3::timestamp as processed,
            current_timestamp() as asof_date
          FROM @ZENPROD.ADS.ARCHIVER_ADS_S3_STAGE/nwlrcampaignsync/${FILE_DATE}/processed_exports.csv;`
     }).execute();
$$;

-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.ADS.LR_PROCESSED_EXPORTS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.ADS.LR_PROCESSED_EXPORTS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.ADS.LR_PROCESSED_EXPORTS_TASK resume;
