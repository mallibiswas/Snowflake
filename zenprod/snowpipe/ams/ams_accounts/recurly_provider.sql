-------------------------------------------------------------------
----------------- RECURLY_PROVIDER table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.RECURLY_PROVIDER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.RECURLY_PROVIDER as
          select
            $1 as id,
            $2 as recurly_id,
            $3 as name,
            $4 as email,
            $5 as url,
            $6::timestamp as created,
            $7::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/recurly_provider.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.RECURLY_PROVIDER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.RECURLY_PROVIDER_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.RECURLY_PROVIDER_TASK resume;