-------------------------------------------------------------------
----------------- PARTNER_ACCOUNT table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.PARTNER_ACCOUNT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.PARTNER_ACCOUNT as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as name,
            $5 as salesforce_id,
            $6 as partner_type,
            $7::number as billing_account_id,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/partneraccount.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.PARTNER_ACCOUNT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.PARTNER_ACCOUNT_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.PARTNER_ACCOUNT_TASK resume;