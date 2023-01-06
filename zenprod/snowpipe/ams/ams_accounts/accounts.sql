-------------------------------------------------------------------
----------------- account table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS_ACCOUNTS.ACCOUNT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS_ACCOUNTS.ACCOUNT as
          select
            $1 as id,
            $2 as payment_info_id,
            $3 as salesforce_account,
            $4 as account_type,
            $5::boolean as active,
            $6::timestamp as created,
            $7::timestamp as updated,
            $8 as account_owner,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS_ACCOUNTS.AMS_ACCOUNTS_S3_STAGE/${FILE_DATE}/account.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS_ACCOUNTS.ACCOUNT_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS_ACCOUNTS.ACCOUNT_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS_ACCOUNTS.ACCOUNT_TASK resume;