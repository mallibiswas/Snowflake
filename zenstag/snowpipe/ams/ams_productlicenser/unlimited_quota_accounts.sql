-------------------------------------------------------------------
----------------- UNLIMITED_QUOTA_ACCOUNTS table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_PRODUCTLICENSER.UNLIMITED_QUOTA_ACCOUNTS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_PRODUCTLICENSER.UNLIMITED_QUOTA_ACCOUNTS as
          select
            $1 as id,
            $2 as account_id,
            $4::timestamp as created,
            current_timestamp() as of_date
          FROM @ZENSTAG.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/unlimited_quota_accounts.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_PRODUCTLICENSER.UNLIMITED_QUOTA_ACCOUNTS_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_PRODUCTLICENSER.UNLIMITED_QUOTA_ACCOUNTS_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_PRODUCTLICENSER.UNLIMITED_QUOTA_ACCOUNTS_TASK resume;
