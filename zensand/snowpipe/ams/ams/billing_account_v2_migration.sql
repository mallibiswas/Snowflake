-------------------------------------------------------------------
----------------- BILLING_ACCOUNT_V2_MIGRATION table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.BILLING_ACCOUNT_V2_MIGRATION_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.BILLING_ACCOUNT_V2_MIGRATION as
          select
            $1 as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as billing_account_v1_id,
            $5::number as billing_account_v2_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/billing_account_v2_migration.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.BILLING_ACCOUNT_V2_MIGRATION_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.BILLING_ACCOUNT_V2_MIGRATION_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.BILLING_ACCOUNT_V2_MIGRATION_TASK resume;