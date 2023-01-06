-------------------------------------------------------------------
----------------- SALESFORCE_TRANSACTION_REFERENCE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.SALESFORCE_TRANSACTION_REFERENCE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.SALESFORCE_TRANSACTION_REFERENCE as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as location_id,
            $5::number as transaction_id,
            $6 as salesforce_id,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/salesforcetransactionreference.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.SALESFORCE_TRANSACTION_REFERENCE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.SALESFORCE_TRANSACTION_REFERENCE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.SALESFORCE_TRANSACTION_REFERENCE_TASK resume;