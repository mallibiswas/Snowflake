-------------------------------------------------------------------
----------------- SALESFORCE_INVOICE_REFERENCE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.SALESFORCE_INVOICE_REFERENCE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.SALESFORCE_INVOICE_REFERENCE as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as location_id,
            $5::number as invoice_id,
            $6 as salesforce_id,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/salesforceinvoicereference.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.SALESFORCE_INVOICE_REFERENCE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.SALESFORCE_INVOICE_REFERENCE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.SALESFORCE_INVOICE_REFERENCE_TASK resume;