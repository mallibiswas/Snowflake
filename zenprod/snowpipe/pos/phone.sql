-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.POS.PHONE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.POS.PHONE as	
          select
            $1::string as customer_id,
            $2::string as phone_number
          FROM @ZENPROD.POS.ARCHIVER_POS_S3_STAGE/phone/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENPROD.POS.PHONE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 * * * * UTC'
as 
    CALL ZENPROD.POS.PHONE_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.POS.PHONE_TASK resume;
