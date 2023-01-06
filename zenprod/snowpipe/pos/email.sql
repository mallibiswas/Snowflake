-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.POS.EMAIL_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.POS.EMAIL as	
          select
            $1::string as customer_id,
            $2::string as email_address
          FROM @ZENPROD.POS.ARCHIVER_POS_S3_STAGE/email/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENPROD.POS.EMAIL_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 * * * * UTC'
as 
    CALL ZENPROD.POS.EMAIL_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.POS.EMAIL_TASK resume;
