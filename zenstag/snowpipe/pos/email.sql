-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.POS.EMAIL_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.POS.EMAIL as	
          select
            $1::string as customer_id,
            $2::string as email_address
          FROM @ZENSTAG.POS.ARCHIVER_POS_S3_STAGE/email/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENSTAG.POS.EMAIL_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.POS.EMAIL_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.POS.EMAIL_TASK resume;
