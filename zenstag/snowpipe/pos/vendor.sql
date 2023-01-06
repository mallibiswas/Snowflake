-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.POS.VENDOR_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.POS.VENDOR as	
          select
            $1::string as id,
            $2::string as name,
            $3::string as region,
            $4::string as host,
            $5::boolean as enabled,
            $6::string as webhook_auth,
            $7::string as client_id,
            $8::string as client_secret
          FROM @ZENSTAG.POS.ARCHIVER_POS_S3_STAGE/vendor/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENSTAG.POS.VENDOR_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.POS.VENDOR_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.POS.VENDOR_TASK resume;
