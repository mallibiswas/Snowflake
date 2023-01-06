-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.POS.VENDOR_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.POS.VENDOR as	
          select
            $1::string as id,
            $2::string as name,
            $3::string as region,
            $4::string as host,
            $5::boolean as enabled,
            $6::string as webhook_auth,
            $7::string as client_id,
            $8::string as client_secret
          FROM @ZENPROD.POS.ARCHIVER_POS_S3_STAGE/vendor/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENPROD.POS.VENDOR_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 * * * * UTC'
as 
    CALL ZENPROD.POS.VENDOR_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.POS.VENDOR_TASK resume;
