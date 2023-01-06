-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.POS.ADDRESS_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.POS.ADDRESS as	
          select
            $1::string as id,
            $2::string as customer_id,
            $3::string as address1,
            $4::string as address2,
            $5::string as address3,
            $6::string as city,
            $7::string as state,
            $8::string as country,
            $9::string as zipcode
          FROM @ZENPROD.POS.ARCHIVER_POS_S3_STAGE/address/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENPROD.POS.ADDRESS_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 * * * * UTC'
as 
    CALL ZENPROD.POS.ADDRESS_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.POS.ADDRESS_TASK resume;
