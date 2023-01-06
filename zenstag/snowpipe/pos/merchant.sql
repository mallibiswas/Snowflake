-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.POS.MERCHANT_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.POS.MERCHANT as	
          select
            $1::string as id,
            $2::string as vendor_id,
            $3::string as foreign_id,
            $4::string as zenreach_bid,
            $5::string as name,
            $6::boolean as enabled,
            $7::string as address1,
            $8::string as address2,
            $9::string as address3,
            $10::string as city,
            $11::string as state,
            $12::string as country,
            $13::string as zipcode,
            $14::timestamp as foreign_created_time,
            $15::timestamp as foreign_updated_time,
            $16::timestamp as zenreach_created_time,
            $17::timestamp as zenreach_updated_time
          FROM @ZENSTAG.POS.ARCHIVER_POS_S3_STAGE/merchant/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENSTAG.POS.MERCHANT_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.POS.MERCHANT_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.POS.MERCHANT_TASK resume;
