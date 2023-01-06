-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.POS.PURCHASE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.POS.PURCHASE as	
          select
            $1::string as id,
            $2::string as merchant_id,
            $3::string as customer_id,
            $4::string as foreign_id,
            $5::integer as total,
            $6::string as status,
            $7::timestamp as foreign_created_time,
            $8::timestamp as foreign_updated_time,
            $9::timestamp as zenreach_created_time,
            $10::timestamp as zenreach_updated_time
          FROM @ZENSAND.POS.ARCHIVER_POS_S3_STAGE/purchase/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENSAND.POS.PURCHASE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.POS.PURCHASE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.POS.PURCHASE_TASK resume;
