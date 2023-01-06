-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENPROD.POS.INVENTORY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.POS.INVENTORY as	
          select
            $1::string as id,
            $2::string as merchant_id,
            $3::string as foreign_id,
            $4::string as name,
            $5::string as sku,
            $6::string as upc,
            $7::integer as price,
            $8::string as currency,
            $9::string as categories,
            $10::string as labels,
            $11::boolean as deleted,
            $12::timestamp as foreign_created_time,
            $13::timestamp as foreign_updated_time,
            $14::timestamp as zenreach_created_time,
            $15::timestamp as zenreach_updated_time
          FROM @ZENPROD.POS.ARCHIVER_POS_S3_STAGE/inventory/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENPROD.POS.INVENTORY_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 * * * * UTC'
as 
    CALL ZENPROD.POS.INVENTORY_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.POS.INVENTORY_TASK resume;
