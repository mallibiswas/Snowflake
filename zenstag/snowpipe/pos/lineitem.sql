-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENSTAG.POS.LINEITEM_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.POS.LINEITEM as	
          select
            $1::string as id,
            $2::string as purchase_id,
            $3::string as foreign_id,
            $4::string as inventory_id,
            $5::string as line_item_name,
            $6::integer as quantity,
            $7::string as unit_name,
            $8::integer as unit_price,
            $9::integer as total_price
          FROM @ZENSTAG.POS.ARCHIVER_POS_S3_STAGE/lineitem/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENSTAG.POS.LINEITEM_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSTAG.POS.LINEITEM_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.POS.LINEITEM_TASK resume;
