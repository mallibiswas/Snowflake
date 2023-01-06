-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.POS.CUSTOMER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.POS.CUSTOMER as	
          select	
              $1::string as id,
              $2::string as merchant_id,
              $3::string as foreign_id,
              $4::string as name,
              $5::string as primary_email,
              $6::integer as birthday_day,
              $7::integer as birthday_month,
              $8::integer as birthday_year,
              $9::integer as anniversary_day,
              $10::integer as anniversary_month,
              $11::integer as anniversary_year,
              $12::string as age,
              $13::string as gender,
              $14::boolean as marketing_allowed,
              $15::timestamp as foreign_created_time,
              $16::timestamp as foreign_updated_time,
              $17::timestamp as zenreach_created_time,
              $18::timestamp as zenreach_updated_time
          FROM @ZENSAND.POS.ARCHIVER_POS_S3_STAGE/customer/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENSAND.POS.CUSTOMER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.POS.CUSTOMER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.POS.CUSTOMER_TASK resume;
