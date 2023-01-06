-- Procedure to completely replace table, this ensures gdpr compliance
create or replace procedure ZENSAND.POS.CREDIT_CARD_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.POS.CREDIT_CARD as	
          select
            $1::string as id,
            $2::string as customer_id,
            $3::string as first_digits,
            $4::string as last_digits,
            $5::string as card_type,
            $6::integer as expiration_month,
            $7::integer as expiration_year,
            $8::string as name
          FROM @ZENSAND.POS.ARCHIVER_POS_S3_STAGE/credit_card/${FILE_DATE}.csv;`
     }).execute();
$$;

-- Create task to call the procedure
create task ZENSAND.POS.CREDIT_CARD_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.POS.CREDIT_CARD_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.POS.CREDIT_CARD_TASK resume;
