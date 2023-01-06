-------------------------------------------------------------------
-----------------  provider_responses table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.DEMOGRAPHICS.PROVIDER_RESPONSES_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.DEMOGRAPHICS.PROVIDER_RESPONSES as
          select
            $1 as id,
            $2 as contact_info,
            $3 as contact_method,
            $4 as provider_name,
            $5 as payload,
            $6::timestamp as created,
            current_timestamp() as asof_date
          FROM @ZENPROD.DEMOGRAPHICS.DEMOGRAPHICS_S3_STAGE/${FILE_DATE}/provider_responses.csv;`
     }).execute();
$$;


-- Create task to call the procedure (every day)
create or replace task ZENPROD.DEMOGRAPHICS.PROVIDER_RESPONSES_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 00 * * * * UTC'
as 
    CALL ZENPROD.DEMOGRAPHICS.PROVIDER_RESPONSES_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.DEMOGRAPHICS.PROVIDER_RESPONSES_TASK resume;
