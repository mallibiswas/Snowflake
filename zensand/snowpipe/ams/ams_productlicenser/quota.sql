-------------------------------------------------------------------
----------------- QUOTA table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS_PRODUCTLICENSER.QUOTA_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS_PRODUCTLICENSER.QUOTA as
          select
            $1 as id,
            $2::number as soft_limit,
            $3::number as hard_limit,
            $4::float as unit_penalty_in_cents,
            $5 as quota_type_id,
            $6::timestamp as created,
            current_timestamp() as of_date
          FROM @ZENSAND.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/quota.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS_PRODUCTLICENSER.QUOTA_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS_PRODUCTLICENSER.QUOTA_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS_PRODUCTLICENSER.QUOTA_TASK resume;