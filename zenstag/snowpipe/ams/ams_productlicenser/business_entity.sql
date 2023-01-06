-------------------------------------------------------------------
----------------- BUSINESS_ENTITY table
-------------------------------------------------------------------
   
-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS_PRODUCTLICENSER.BUSINESS_ENTITY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS_PRODUCTLICENSER.BUSINESS_ENTITY as
          select
            $1 as id,
            $2 as parent_id,
            $3 as name,
            $4 as account_id,
            $5 as crm_id,
            $6 as salesforce_id,
            $7 as type,
            $8::timestamp as created,
            $9::timestamp as updated,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS_PRODUCTLICENSER.AMS_PRODUCTLICENSER_S3_STAGE/${FILE_DATE}/business_entity.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS_PRODUCTLICENSER.BUSINESS_ENTITY_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS_PRODUCTLICENSER.BUSINESS_ENTITY_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS_PRODUCTLICENSER.BUSINESS_ENTITY_TASK resume;
