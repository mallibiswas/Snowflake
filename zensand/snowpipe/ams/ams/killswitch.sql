-------------------------------------------------------------------
----------------- KILLSWITCH table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.AMS.KILLSWITCH_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSAND.AMS.KILLSWITCH as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as name,
            $5 as killed,
            current_timestamp() as asof_date
          FROM @ZENSAND.AMS.AMS_S3_STAGE/${FILE_DATE}/killswitch.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.AMS.KILLSWITCH_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSAND.AMS.KILLSWITCH_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.AMS.KILLSWITCH_TASK resume;