-------------------------------------------------------------------
----------------- APIAUTHKEY table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.APIAUTHKEY_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.APIAUTHKEY as
          select
            $1 as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4 as name,
            $5 as token,
            $6::boolean as allow_http,
            $7::boolean as revoked,
            $8 as revoke_reason,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/apiauthkey.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.APIAUTHKEY_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.APIAUTHKEY_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.APIAUTHKEY_TASK resume;