-------------------------------------------------------------------
----------------- LOCATION_STAFF_USER_ROLE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSTAG.AMS.LOCATION_STAFF_USER_ROLE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENSTAG.AMS.LOCATION_STAFF_USER_ROLE as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as location_id,
            $5::number as staff_user_id,
            $6::number as role_id,
            $7::boolean as active,
            current_timestamp() as asof_date
          FROM @ZENSTAG.AMS.AMS_S3_STAGE/${FILE_DATE}/locationstaffuserrole.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSTAG.AMS.LOCATION_STAFF_USER_ROLE_TASK
    WAREHOUSE = ZENSTAG
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENSTAG.AMS.LOCATION_STAFF_USER_ROLE_PROCEDURE(CURRENT_DATE());

alter task ZENSTAG.AMS.LOCATION_STAFF_USER_ROLE_TASK resume;