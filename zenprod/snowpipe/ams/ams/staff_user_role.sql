-------------------------------------------------------------------
----------------- STAFF_USER_ROLE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENPROD.AMS.STAFF_USER_ROLE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
    snowflake.createStatement({ 
      sqlText: `
          create or replace transient table ZENPROD.AMS.STAFF_USER_ROLE as
          select
            $1::number as id,
            $2::timestamp as created,
            $3::timestamp as updated,
            $4::number as staff_user_id,
            $5::number as role_id,
            $6::boolean as active,
            current_timestamp() as asof_date
          FROM @ZENPROD.AMS.AMS_S3_STAGE/${FILE_DATE}/staffuserrole.csv;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENPROD.AMS.STAFF_USER_ROLE_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON */45 * * * * UTC'
as 
    CALL ZENPROD.AMS.STAFF_USER_ROLE_PROCEDURE(CURRENT_DATE());

alter task ZENPROD.AMS.STAFF_USER_ROLE_TASK resume;