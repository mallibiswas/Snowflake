-------------------------------------------------------------------
----------------- AUTH_USER table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.AUTH_USER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.AUTH_USER as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:username::string as username,
      $1:first_name::string as first_name,
      $1:last_name::string as last_name,
      $1:is_active::boolean as is_active,
      $1:email::string as email,
      $1:is_superuser::boolean as is_superuser,
      $1:is_staff::boolean as is_staff,
      GET_PATH($1, 'last_login:$date')::datetime as last_login,
      $1:password::string as password,
      GET_PATH($1, 'date_joined:$date')::datetime as date_joined,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/auth_user.json;`
     }).execute();
$$;


-- Create task to call the procedure 
create task ZENPROD.CRM.AUTH_USER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.AUTH_USER_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.AUTH_USER_TASK resume;
