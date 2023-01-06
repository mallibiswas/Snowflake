-------------------------------------------------------------------
----------------- AUTH_USER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.AUTH_USER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.AUTH_USER as
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
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/auth_user.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.AUTH_USER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.AUTH_USER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.AUTH_USER_TASK resume;
