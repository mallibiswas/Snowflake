-------------------------------------------------------------------
----------------- PORTAL_BUSINESS_RELATIONSHIP table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.PORTAL_BUSINESS_RELATIONSHIP_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.PORTAL_BUSINESS_RELATIONSHIP as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'last_login:$date')::timestamp as last_login,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:contact_allowed::boolean as contact_allowed,
      GET_PATH($1, 'last_updated:$date')::timestamp as last_updated,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_businessrelationship.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.PORTAL_BUSINESS_RELATIONSHIP_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.PORTAL_BUSINESS_RELATIONSHIP_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.PORTAL_BUSINESS_RELATIONSHIP_TASK resume;


