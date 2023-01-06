-------------------------------------------------------------------
----------------- SMBSITE_OFFER table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_OFFER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_OFFER as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      $1:title::string as title,
      $1:text::string as text,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:logo_id::string as logo_id,
      GET_PATH($1, 'expiration:$date')::timestamp as expiration,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_offer.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_OFFER_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_OFFER_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_OFFER_TASK resume;
