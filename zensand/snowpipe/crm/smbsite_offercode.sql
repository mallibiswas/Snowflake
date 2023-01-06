-------------------------------------------------------------------
----------------- SMBSITE_OFFERCODE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.SMBSITE_OFFERCODE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.SMBSITE_OFFERCODE as
    select  
      $1:_id::string as offer_id,
      $1:code::string as offer_code,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/smbsite_offercode.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute) 
create task ZENSAND.CRM.SMBSITE_OFFERCODE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.SMBSITE_OFFERCODE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.SMBSITE_OFFERCODE_TASK resume;
