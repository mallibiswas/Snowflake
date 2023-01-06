-------------------------------------------------------------------
----------------- PORTAL_ROUTER_TYPE table
-------------------------------------------------------------------

-- Create procedure to replace table (at every 45th minute)
create or replace procedure ZENSAND.CRM.PORTAL_ROUTER_TYPE_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENSAND.CRM.PORTAL_ROUTER_TYPE as
    select  
      $1:_id::string as id,
      $1:comment::string as comment,
      $1:router_version::string as router_version,
      $1:is_custom_firmware::boolean as is_custom_firmware,
      $1:known_unsupportable::boolean as known_unsupportable,
      $1:auth_technique::string as auth_technique,
      $1:workflow_class::string as workflow_class,
      $1:router_make::string as router_make,
      $1:custom_firmware_file_1::string as custom_firmware_file_1,
      $1:custom_firmware_file_2::string as custom_firmware_file_2,
      $1:default_username::string as default_username,
      $1:software_version::string as software_version,
      $1:wan_ifname::string as wan_ifname,
      $1:router_model::string as router_model,
      $1:stock_firmware_file::string as stock_firmware_file,
      $1:non_consumer::boolean as non_consumer,
      $1:default_password::string as default_password,
      $1:takeover_technique::string as takeover_technique,
      $1:custom_routertype_id::string as custom_routertype_id,
      GET_PATH($1, 'configuration_logic_id:$oid')::string as configuration_logic_id,
      $1:detection_logic::variant as detection_logic,
      current_timestamp() as asof_date
    FROM @ZENSAND.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_routertype.json;`
     }).execute();
$$;


-- Create task to call the procedure (at every 45th minute)
create task ZENSAND.CRM.PORTAL_ROUTER_TYPE_TASK
    WAREHOUSE = ZENSAND
    SCHEDULE = 'USING CRON 0 20 * * * UTC'
as 
    CALL ZENSAND.CRM.PORTAL_ROUTER_TYPE_PROCEDURE(CURRENT_DATE());

alter task ZENSAND.CRM.PORTAL_ROUTER_TYPE_TASK resume;
