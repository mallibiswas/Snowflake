-------------------------------------------------------------------
----------------- PORTAL_ROUTER table
-------------------------------------------------------------------

-- Create procedure to replace table after latest s3 dump
create or replace procedure ZENPROD.CRM.PORTAL_ROUTER_PROCEDURE(FILE_DATE STRING)
returns boolean
language javascript
strict
as
$$
snowflake.createStatement({
  sqlText:`
    create or replace transient table ZENPROD.CRM.PORTAL_ROUTER as
    select  
      GET_PATH($1, '_id:$oid')::string as id,
      GET_PATH($1, 'userprofile_id:$oid')::string as userprofile_id,
      GET_PATH($1, 'desired_version_id:$oid')::string as desired_version_id,
      GET_PATH($1, 'business_id:$oid')::string as business_id,
      $1:ssid_2::string as ssid2,
      $1:original_password::string as original_password,
      GET_PATH($1, 'update_available:$date')::timestamp as update_available,
      $1:pinged_recently::boolean as pinged_recently,
      GET_PATH($1, 'current_version_id:$oid')::string as current_version_id,
      $1:adding_client::string as adding_client,
      GET_PATH($1, 'installed:$date')::timestamp as installed,
      $1:uplink_speed::integer as uplink_speed,
      $1:key_1::string as key_1,
      $1:key_2::string as key_2,
      $1:original_username::string as original_username,
      $1:ssh_portnum::integer as ssh_portnum,
      $1:wan_subnet_mask::string as wan_subnet_mask,
      $1:gateway_id::string as gateway_id,
      $1:stock_routertype_id::string as stock_routertype_id,
      GET_PATH($1, 'last_update:$date')::timestamp as last_update,
      GET_PATH($1, 'last_ping:$date')::timestamp as last_ping,
      GET_PATH($1, 'last_probe_upload:$date')::timestamp as last_probe_upload,
      $1:last_sys_uptime::timestamp as last_sys_uptime,
      GET_PATH($1, 'last_crm_req:$date')::timestamp as ast_crm_req,
      GET_PATH($1, 'last_update_check:$date')::timestamp as last_update_check,
      $1:last_wifidog_uptime::integer as last_wifidog_uptime,
      GET_PATH($1, 'last_sys_memfree:$date')::timestamp as last_sys_memfree,
      GET_PATH($1, 'last_sys_load:$date')::timestamp as last_sys_load,
      $1:stock_firmware_version_id::string as stock_firmware_version_id,
      $1:management_port::integer as management_port,
      $1:wan_connection_type::string as wan_connection_type,
      $1:dns_type::string as dns_type,
      $1:latitude::float as latitude,
      $1:longitude::float as longitude,
      $1:lan_mac::string as lan_mac,
      $1:wan_dns::string as wan_dns,
      $1:ssh_tunnel_available::boolean as ssh_tunnel_available,
      $1:username::string as username,
      $1:ssid_3::string as ssid_3,
      GET_PATH($1, 'updated:$date')::timestamp as updated,
      $1:ssid_1::string as ssid_1,
      $1:ssid_4::string as ssid_4,
      $1:adding_device_id::string as adding_device_id,
      $1:download_link_speed::integer as download_link_speed,
      GET_PATH($1, 'date_added:$date')::timestamp as date_added,
      $1:password::string as password,
      $1:ip_address::string as ip_address,
      $1:wan_gateway::string as wan_gateway,
      $1:security_1::string as security_1,
      $1:security_2::string as security_2,
      $1:security_3::string as security_3,
      $1:security_4::string as security_4,
      GET_PATH($1, 'created:$date')::timestamp as created,
      $1:time_zone::string as time_zone,
      $1:wan_ip::string as wan_ip,
      $1:internal_ipaddress::string as internal_ipaddress,
      $1:current_routertype_id::string as current_routertype_id,
      $1:wlan_mac::string as wlan_mac,
      $1:wan_mac::string as wan_mac,
      $1:street_address::string as street_address,
      DATEADD(day,-1,current_date()) as asof_date
    FROM @ZENPROD.CRM.MONGO_S3_STAGE/${FILE_DATE}/portal_router.json;`
     }).execute();
$$;


-- Create task to call the procedure
create task ZENPROD.CRM.PORTAL_ROUTER_TASK
    WAREHOUSE = ZENPROD
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
    CALL ZENPROD.CRM.PORTAL_ROUTER_PROCEDURE(DATEADD(day, -1, CURRENT_DATE()));

alter task ZENPROD.CRM.PORTAL_ROUTER_TASK resume;
