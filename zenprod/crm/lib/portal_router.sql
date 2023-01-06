-----------------------------------------------------------------------
---------------------  PORTAL ROUTER ----------------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PORTAL_ROUTER;

copy into &{dbname}.&{stageschema}.PORTAL_ROUTER
from 
(
    select  $1:_id:"$oid"::string as portal_router_id,
            $1:adding_client::string as adding_client,
            $1:adding_device_id::string as adding_device_id,
            $1:business_id:"$oid"::string as business_id,
            $1:created:"$date"::datetime as created,
            $1:current_routertype_id::string as current_routertype_id,
            $1:current_version_id:"$oid"::string as current_version_id,
            $1:date_added:"$date"::datetime as date_added,
            $1:desired_version_id:"$oid"::string as desired_version_id,
            $1:dns_type::string as dns_type,
            $1:downlink_speed::string as downlink_speed,
            $1:gateway_id::string as gateway_id,
            $1:installed:"$date"::datetime as installed,
            $1:internal_ipaddress::string as internal_ipaddress,
            $1:ipaddress::string as ipaddress,
            $1:key_1::string as key_1,
            $1:key_2::string as key_2,
            string_to_mac($1:lan_mac::string) as lan_mac,
            $1:last_crm_req:"$date"::datetime as last_crm_req,
            $1:last_ping:"$date"::datetime as last_ping,
            $1:last_probe_upload:"$date"::datetime as last_probe_upload,
            $1:last_sys_load:"$date"::datetime as last_sys_load,
            $1:last_sys_memfree:"$date"::datetime as last_sys_memfree,
            $1:last_sys_uptime:"$date"::datetime as last_sys_uptime,
            $1:last_update:"$date"::datetime as last_update,
            $1:last_update_check:"$date"::datetime as last_update_check,
	    case when $1:last_wifidog_uptime:"$numberLong" is not null then null else $1:last_wifidog_uptime::integer end as last_wifidog_uptime,
            $1:latitude::float as latitude,
            $1:longitude::float as longitude,
            $1:management_port::number(38,0) as management_port,
            $1:original_password::string as original_password,
            $1:original_username::string as original_username,
            $1:password::string as password,
            $1:pinged_recently::boolean as pinged_recently,
            $1:security_1::string as security_1,
            $1:security_2::string as security_2,
            $1:security_3::string as security_3,
            $1:security_4::string as security_4,
            $1:ssh_portnum::number(38,0) as ssh_portnum,
            $1:ssh_tunnel_available::boolean as ssh_tunnel_available,
            $1:ssid_1::string as ssid_1,
            $1:ssid_2::string as ssid_2,
            $1:ssid_3::string as ssid_3,
            $1:ssid_4::string as ssid_4,
            $1:stock_firmware_version_id::string as stock_firmware_version_id,
            $1:stock_routertype_id::string as stock_routertype_id,
            $1:street_address::string as street_address,
            $1:time_zone::string as time_zone,
            $1:update_available:"$date"::datetime as update_available,
            $1:updated:"$date"::datetime as updated,
            $1:uplink_speed::number(38,0) as uplink_speed,
            $1:username::string as username,
            $1:userprofile_id:"$oid"::string as userprofile_id,
            $1:wan_connection_type::string as wan_connection_type,
            $1:wan_dns::string as wan_dns,
            $1:wan_gateway::string as wan_gateway,
            $1:wan_ip::string as wan_ip,
            string_to_mac($1:wan_mac::string) as wan_mac,  
            $1:wan_subnet_mask::string as wan_subnet_mask,
            $1:wifidog_profile::string as wifidog_profile,
            string_to_mac($1:wlan_mac::string) as wlan_mac,
		'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/portal_router.json  
); 

alter table &{dbname}.&{stageschema}.PORTAL_ROUTER swap with &{dbname}.&{schemaname}.PORTAL_ROUTER;


