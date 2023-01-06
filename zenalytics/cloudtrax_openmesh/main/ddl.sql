create or replace table public.cloudtrax_openmesh_router_status
(mac VARCHAR(16777216),
 down boolean,
 uptime VARCHAR(16777216),
 uptime_seconds number(38,2),
 ip VARCHAR(16777216),
 name VARCHAR(16777216),
 network_first_add datetime,           
 outdoor boolean,
 active_clients number(38,2),
 alerts variant,
 lan_info variant,
asof_date datetime default current_timestamp())

