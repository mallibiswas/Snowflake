---------------------------------------------------------------
-------------------------- ADSBIZ  ----------------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

create or replace transient table &{dbname}.&{schemaname}.adsbiz as 
select 
$1 as adbiz_id,
$2 as business_id,
$3 as parent_id,
$4 as name,
$5::boolean as is_active,
$6::boolean as is_gk,
$7 as fb_ad_acct_id,
$8::timestamp as last_synced,
$9::timestamp as created,
$10 as fb_page_id,
$11 as fb_offline_event_set_id,
$14 as g_ad_acct_id,
$15 as g_conversion_name,
$16 as fb_default_attribution_window,
current_timestamp() as asof_date
FROM @&{stagename}/&{stagepath}/adsbiz.csv
; 

