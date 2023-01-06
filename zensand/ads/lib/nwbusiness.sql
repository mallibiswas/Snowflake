---------------------------------------------------------------
-------------------------- ADSBIZ  ----------------------------
---------------------------------------------------------------

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

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
$12 as g_ad_acct_id,
$13 as g_conversion_name,
$14 as fb_product_id,
current_timestamp() as asof_date
FROM @&{stagename}/&{stagepath}/adsbiz.csv
;
