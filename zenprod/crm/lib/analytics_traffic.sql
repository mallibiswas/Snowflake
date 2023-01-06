-----------------------------------------------------------------------
--------------------- ANALYTICS TRAFFIC      --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.analytics_traffic;

copy into &{dbname}.&{stageschema}.analytics_traffic
from
(
select  $1:_id:"$oid"::string as analytics_traffic_id,
      $1:business_id:"$oid"::string as business_id,
      $1:updated:"$date"::datetime as updated,
      $1:timestamp:"$date"::datetime as timestamp,
      $1:avg_visit_duration::integer as avg_visit_duration,
      $1:period::integer as period,
      $1:visitors::integer as visitors,
      $1:new_visitors::integer as new_visitors,
      $1:repeat_visitors::integer as repeat_visitors,
      $1:passersby::integer as passersby,
      $1:converted_visitors::integer as converted_visitors,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/analytics_traffic.json 
);

alter table &{dbname}.&{stageschema}.analytics_traffic swap with &{dbname}.&{schemaname}.analytics_traffic;

