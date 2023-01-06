-----------------------------------------------------------------------
--------------------- ANALYTICS AGGREGATESTATS --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.analytics_aggregatestats;

copy into &{dbname}.&{stageschema}.analytics_aggregatestats
from
(
select  $1:_id:"$oid"::string as analytics_aggregatestats_id,
      $1:business_id:"$oid"::string as business_id,
      $1:updated:"$date"::datetime as updated,
      $1:created:"$date"::datetime as created,
      $1:timestamp:"$date"::datetime as timestamp,
      $1:reach::integer as reach,
      $1:period::integer as period,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/analytics_aggregatestats.json 
);

alter table &{dbname}.&{stageschema}.analytics_aggregatestats swap with &{dbname}.&{schemaname}.analytics_aggregatestats;

