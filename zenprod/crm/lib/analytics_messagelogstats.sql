-----------------------------------------------------------------------
--------------------- ANALYTICS MESAGELOGSTATS --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.analytics_messagelogstats;

copy into &{dbname}.&{stageschema}.analytics_messagelogstats
from
(
select  $1:_id:"$oid"::string as messagelog_id,
      $1:business_id:"$oid"::string as business_id,
      $1:timestamp:"$date"::datetime as timestamp,
      $1:zenboost_sent::integer as zenboost_sent,
      $1:period::integer as period,
      $1:sent::integer as sent,
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/analytics_messagelogstats.json 
);

alter table &{dbname}.&{stageschema}.analytics_messagelogstats swap with &{dbname}.&{schemaname}.analytics_messagelogstats;

