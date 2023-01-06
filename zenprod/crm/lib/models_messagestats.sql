-----------------------------------------------------------------------
---------------------  models_messagestats ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.models_messagestats;

copy into &{dbname}.&{stageschema}.models_messagestats
from 
(
select  $1:_id:"$oid"::string as messagestats_id,
      $1:message_id:"$oid"::string as message_id,                  
      $1:click_breakdown::variant as click_breakdown,
      $1:offers_stats::variant as offers_stats,      
      $1:bounced::integer as bounced,
      $1:soft_bounced::integer as soft_bounced,
      $1:clicked::integer as clicked,
      $1:clicks::variant as clicks,
      $1:conversions::integer as conversions,
      $1:cumulative_conversions::variant as cumulative_conversions,
      $1:cumulative_opened::variant as cumulative_opened,
      $1:opened::integer as opened,
      $1:sent::integer as sent,
      $1:delivered::integer as delivered,
      $1:trackable::integer as trackable,
      $1:unsubscribed::integer as unsubscribed,
      $1:updated:"$date"::datetime as updated,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/models_messagestats.json  
); 

alter table &{dbname}.&{stageschema}.models_messagestats swap with &{dbname}.&{schemaname}.models_messagestats;


