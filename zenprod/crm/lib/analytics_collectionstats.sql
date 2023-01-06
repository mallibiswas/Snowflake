-----------------------------------------------------------------------
--------------------- ANALYTICS COLLECTIONSTATS -----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.analytics_collectionstats;

copy into &{dbname}.&{stageschema}.analytics_collectionstats
from
(
select $1:_id:"$oid"::string as collection_id,
      $1:business_id:"$oid"::string as business_id,
      $1:date:"$date"::datetime as date,
      $1:phones::integer as phones,
      $1:valid_emails::integer as valid_emails,
      $1:mailgun_corrected_emails::integer as mailgun_corrected_emails,
      $1:wifast_corrected_emails::integer as wifast_corrected_emails,
      $1:likes::integer as likes,
      $1:follows::integer as follows,
      $1:invalid_emails::integer as invalid_emails,
      $1:zenboost_network::integer as zenboost_network,
      $1:emails::integer as emails, 
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/analytics_collectionstats.json 
);

alter table &{dbname}.&{stageschema}.analytics_collectionstats swap with &{dbname}.&{schemaname}.analytics_collectionstats;

