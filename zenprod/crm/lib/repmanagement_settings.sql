-----------------------------------------------------------------------
--------------------- repmanagement_settings      --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.repmanagement_settings;

copy into &{dbname}.&{stageschema}.repmanagement_settings
from
(
select  $1:_id:"$oid"::string as repmanagement_settings_id,
      $1:business_id:"$oid"::string as business_id,
      $1:created:"$date"::datetime as created,
      $1:facebook_url_enabled::boolean as facebook_url_enabled,
      $1:facebook_url::string as facebook_url, 
      $1:tripadvisor_url_enabled::boolean as tripadvisor_url_enabled,
      $1:tripadvisor_url::string as tripadvisor_url, 
      $1:yelp_url_enabled::boolean as yelp_url_enabled,
      $1:yelp_url::string as yelp_url,       
      $1:opentable_url_enabled::boolean as opentable_url_enabled,
      $1:opentable_url::string as opentable_url,
      $1:google_url_enabled::boolean as google_url_enabled,      
      $1:google_url::string as google_url,      
      '&{asof_date}'::date as asof_date
from @&{stagename}/&{stagepath}/repmanagement_settings.json 
);

alter table &{dbname}.&{stageschema}.repmanagement_settings swap with &{dbname}.&{schemaname}.repmanagement_settings;

