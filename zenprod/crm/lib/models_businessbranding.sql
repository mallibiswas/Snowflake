-----------------------------------------------------------------------
---------------------  models_businessbranding ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.models_businessbranding;

copy into &{dbname}.&{stageschema}.models_businessbranding
from 
(
select $1:_id:"$oid"::string as businessbranding_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:created:"$date"::datetime as created,
      $1:button_color::string as button_color,
      $1:font::string as font,
      $1:logo_id::string as logo_id,
      $1:tone::string as tone,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/models_businessbranding.json  
); 

alter table &{dbname}.&{stageschema}.models_businessbranding swap with &{dbname}.&{schemaname}.models_businessbranding;


