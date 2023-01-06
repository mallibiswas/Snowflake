-----------------------------------------------------------------------
---------------------  models_accounttutorialcompletion ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.models_accounttutorialcompletion;

copy into &{dbname}.&{stageschema}.models_accounttutorialcompletion
from 
(
select $1:_id:"$oid"::string as accounttutorialcompletion_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:userprofile_id:"$oid"::string as userprofile_id,                  
      $1:completed:"$date"::datetime as completed,
      $1:extra_args::variant as extra_args,
      $1:tutorial::string as tutorial,
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/models_accounttutorialcompletion.json  
); 

alter table &{dbname}.&{stageschema}.models_accounttutorialcompletion swap with &{dbname}.&{schemaname}.models_accounttutorialcompletion;


