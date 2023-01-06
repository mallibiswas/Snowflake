-----------------------------------------------------------------------
---------------------  models_onboardingwizardpagecompletions ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.models_onboardingwizardpagecompletions;

copy into &{dbname}.&{stageschema}.models_onboardingwizardpagecompletions
from 
(
select $1:_id:"$oid"::string as onboardingwizardpagecompletions_id,
      $1:business_id:"$oid"::string as business_id,                  
      $1:completed:"$date"::datetime as completed,
      $1:page::string as page,      
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/models_onboardingwizardpagecompletions.json  
); 

alter table &{dbname}.&{stageschema}.models_onboardingwizardpagecompletions swap with &{dbname}.&{schemaname}.models_onboardingwizardpagecompletions;


