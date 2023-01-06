-----------------------------------------------------------------------
---------------------  models_usertutorialcompletion ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.models_usertutorialcompletion;

copy into &{dbname}.&{stageschema}.models_usertutorialcompletion
from 
(
select  $1:_id:"$oid"::string as usertutorialcompletion_id,
      $1:completed:composer_blast_edit:"$date"::datetime as composer_blast_edit,
      $1:completed:composer_blast_template:"$date"::datetime as composer_blast_template,
      $1:completed:composer_sm_edit:"$date"::datetime as composer_sm_edit,
      $1:completed:composer_sm_template:"$date"::datetime as composer_sm_template,
      $1:completed:contacts:"$date"::datetime as contacts,
      $1:completed:email_campaign:"$date"::datetime as email_campaign,
      $1:completed:insights:"$date"::datetime as completed,
      $1:completed:smart_message:"$date"::datetime as smart_message,
      $1:completed:"tutorial-index":"$date"::datetime as "TUTORIAL-INDEX",
      $1:completed:hotspot:"$date"::datetime as hotspot,
      $1:email::string as email,      
      $1:skipped_all::boolean as skipped_all,      
      '&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/models_usertutorialcompletion.json  
); 

alter table &{dbname}.&{stageschema}.models_usertutorialcompletion swap with &{dbname}.&{schemaname}.models_usertutorialcompletion;


