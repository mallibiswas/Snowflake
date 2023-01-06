-----------------------------------------------------------------------
---------------------  PORTAL USERPORFILE -----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PORTAL_USERPROFILE;

copy into &{dbname}.&{stageschema}.PORTAL_USERPROFILE
from (
      select  $1:_id:"$oid"::string as userprofile_id,
              $1:credits::integer as credits,        
              $1:date_added:"$date"::datetime as date_added,
              $1:demographicprofile_id:"$oid"::string as demographicprofile_id,
              $1:email::string as email,
              $1:email_corrected_by::string as email_corrected_by,
              $1:email_is_valid::boolean as email_is_valid,
              $1:email_last_validated:"$date"::datetime as email_last_validated,
              $1:email_reason::string as email_reason,
              $1:email_score::number(5,2) as email_score,
              $1:facebookprofile_id:"$oid"::string as facebookprofile_id,
              $1:importer_id:"$oid"::string as importer_id,
              $1:information_source::string as information_source,
--              $1:nt_password::string as nt_password,
--              $1:twitterprofile_id:"$oid"::string as twitterprofile_id,
              $1:user_id:"$oid"::string as user_id,
		'&{asof_date}'::date as asof_date
      from @&{stagename}/&{stagepath}/portal_userprofile.json 
)
on_error='CONTINUE'
; 

alter table &{dbname}.&{stageschema}.PORTAL_USERPROFILE swap with &{dbname}.&{schemaname}.PORTAL_USERPROFILE;
