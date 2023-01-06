-----------------------------------------------------------------------
---------------------  PORTAL BUSINESSRELATIONSHIP --------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PORTAL_BUSINESSRELATIONSHIP;

copy into &{dbname}.&{stageschema}.PORTAL_BUSINESSRELATIONSHIP
from
(
    select  $1:_id:"$oid"::string as businessrelatinship_id,
            $1:business_id:"$oid"::string as business_id,        
            $1:created:"$date"::datetime as created,
            $1:importer_id:"$oid"::string as importer_id,
            $1:is_employee::boolean as is_employee,
            $1:userprofile_id:"$oid"::string as userprofile_id,
	    $1:login_count::integer as login_count, 
	    $1:last_updated:"$date"::timestamp as last_updated, 
	    $1:last_login:"$date"::timestamp as last_login, 
	    $1:contact_allowed::boolean as contact_allowed,
	    '&{asof_date}'::date as asof_date	
    from @&{stagename}/&{stagepath}/portal_businessrelationship.json 
);

alter table &{dbname}.&{stageschema}.PORTAL_BUSINESSRELATIONSHIP swap with &{dbname}.&{schemaname}.PORTAL_BUSINESSRELATIONSHIP;
