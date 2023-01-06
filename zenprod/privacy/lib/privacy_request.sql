-----------------------------------------------------------------------
---------------------  privacy_request --------------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.PRIVACY_REQUEST;

copy into &{dbname}.&{stageschema}.PRIVACY_REQUEST
from 
(
select 
	$1:id::string as request_id,
	$1:account_id::string as root_business_id,
	$1:contact_info::string as contact_info,
	$1:contact_method::string as contact_method,
	$1:portal_id::string as portal_id,
	$1:request_type::string as request_type,
	$1:source::string as source,
	$1:created::timestamp_tz as created,
	$1:completed::timestamp_tz as completed,
	parse_json($1:expected_response_sources::variant) as expected_response_sources,
  	'&{asof_date}'::date as asof_date
  from @&{stagename}/&{stagepath}/&{stagefile}
); 

alter table &{dbname}.&{stageschema}.PRIVACY_REQUEST swap with &{dbname}.&{schemaname}.PRIVACY_REQUEST;


