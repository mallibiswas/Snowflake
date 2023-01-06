-----------------------------------------------------------------------
---------------------  smbsite_mac_to_contact ----------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use schema &{stageschema};
use role &{rolename};

alter session set QUOTED_IDENTIFIERS_IGNORE_CASE = false;

truncate table &{dbname}.&{stageschema}.smbsite_mac_to_contact;

copy into &{dbname}.&{stageschema}.smbsite_mac_to_contact
from 
(
select 
    	$1:id::string as mac_to_contact_id,
    	$1:account_id::string as account_id,
    	$1:contact_id::string as contact_id,
    	$1:last_seen::timestamp as last_seen,
    	$1:location_id::string as business_id,
    	lower($1:mac::string) as client_mac,
      	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/smbsite_mac_to_contact00000.json  
); 

alter table &{dbname}.&{stageschema}.smbsite_mac_to_contact swap with &{dbname}.&{schemaname}.smbsite_mac_to_contact;


