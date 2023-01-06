--
use warehouse &{whname};
use database &{dbname};
use schema &{tgtschemaname};
use role &{rolename};
--
ALTER SESSION SET TIMEZONE = 'UTC';
--
-- main query
delete 
from  	&{dbname}.&{tgtschemaname}.&{tgttablename} a
using 	&{dbname}.&{refschemaname}.PRIVACY_REQUEST b
where   a.email = b.contact_info;


