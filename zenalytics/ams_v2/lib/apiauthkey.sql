-----------------------------------------------------------------------
------------------ APIAUTHKEY -----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.APIAUTHKEY;

copy into  &{dbname}._STAGING.APIAUTHKEY
from
(
	select 
	$1::integer as apiauth_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as name,
	$5 as token,
	$6 as allow_http,
	$7 as revoked,
	$8 as revoke_reason,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/apiauthkey.csv
);

alter table &{dbname}._STAGING.APIAUTHKEY swap with &{dbname}.AMS.APIAUTHKEY;
