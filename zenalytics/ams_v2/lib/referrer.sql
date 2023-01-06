---------------------------------------------------------------
------------------------- REFERRER ----------------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.REFERRER;

copy into  &{dbname}._STAGING.REFERRER
from
(
	select 
	$1::integer as referrer_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as name,
	$5 as email,
	$6 as payable_account,
	$7 as salesforce_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/referrer.csv
);

alter table &{dbname}._STAGING.REFERRER swap with &{dbname}.AMS.REFERRER;
