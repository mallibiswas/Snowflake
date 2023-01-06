---------------------------------------------------------------
------------------ PARTNERACCOUNT ----------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.PARTNER_ACCOUNT;

copy into  &{dbname}._STAGING.PARTNER_ACCOUNT
from
(
	select 
	$1::integer as partner_account_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as name,
	$5 as salesforce_id,
	$6 as partner_type,
	$7::integer as billing_account_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/partneraccount.csv
);

alter table &{dbname}._STAGING.PARTNER_ACCOUNT swap with &{dbname}.AMS.PARTNER_ACCOUNT;
