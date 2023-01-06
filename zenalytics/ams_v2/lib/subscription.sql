---------------------------------------------------------------
------------------------- SUBSCRIPTION -----------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SUBSCRIPTION;

copy into  &{dbname}._STAGING.SUBSCRIPTION
from
(
	select 
	$1::integer as subscription_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as billing_account_id,
	$6 as recurly_subscription_token,
	$7 as subscription_state,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/subscription.csv
);

alter table &{dbname}._STAGING.SUBSCRIPTION swap with &{dbname}.AMS.SUBSCRIPTION;
