-------------------------------------------------------
------------------ ACCOUNT ------------------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.ACCOUNT;

copy into  &{dbname}._STAGING.ACCOUNT
from
(
	select 
	$1::integer as account_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as name,
	$5 as salesforce_id,
	$6 as business_profile_id,
	$7 as account_state,
	$8::boolean as disqualified,
	$9 as billing_mode,
	$10::integer as partner_account_id,
	$11::integer as billing_account_id,
	$12::boolean as is_test,
	$13::string as v3_account_id,
	case when $13 is not null then to_date($3) else null end as migrated_date,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/account.csv
);

alter table &{dbname}._STAGING.ACCOUNT swap with &{dbname}.AMS.ACCOUNT;
