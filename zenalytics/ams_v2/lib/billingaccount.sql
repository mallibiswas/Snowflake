-----------------------------------------------------------------------
------------------------- BILLINGACCOUNT ------------------------------
-----------------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.BILLING_ACCOUNT;

copy into  &{dbname}._STAGING.BILLING_ACCOUNT
from
(
select 
	$1::integer as billing_account_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5 as provider_id,
	$6 as ba_state,
	$7 as name,
	$8 as payment_type,
	$9 as last_four,
	$10 as info_type,
	$11 as holder,
	$12::date as expiry,
	$13 as address_line1,
	$14 as address_line2,
	$15 as address_zip,
	$16 as address_city,
	$17 as address_state,
	$18 as address_country,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/billingaccount.csv
);

alter table &{dbname}._STAGING.BILLING_ACCOUNT swap with &{dbname}.AMS.BILLING_ACCOUNT;
