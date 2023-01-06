-----------------------------------------------------------------------
------------------ BILLING_ACCOUNT_V2 -----------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};


truncate table &{dbname}._STAGING.BILLING_ACCOUNT_V2;

copy into  &{dbname}._STAGING.BILLING_ACCOUNT_V2
from
(
	select 
	$1::integer as billing_account_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as partner_account_id,
	$6 as email,
	$7 as phone_number,
	$8 as billing_account_state,
	$9 as payment_method,
	$10 as address_line1,
	$11 as address_line2,
	$12 as address_city,
	$13 as address_state,
	$14 as address_zip,
	$15 as address_country,
	$16 as recurly_account_id,
	$17 as info_type,
	$18::date as expiry,
	$19 as holder,
	$20 as last_four,
	$21::integer as old_world_billing_account_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/billing_account_v2.csv
);

alter table &{dbname}._STAGING.BILLING_ACCOUNT_V2 swap with &{dbname}.AMS.BILLING_ACCOUNT_V2;
