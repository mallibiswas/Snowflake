----------------------------------------------------------
------------------------- INVOICE ------------------------
----------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.INVOICE;

copy into  &{dbname}._STAGING.INVOICE
from
(
	select 
	$1::integer as invoice_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as subscription_id,
	$6::integer as billing_account_id,
	$7 as provider_id,
	$8::date as date,
	$9::integer as invoice_number,
	$10 as invoice_state,
	$11::integer as total_in_cents,
	$12 as currency,
	$13::datetime as closed_at,
	$14 as collection_method,
	$15 as net_terms,
	$16 as salesforce_id,
	$17 as processed_for_referral,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/invoice.csv
);

alter table &{dbname}._STAGING.INVOICE swap with &{dbname}.AMS.INVOICE;
