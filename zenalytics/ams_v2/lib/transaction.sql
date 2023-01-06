-------------------------------------------------------
--------------------- TRANSACTION ---------------------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.TRANSACTION;

copy into  &{dbname}._STAGING.TRANSACTION
from
(
	select 
	$1::integer as transaction_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as account_id,
	$5::integer as billing_account_id,
	$6::integer as invoice_id,
	$7::integer as subscription_id,
	$8 as provider_id,
	$9::date as date,
	$10 as action,
	$11::integer as amount_in_cents,
	$12 as status,
	$13 as salesforce_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/transaction.csv
);

alter table &{dbname}._STAGING.TRANSACTION swap with &{dbname}.AMS.TRANSACTION;
