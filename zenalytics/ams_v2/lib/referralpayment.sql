---------------------------------------------------------------
------------------------- REFERRALPAYMENT ---------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.REFERRALPAYMENT;

copy into  &{dbname}._STAGING.REFERRALPAYMENT
from
(
	select 
	$1::integer as referralpayment_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as referrer_id,
	$5::integer as location_id,
	$6::integer as source_invoice_id,
	$7::integer as amount_in_cents,
	$8::number(18,2) as percentage,
	$9::integer as payout_bill_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/referralpayment.csv
);

alter table &{dbname}._STAGING.REFERRALPAYMENT swap with &{dbname}.AMS.REFERRALPAYMENT;
