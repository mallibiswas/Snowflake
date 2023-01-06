-----------------------------------------------------------------------
-------------------- AMDASHBOARD_BILLING_DATA -------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.AMDASHBOARD_BILLING_DATA;

copy into  &{dbname}._STAGING.AMDASHBOARD_BILLING_DATA
from
(
	select 
		$1 as business_id,
		$2 as amdashboard_account_id,
		$3::date as next_billing_date,
		$4::integer as invoice_past_due_count,
		$5::date as invoice_past_due_first_date,
		$6::number(38,2) as invoice_past_due_amount,
		$7::integer as invoice_paid_count,
		$8::date as invoice_paid_first_date,
		$9::number(38,2) as invoice_paid_amount,
		'&{asof_date}'::date as asof_date
	FROM @&{stagename}/&{stagepath}/amdashboard_billing_data.csv
);

alter table &{dbname}._STAGING.AMDASHBOARD_BILLING_DATA swap with &{dbname}.AMS.AMDASHBOARD_BILLING_DATA;
	
