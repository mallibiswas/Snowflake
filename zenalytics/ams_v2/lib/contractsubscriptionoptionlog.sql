-----------------------------------------------------------------------
------------------ CONTRACTSUBSCRIPTIONOPTIONLOG ----------------------
-----------------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.CONTRACTSUBSCRIPTIONOPTIONLOG;

copy into  &{dbname}._STAGING.CONTRACTSUBSCRIPTIONOPTIONLOG
from
(
	select 
	$1::integer as contractsubscriptionoption_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as contract_id,
	$5::integer as subscription_option_id,
	$6 as original_payment_term,
	$7 as original_renewal_term,
	$8 as new_payment_term,
	$9 as new_renewal_term,
	$10 as discount_percent,
	$11::integer as original_service_fee,
	$12::integer as new_service_fee,
	$13::integer as subscription_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/contractsubscriptionoptionlog.csv
);

alter table &{dbname}._STAGING.CONTRACTSUBSCRIPTIONOPTIONLOG swap with &{dbname}.AMS.CONTRACTSUBSCRIPTIONOPTIONLOG;
