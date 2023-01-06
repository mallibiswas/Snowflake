------------------------------------------------------------
--------------------- LOCATIONREFERRAL ---------------------
------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.LOCATIONREFERRAL;

copy into  &{dbname}._STAGING.LOCATIONREFERRAL
from
(
	select 
	$1::integer as locationreferral_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as referrer_id,
	$5::integer as location_id,
	$6 as payment_percentage,
	$7::date as referred_date,
	$8::integer as duration_in_months,
	$9::date as payment_start_date,
	$10::date as payment_end_date,
	$11::date as termination_date,
        '&{asof_date}'::date as asof_date
	from @&{stagename}/&{stagepath}/locationreferral.csv
);

alter table &{dbname}._STAGING.LOCATIONREFERRAL swap with &{dbname}.AMS.LOCATIONREFERRAL;
