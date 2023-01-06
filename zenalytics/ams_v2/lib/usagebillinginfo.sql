------------------------------------------------------------
--------------------- USAGEBILLINGINFO ---------------------
------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.USAGEBILLINGINFO;

copy into  &{dbname}._STAGING.USAGEBILLINGINFO
from
(
	select 
	$1::integer as usagebilling_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as usage_type,
	$5::number(18,2) as soft_limit,
	$6::number(18,2) as hard_limit,
	$7::number(18,2) as unit_cost_over_limit,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/usagebillinginfo.csv
);

alter table &{dbname}._STAGING.USAGEBILLINGINFO swap with &{dbname}.AMS.USAGEBILLINGINFO;
