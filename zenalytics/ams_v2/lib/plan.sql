---------------------------------------------------------------
----------------------------- PLAN ----------------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.PLAN;

copy into  &{dbname}._STAGING.PLAN
from
(
	select 
	$1::integer as plan_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as plan_code,
	$5 as product_code,
	$6 as payment_term,
	$7 as name,
	$8 as contract_template,
	$9::boolean as has_trial,
	$10::integer as product_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/plan.csv
);

alter table &{dbname}._STAGING.PLAN swap with &{dbname}.AMS.PLAN;
