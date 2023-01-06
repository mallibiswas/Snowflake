---------------------------------------------------------------
-------------------------- SAMPLE RATES  ----------------------
---------------------------------------------------------------

use role &{rolename};
use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};

--- Sample Rates are loaded from nwsampling bucket
--- this data replaces the old presence_sampling_stats data
create or replace transient table &{dbname}.&{schemaname}.sample_rates as
select 	$1 as business_id,
	$2::number as sample_rate_multiplier,
	$3::date as day,
	$4::timestamp_ntz as updated,
	$5 as source,
	current_timestamp() as asof_date
FROM @&{stagename}/&{stagepath}/sample_rates.csv
;
