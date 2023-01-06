---------------------------------------------------------------
-------------------------- SAMPLE RATES  ----------------------
---------------------------------------------------------------


--- Presence Sampling Stats

use database &{dbname};
use warehouse &{whname};
use schema &{schemaname};
use role &{rolename};

/*
create or replace transient table &{dbname}.&{schemaname}.presence_sampling_stats as 
select 
$1 as sampling_id,
$2 as business_id,
$3::timestamp as date_hour,
$4::integer as walkin_merchant,
$5::integer as walkin_network,
$6::integer as walkin_unidentified,
$7::integer as walkby_merchant,
$8::integer as walkby_network,
$9::integer as walkby_unidentified,
$10::timestamp as created,
$11::timestamp as updated,
'&{asof_date}'::date as asof_date
FROM @&{stagename}/&{stagepath}/presence_sampling_stats.csv
; 
*/

--- Sample Rates
create or replace transient table &{dbname}.&{schemaname}.sample_rates as
select 	$1 as business_id, 
	$2::number as sample_rate_multiplier, 
	$3::date as day, 
	$4::timestamp_ntz as updated, 
	$5 as source, 
	current_timestamp() as asof_date
FROM @&{stagename}/&{stagepath}/sample_rates.csv
;
