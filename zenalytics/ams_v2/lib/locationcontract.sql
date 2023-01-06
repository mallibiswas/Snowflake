----------------------------------------------------------
------------------------- LOCATIONCONTRACT ------------------------
----------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.LOCATIONCONTRACT;

copy into  &{dbname}._STAGING.LOCATIONCONTRACT
from
(
	select 
	$1::integer as locationcontract_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as location_id,
	$5::integer as contract_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/locationcontract.csv
);

alter table &{dbname}._STAGING.LOCATIONCONTRACT swap with &{dbname}.AMS.LOCATIONCONTRACT;
