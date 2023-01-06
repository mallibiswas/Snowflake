---------------------------------------------------------------
---------------- SALESFORCETRANSACTIONREFERENCE ---------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SALESFORCETRANSACTIONREFERENCE;

copy into  &{dbname}._STAGING.SALESFORCETRANSACTIONREFERENCE
from
(
	select 
	$1::integer as salesforcetransaction_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as location_id,
	$5::integer as transaction_id,
	$6 as salesforce_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/salesforcetransactionreference.csv
);

alter table &{dbname}._STAGING.SALESFORCETRANSACTIONREFERENCE swap with &{dbname}.AMS.SALESFORCETRANSACTIONREFERENCE;
