---------------------------------------------------------------
---------------- SALESFORCEINVOICEREFERENCE -------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.SALESFORCEINVOICEREFERENCE;

copy into  &{dbname}._STAGING.SALESFORCEINVOICEREFERENCE
from
(
	select 
	$1::integer as salesforceinvoice_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4::integer as location_id,
	$5::integer as invoice_id,
	$6 as salesforce_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/salesforceinvoicereference.csv
);

alter table &{dbname}._STAGING.SALESFORCEINVOICEREFERENCE swap with &{dbname}.AMS.SALESFORCEINVOICEREFERENCE;
