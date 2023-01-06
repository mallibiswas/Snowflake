-------------------------------------------------------
---------- USAGEBILLINGINFO_PACKAGE_THROUGH -----------
-------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.USAGEBILLINGINFO_PACKAGE_THROUGH;

copy into  &{dbname}._STAGING.USAGEBILLINGINFO_PACKAGE_THROUGH
from
(
	select 
	$1::integer as usagebillinginfo_package_id,
	$2::integer as usagebillinginfo_id,
	$3::integer as package_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/usagebillinginfo_package_through.csv
);

alter table &{dbname}._STAGING.USAGEBILLINGINFO_PACKAGE_THROUGH swap with &{dbname}.AMS.USAGEBILLINGINFO_PACKAGE_THROUGH;
