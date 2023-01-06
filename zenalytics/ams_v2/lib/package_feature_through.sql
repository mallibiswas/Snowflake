---------------------------------------------------------------
------------------ PACKAGE_FEATURE_THROUGH ----------------------
---------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.PACKAGE_FEATURE_THROUGH;

copy into  &{dbname}._STAGING.PACKAGE_FEATURE_THROUGH
from
(
	select 
	$1::integer as package_feature_id,
	$2::integer as package_id,
	$3::integer as feature_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/package_feature_through.csv
);

alter table &{dbname}._STAGING.PACKAGE_FEATURE_THROUGH swap with &{dbname}.AMS.PACKAGE_FEATURE_THROUGH;
