-----------------------------------------------------------------------
----------------------------------- FEATURE ---------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.FEATURE;

copy into  &{dbname}._STAGING.FEATURE
from
(
	select 
	$1::integer as feature_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as feature_code,
	$5 as feature_name,
	$6 as feature_description,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/feature.csv
);

alter table &{dbname}._STAGING.FEATURE swap with &{dbname}.AMS.FEATURE;
