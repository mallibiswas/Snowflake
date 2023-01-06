----------------------------------------------------------------------
-------------------------------- FEATUREFLAG ------------------------
----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.FEATUREFLAG;

copy into  &{dbname}._STAGING.FEATUREFLAG
from
(
	select 
	$1::integer as feature_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as enabled,
	$5 as feature_name,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/featureflag.csv
);

alter table &{dbname}._STAGING.FEATUREFLAG swap with &{dbname}.AMS.FEATUREFLAG;
