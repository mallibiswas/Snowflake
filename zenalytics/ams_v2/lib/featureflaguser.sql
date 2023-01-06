------------------------------------------------------------------
------------------------- FEATUREFLAGUSER ------------------------
------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.FEATUREFLAGUSER;

copy into  &{dbname}._STAGING.FEATUREFLAGUSER
from
(
	select 
	$1::integer as featureflaguser_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as email,
	$5::integer as feature_id,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/featureflaguser.csv
);

alter table &{dbname}._STAGING.FEATUREFLAGUSER swap with &{dbname}.AMS.FEATUREFLAGUSER;
