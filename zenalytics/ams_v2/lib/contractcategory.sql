-----------------------------------------------------------------------
--------------------------- CONTRACTCATEGORY --------------------------
-----------------------------------------------------------------------

use database &{dbname};
use warehouse &{whname};
use role &{rolename};

truncate table &{dbname}._STAGING.CONTRACTCATEGORY;

copy into  &{dbname}._STAGING.CONTRACTCATEGORY
from
(
	select
		$1 as category_id,
		$2::timestamp as created,
		$3::timestamp as updated,
		$4 as title,
		'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/contractcategory.csv
);

alter table &{dbname}._STAGING.CONTRACTCATEGORY swap with &{dbname}.AMS.CONTRACTCATEGORY;
