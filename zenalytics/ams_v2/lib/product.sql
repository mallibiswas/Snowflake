---------------------------------------------------------------
------------------------- PRODUCT ---------------------------
---------------------------------------------------------------

use warehouse &{whname};

truncate table &{dbname}._STAGING.PRODUCT;

copy into  &{dbname}._STAGING.PRODUCT
from
(
	select 
	$1::integer as product_id,
	$2::timestamp as created,
	$3::timestamp as updated,
	$4 as code,
	$5 as name,
	'&{asof_date}'::date as asof_date
    from @&{stagename}/&{stagepath}/product.csv
);

alter table &{dbname}._STAGING.PRODUCT swap with &{dbname}.AMS.PRODUCT;
